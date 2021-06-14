# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# import asyncio
import logging
import time

from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.jobs.exporters.databasse.mongo_db import Database
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.jobs.export_tokens_job import clean_user_provided_content
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.mappers.token_transfer_mapper import EthTokenTransferMapper
from ethereumetl.mappers.wallet_mapper import get_wallet_dict
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor, TRANSFER_EVENT_TOPIC
from ethereumetl.utils import validate_range

logger = logging.getLogger(__name__)


class ExportTokenTransfersJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            w3,
            item_exporter,
            max_workers,
            database=Database(),
            tokens=None,
            latest_block=None,
            provider_uris=None,

    ):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.web3 = w3
        self.tokens = tokens
        self.item_exporter = item_exporter

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

        self.receipt_log_mapper = EthReceiptLogMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()
        self.token_transfer_extractor = EthTokenTransferExtractor()
        self.token_dict_cache = []
        self.ethTokenService = EthTokenService(w3, clean_user_provided_content, provider_uris)
        self.database = database
        self.latest_block = latest_block
        if latest_block:
            self.block_thread_hole = int(latest_block * 0.8)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        # self.token_dict_cache = []
        assert len(block_number_batch) > 0
        # https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getfilterlogs
        filter_params = {
            'fromBlock': block_number_batch[0],
            'toBlock': block_number_batch[-1],
            'topics': [TRANSFER_EVENT_TOPIC]
        }

        if self.tokens is not None and len(self.tokens) > 0:
            filter_params['address'] = self.tokens
        # start_time = time.time()
        event_filter = self.web3.eth.filter(filter_params)
        events = event_filter.get_all_entries()

        # print(
        #     f"time to call event filter from {block_number_batch[0]} to {block_number_batch[-1]} is{time.time() - start_time}")
        # start = time()
        # loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        tasks = []
        # print(f"num events is{len(events)}")
        # start_time = time.time()
        for event in events:
            # tasks.append(loop.create_task(self._handler_event(event)))
            # tasks.append(self._handler_event(event))
            self._handler_event(event)
        # loop.run_until_complete(asyncio.wait(tasks))
        # loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        # loop.close()
        # end = time()
        # print(f'Time to run all transfer: {end - start:.2f} sec')
        # logger.info(f"time to hander {len(events)} is {time.time() - start_time}")
        self.web3.eth.uninstallFilter(event_filter.filter_id)

    def _handler_event(self, event):
        log = self.receipt_log_mapper.web3_dict_to_receipt_log(event)
        start_time1 = start_time = time.time()
        token_transfer = self.token_transfer_extractor.extract_transfer_from_log(log)
        print(f"time to extract token transfer is {time.time() - start_time}")
        if token_transfer is not None:
            token_transfer_dict = self.token_transfer_mapper.token_transfer_to_dict(token_transfer)
            # start_time = time()
            block_number = int(token_transfer_dict.get("block_number"))
            if not self.latest_block or block_number > self.block_thread_hole:
                start_time = time.time()
                self._update_balance(token_transfer_dict)
                logger.info(f"time to update balance is {time.time() - start_time}")
            # end_time = time()
            # print("run time to update balance:" + str(end_time - start_time))
            # print(token_transfer_dict)
            # self.token_dict_cache.append(token_transfer_dict)
            # start_time = time.time()
            self.item_exporter.export_item(token_transfer_dict)

            logger.info(f"Time to export item {time.time() - start_time1}")

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _update_balance(self, token_transfer_dict):
        # start_time = start_time_init = time.time()
        block_number = token_transfer_dict.get("block_number")
        token_address = token_transfer_dict.get("contract_address")
        from_address = token_transfer_dict.get("from_address")
        to_address = token_transfer_dict.get("to_address")
        wallets = []

        # print(f"time to init info {time.time() - start_time}")

        # from_wallet = self.database.get_wallet(from_address)
        # balances = from_wallet.get("balances")
        # if balances and balances.get(token_address.lower()) and from_wallet.get(
        #         "at_block_number") and from_wallet.get("at_block_number") < block_number:
        #     pre_from_balance = str(balances.get(token_address.lower()))
        #     from_balance = str(int(pre_from_balance) - int(token_transfer_dict.get("value")))
        # else:
        # pre_from_balance = self.ethTokenService.get_balance(token_address, from_address, block_number - 1)
        # data = {"data": None}
        # start_time = time.time()
        pre_from_balance = self.ethTokenService.get_balance(token_address, from_address, block_number - 1)
        # logger.info(f"get pre from balance {time.time() - start_time}")
        # pre_from_balance = data.get("data")
        # start_time = time.time()
        if pre_from_balance == None:
            # pre_from_balance = 0
            from_balance = 0
        else:
            from_balance = pre_from_balance - int(token_transfer_dict.get("value"))

        if from_balance >= 0:
            wallet = get_wallet_dict(from_address, str(from_balance), str(pre_from_balance), block_number,
                                     token_address)
            wallets.append(wallet)
        # print(f"time to append wallet{time.time() - start_time}")
        # to_wallet = self.database.get_wallet(to_address)
        # balances = to_wallet.get("balances")
        # if balances and balances.get(token_address.lower()) and to_wallet.get("at_block_number") and to_wallet.get(
        #         "at_block_number") < block_number:
        #     pre_to_balance = str(balances.get(token_address.lower()))
        #     to_balance = str(int(pre_to_balance) - int(token_transfer_dict.get("value")))
        # else:
        # pre_to_balance = self.ethTokenService.get_balance(token_address, to_address, block_number - 1
        # data = {"data": 0}
        # start_time = time.time()
        pre_to_balance = self.ethTokenService.get_balance(token_address, to_address, block_number - 1)
        # logger.info(f"get pre to balance {time.time() - start_time}")
        # pre_to_balance = data.get("data")
        # print("pre_to_balance ", pre_to_balance)
        # start_time = time.time()
        if pre_to_balance == None:
            # pre_to_balance = 0
            to_balance = 0
        else:
            to_balance = pre_to_balance + int(token_transfer_dict.get("value"))
        if to_balance >= 0:
            wallet = get_wallet_dict(to_address, str(to_balance), str(pre_to_balance), block_number, token_address)
            wallets.append(wallet)

        # print(f"time to append wallet{time.time() - start_time}")
        # start_time = time.time()
        token_transfer_dict["wallets"] = wallets
        # print(f"time to add wallets to token transfer dict {time.time() - start_time}")
        # print(f"all run time  {time.time() - start_time_init}")
        return token_transfer_dict

    def get_cache(self):
        return self.token_dict_cache

    def clean_cache(self):
        self.token_dict_cache = []
