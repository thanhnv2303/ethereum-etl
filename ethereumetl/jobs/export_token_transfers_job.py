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
from config.constant import EventFilterConstant, TokenConstant, TransactionConstant
from data_storage.wallet_filter_storage import WalletFilterMemoryStorage
from data_storage.wallet_storage import WalletMemoryStorage
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.jobs.export_tokens_job import clean_user_provided_content
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.mappers.token_transfer_mapper import EthTokenTransferMapper
from ethereumetl.mappers.wallet_mapper import get_wallet_dict
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor, TRANSFER_EVENT_TOPIC
from ethereumetl.utils import validate_range
from services.wallet_services import get_balance_at_block_smart_contract, update_balance_to_cache

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
            ethTokenService=None

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
        if ethTokenService:
            self.ethTokenService = ethTokenService
        else:
            self.ethTokenService = EthTokenService(w3, clean_user_provided_content)

        self.database = database
        self.latest_block = latest_block
        if latest_block:
            self.block_thread_hole = int(latest_block * 0.8)

    def _start(self):
        self.wallet_storage = WalletMemoryStorage.getInstance()
        self.wallet_filter = WalletFilterMemoryStorage.getInstance()
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
            EventFilterConstant.fromBlock: block_number_batch[0],
            EventFilterConstant.toBlock: block_number_batch[-1],
            EventFilterConstant.topics: [TRANSFER_EVENT_TOPIC]
        }

        if self.tokens is not None and len(self.tokens) > 0:
            filter_params[TokenConstant.address] = self.tokens
        event_filter = self.web3.eth.filter(filter_params)
        events = event_filter.get_all_entries()

        for event in events:
            self._handler_event(event)
        self.web3.eth.uninstallFilter(event_filter.filter_id)

    def _handler_event(self, event):
        log = self.receipt_log_mapper.web3_dict_to_receipt_log(event)
        start_time1 = start_time = time.time()
        token_transfer = self.token_transfer_extractor.extract_transfer_from_log(log)
        logger.info(f"time to extract token transfer is {time.time() - start_time}")
        if token_transfer is not None:
            token_transfer_dict = self.token_transfer_mapper.token_transfer_to_dict(token_transfer)
            # start_time = time()
            block_number = int(token_transfer_dict.get(TokenConstant.block_number))
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
        block_number = token_transfer_dict.get(TransactionConstant.block_number)
        token_address = token_transfer_dict.get(TokenConstant.contract_address)
        from_address = token_transfer_dict.get(TransactionConstant.from_address)
        to_address = token_transfer_dict.get(TransactionConstant.to_address)

        if self.wallet_filter.get(from_address) or self.wallet_filter.get(to_address):
            return
        
        wallets = []
        start_time = time.time()
        pre_from_balance, _wallet = get_balance_at_block_smart_contract(wallet_storage=self.wallet_storage,
                                                                        ethService=self.ethTokenService,
                                                                        address=from_address,
                                                                        token_address=token_address,
                                                                        block_number=block_number)
        logger.info(f"time to get balance of {from_address} at block num {block_number} is {time.time() - start_time}")
        if pre_from_balance == None:
            from_balance = 0
        else:
            from_balance = pre_from_balance - int(token_transfer_dict.get(TransactionConstant.value))

            ## update to cache
            update_balance_to_cache(wallet_storage=self.wallet_storage, _wallet=_wallet,
                                    token_address=token_address,
                                    balance=from_balance)

        if from_balance >= 0:
            wallet = get_wallet_dict(from_address, str(from_balance), str(pre_from_balance), block_number,
                                     token_address)
            wallets.append(wallet)

        start_time = time.time()
        pre_to_balance, _wallet = get_balance_at_block_smart_contract(wallet_storage=self.wallet_storage,
                                                                      ethService=self.ethTokenService,
                                                                      address=token_address,
                                                                      token_address=token_address,
                                                                      block_number=block_number)

        logger.info(f"time to get balance of {from_address} at block num {block_number} is {time.time() - start_time}")
        if pre_to_balance == None:
            to_balance = 0
        else:
            to_balance = pre_to_balance + int(token_transfer_dict.get(TransactionConstant.value))

            ## update to cache
            update_balance_to_cache(wallet_storage=self.wallet_storage, _wallet=_wallet,
                                    token_address=token_address,
                                    balance=to_balance)

        if to_balance >= 0:
            wallet = get_wallet_dict(to_address, str(to_balance), str(pre_to_balance), block_number, token_address)
            wallets.append(wallet)
        token_transfer_dict[TransactionConstant.wallets] = wallets

        return token_transfer_dict

    def get_cache(self):
        return self.token_dict_cache

    def clean_cache(self):
        self.token_dict_cache = []
