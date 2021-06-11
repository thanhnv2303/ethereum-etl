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
import json
import time

from web3 import Web3

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.json_rpc_requests import generate_get_block_by_number_json_rpc
from ethereumetl.mappers.block_mapper import EthBlockMapper
from ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ethereumetl.mappers.wallet_mapper import get_wallet_dict
from ethereumetl.service.eth_service import EthService
from ethereumetl.utils import rpc_response_batch_to_results, validate_range


# Exports blocks and transactions
class ExportBlocksJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter,
            export_blocks=True,
            export_transactions=True,
            latest_block=None,
            provider_uris=None,
            web3=None
    ):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block
        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions

        self.latest_block = latest_block
        if latest_block:
            self.block_thread_hole = int(latest_block * 0.8)

        if not self.export_blocks and not self.export_transactions:
            raise ValueError('At least one of export_blocks or export_transactions must be True')

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()
        self.blocks_cache = []
        self.transactions_cache = []
        if web3:
            self.w3 = web3
        else:
            self.w3 = Web3(batch_web3_provider)
        self.ethService = EthService(self.w3, provider_uris)
        # self.ethService = EthService(batch_web3_provider)

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        blocks_rpc = list(generate_get_block_by_number_json_rpc(block_number_batch, self.export_transactions))
        response = self.batch_web3_provider.make_batch_request(json.dumps(blocks_rpc))
        results = rpc_response_batch_to_results(response)
        blocks = [self.block_mapper.json_dict_to_block(result) for result in results]

        for block in blocks:
            self._export_block(block)

    def _export_block(self, block):
        if self.export_blocks:
            block_dict = self.block_mapper.block_to_dict(block)
            self.blocks_cache.append(block_dict)
            self.item_exporter.export_item(block_dict)

        if self.export_transactions:
            # loop = asyncio.new_event_loop()
            # asyncio.set_event_loop(loop)
            # tasks = []
            # print("num transactions at block "+str(block.number)+ " : "+ str(len(block.transactions)))
            start_time = time.time()
            for tx in block.transactions:
                transaction_dict = self.transaction_mapper.transaction_to_dict(tx)
                self._handler_transaction(transaction_dict)
                # tasks.append(loop.create_task(self._handler_transaction(transaction_dict)))
                # tasks.append(self._handler_transaction(transaction_dict))

            print("time to update " + str(len(block.transactions)) + " is " + str(time.time() - start_time))
            # loop.run_until_complete(asyncio.wait(tasks))
            # loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            # loop.close()

    def _handler_transaction(self, transaction_dict):
        block_number = int(transaction_dict.get("block_number"))
        start_time = time.time()
        if True or not self.latest_block or block_number > self.block_thread_hole:
            self._update_balance(transaction_dict)
            print("time to update balance " + str(time.time() - start_time))
        # print(transaction_dict)
        # self.transactions_cache.append(transaction_dict)
        self.item_exporter.export_item(transaction_dict)
        print("time to handle transaction " + str(time.time() - start_time))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _update_balance(self, transaction_dict):
        # return transaction_dict
        if transaction_dict.get("input") == "0x":
            block_number = transaction_dict.get("block_number")
            from_address = transaction_dict.get("from_address")
            to_address = transaction_dict.get("to_address")
            value = transaction_dict.get("value")
            if value:
                value = int(value)
            else:
                value = 0
            # start_time = time.time()
            pre_from_balance = self.ethService.get_balance(from_address, block_number - 1)
            # end_time = time.time()
            # print("time to call get balance native token of " + from_address + " at contract " + " is" + str(
            #     end_time - start_time))
            if pre_from_balance == None:
                pre_from_balance = 0
                from_balance = 0
            else:
                from_balance = str(int(pre_from_balance) - value)

            pre_to_balance = self.ethService.get_balance(to_address, block_number - 1)
            if pre_to_balance == None:
                pre_to_balance = 0
                to_balance = 0
            else:
                to_balance = str(int(pre_to_balance) + transaction_dict.get("value"))

            wallets = []
            if int(to_balance) >= 0:
                wallet = get_wallet_dict(to_address, to_balance, pre_to_balance, block_number)
                wallets.append(wallet)
            if int(from_balance) >= 0:
                wallet = get_wallet_dict(from_address, from_balance, pre_from_balance, block_number)
                wallets.append(wallet)

            transaction_dict["wallets"] = wallets

    def get_cache(self):
        return self.blocks_cache + self.transactions_cache

    def get_transactions_cache(self):
        return self.transactions_cache

    def get_blocks_cache(self):
        return self.blocks_cache

    def clean_cache(self):
        self.blocks_cache = []
        self.transactions_cache = []
