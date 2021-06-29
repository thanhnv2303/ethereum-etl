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
import logging
import time

from web3 import Web3

from blockchainetl.jobs.base_job import BaseJob
from config.config import FilterConfig
from config.constant import LoggerConstant, TransactionConstant, TokenConstant, TestPerformanceConstant
from data_storage.memory_storage import MemoryStorage
from data_storage.wallet_filter_storage import WalletFilterMemoryStorage
from data_storage.wallet_storage import WalletMemoryStorage
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.block_mapper import EthBlockMapper
from ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ethereumetl.mappers.wallet_mapper import get_wallet_dict
from ethereumetl.service.eth_service import EthService
from services.json_rpc_requests import generate_get_block_by_number_json_rpc
from services.wallet_services import get_balance_at_block, update_balance_to_cache
from utils.boolean_utils import to_bool
from utils.utils import rpc_response_batch_to_results, validate_range

logger = logging.getLogger(LoggerConstant.ExportBlocksJob)


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
        self.local_storage = MemoryStorage.getInstance()

        self.filter_for_lending = to_bool(FilterConfig.FILTER_FOR_LENDING)

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
        start_time = time.time()
        blocks_rpc = list(generate_get_block_by_number_json_rpc(block_number_batch, self.export_transactions))
        response = self.batch_web3_provider.make_batch_request(json.dumps(blocks_rpc))
        results = rpc_response_batch_to_results(response)
        end_time = time.time()
        get_block_by_number_time = self.local_storage.get(TestPerformanceConstant.get_block_by_number_json)
        self.local_storage.set(TestPerformanceConstant.get_block_by_number_json,
                               get_block_by_number_time + (time.time() - start_time))
        logger.info(
            f"time to get info blocks {block_number_batch[0]} - {block_number_batch[-1]} is {end_time - start_time}")
        blocks = [self.block_mapper.json_dict_to_block(result) for result in results]
        for block in blocks:
            self._export_block(block)
        logger.info(
            f"total time to process {block_number_batch[0]} - {block_number_batch[-1]} blocks  is {time.time() - start_time}")

    def _export_block(self, block):
        if self.export_blocks:
            block_dict = self.block_mapper.block_to_dict(block)
            self.blocks_cache.append(block_dict)
            self.item_exporter.export_item(block_dict)

        if self.export_transactions:
            start_time = time.time()
            for tx in block.transactions:
                transaction_dict = self.transaction_mapper.transaction_to_dict(tx)
                self._handler_transaction(transaction_dict)

            logger.info(f"total processed transaction {len(block.transactions)} take : {time.time() - start_time}s")

    def _handler_transaction(self, transaction_dict):
        block_number = int(transaction_dict.get(TransactionConstant.block_number))
        start_time = time.time()
        if True or not self.latest_block or block_number > self.block_thread_hole:
            self._update_balance(transaction_dict)
            logger.debug(f"time to update balance " + str(time.time() - start_time))
        self.item_exporter.export_item(transaction_dict)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _update_balance(self, transaction_dict):
        # return transaction_dict
        if transaction_dict.get(TransactionConstant.input) == TokenConstant.native_token:
            start_time_0 = time.time()
            block_number = transaction_dict.get(TransactionConstant.block_number)
            from_address = transaction_dict.get(TransactionConstant.from_address)
            to_address = transaction_dict.get(TransactionConstant.to_address)

            if self.filter_for_lending and not self.wallet_filter.get(from_address) \
                    and not self.wallet_filter.get(to_address):
                return

            value = transaction_dict.get(TransactionConstant.value)
            if value:
                value = int(value)
            else:
                value = 0
            start_time = time.time()

            token_address = TokenConstant.native_token
            pre_from_balance, _wallet = get_balance_at_block(wallet_storage=self.wallet_storage,
                                                             ethService=self.ethService,
                                                             address=from_address, block_number=block_number - 1)
            end_time = time.time()
            logger.info(f"time to call get balance native token of {from_address} is" + str(
                end_time - start_time))
            if pre_from_balance == None:
                # pre_from_balance = 0
                from_balance = 0
            else:
                from_balance = pre_from_balance - value

                update_balance_to_cache(wallet_storage=self.wallet_storage, _wallet=_wallet,
                                        token_address=token_address,
                                        balance=from_balance)

            start_time = time.time()

            pre_to_balance, _wallet = get_balance_at_block(wallet_storage=self.wallet_storage,
                                                           ethService=self.ethService,
                                                           address=to_address, block_number=block_number - 1)
            end_time = time.time()
            logger.info(f"time to call get balance native token of " + from_address + "  is" + str(
                end_time - start_time))
            if pre_to_balance == None:
                # pre_to_balance = 0
                to_balance = 0
            else:
                to_balance = pre_to_balance + transaction_dict.get(TransactionConstant.value)
                update_balance_to_cache(wallet_storage=self.wallet_storage, _wallet=_wallet,
                                        token_address=token_address,
                                        balance=to_balance)

            wallets = []
            if to_balance >= 0:
                wallet = get_wallet_dict(to_address, str(to_balance), str(pre_to_balance), block_number)
                wallets.append(wallet)
            if from_balance >= 0:
                wallet = get_wallet_dict(from_address, str(from_balance), str(pre_from_balance), block_number)
                wallets.append(wallet)

            transaction_dict[TransactionConstant.wallets] = wallets
            logger.debug(f" Time to process transaction {time.time() - start_time_0}")

    def get_cache(self):
        return self.blocks_cache + self.transactions_cache

    def get_transactions_cache(self):
        return self.transactions_cache

    def get_blocks_cache(self):
        return self.blocks_cache

    def clean_cache(self):
        self.blocks_cache = []
        self.transactions_cache = []
