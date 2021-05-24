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
import logging

from blockchainetl.jobs.base_job import BaseJob
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
            web3,
            item_exporter,
            max_workers,
            tokens=None):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.web3 = web3
        self.tokens = tokens
        self.item_exporter = item_exporter

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

        self.receipt_log_mapper = EthReceiptLogMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()
        self.token_transfer_extractor = EthTokenTransferExtractor()
        self.token_dict_cache = []
        self.ethTokenService = EthTokenService(web3, clean_user_provided_content)

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

        event_filter = self.web3.eth.filter(filter_params)
        events = event_filter.get_all_entries()
        for event in events:
            log = self.receipt_log_mapper.web3_dict_to_receipt_log(event)
            token_transfer = self.token_transfer_extractor.extract_transfer_from_log(log)
            if token_transfer is not None:
                token_transfer_dict = self.token_transfer_mapper.token_transfer_to_dict(token_transfer)
                self._update_balance(token_transfer_dict)
                self.token_dict_cache.append(token_transfer_dict)
                self.item_exporter.export_item(token_transfer_dict)

        self.web3.eth.uninstallFilter(event_filter.filter_id)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _update_balance(self, token_transfer_dict):
        block_number = token_transfer_dict.get("block_number")
        token_address = token_transfer_dict.get("token_address")
        from_address = token_transfer_dict.get("from_address")
        to_address = token_transfer_dict.get("to_address")
        wallets = []
        from_balance = self.ethTokenService.get_balance(token_address, from_address, block_number)
        if from_balance:
            wallet = get_wallet_dict(from_address, from_balance, block_number, token_address)
            wallets.append(wallet)

        to_balance = self.ethTokenService.get_balance(token_address, to_address, block_number)
        if to_balance:
            wallet = get_wallet_dict(to_address, to_balance, block_number, token_address)
            wallets.append(wallet)
        token_transfer_dict["wallets"] = wallets

        return token_transfer_dict

    def get_cache(self):
        return self.token_dict_cache

    def clean_cache(self):
        self.token_dict_cache = []
