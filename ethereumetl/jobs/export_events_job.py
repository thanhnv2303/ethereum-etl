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
from config.constant import EventConstant, EventFilterConstant, TokenConstant, TransactionConstant, LendingTypeConstant
from config.event_lending_constant import EventLendingConstant
from data_storage.wallet_filter_storage import WalletFilterMemoryStorage
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.jobs.export_tokens_job import clean_user_provided_content
from ethereumetl.mappers.event_mapper import EthEventMapper
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.mappers.wallet_mapper import get_wallet_dict, wallet_append_lending_info
from ethereumetl.service.eth_event_service import get_topic_filter, get_list_params_in_order, EventSubscriber, \
    get_all_address_name_field
from ethereumetl.service.eth_lending_service import EthLendingService
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.service.event_extractor import EthEventExtractor
from ethereumetl.utils import validate_range

logger = logging.getLogger(__name__)


class ExportEventsJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            web3,
            item_exporter,
            max_workers,
            subscriber_event,
            is_lending=False,
            tokens=None,
            ethTokenService=None,
            ethLendingService=None
    ):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self._is_lending = is_lending
        self.web3 = web3
        self.tokens = tokens
        self.item_exporter = item_exporter

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

        self.receipt_log_mapper = EthReceiptLogMapper()
        self.event_mapper = EthEventMapper()
        self.event_extractor = EthEventExtractor()
        self.subscriber_event = subscriber_event
        self.topic = ""
        self.event_subscriber = None
        self.address_name_field = []
        self._init_events_subscription()

        self.eth_events_dict_cache = []
        if ethTokenService:
            self.ethTokenService = ethTokenService
        else:
            self.ethTokenService = EthTokenService(web3, clean_user_provided_content)

        if ethLendingService:
            self.ethLendingService = ethLendingService
        else:
            self.ethLendingService = EthLendingService(web3, clean_user_provided_content)

        if subscriber_event.get(LendingTypeConstant.lendingType):
            self.token_type = subscriber_event.get(LendingTypeConstant.lendingType)
        else:
            self.token_type = LendingTypeConstant.ERC20

    def _init_events_subscription(self):
        event_abi = self.subscriber_event
        if event_abi.get(EventConstant.type) == EventConstant.event:
            method_signature_hash = get_topic_filter(event_abi)
            list_params_in_order = get_list_params_in_order(event_abi)
            event_name = event_abi.get(EventConstant.name)
            event_subscriber = EventSubscriber(method_signature_hash, event_name, list_params_in_order)
            self.event_subscriber = event_subscriber
            self.topic = method_signature_hash
            self.address_name_field = get_all_address_name_field(event_abi)

    def _start(self):
        self.wallet_filter = WalletFilterMemoryStorage.getInstance()
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        # self.eth_events_dict_cache = []
        assert len(block_number_batch) > 0
        # https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getfilterlogs
        filter_params = {
            EventFilterConstant.fromBlock: block_number_batch[0],
            EventFilterConstant.toBlock: block_number_batch[-1],
            EventFilterConstant.topics: [self.topic]
        }
        if self.tokens is not None and len(self.tokens) > 0:
            filter_params[TokenConstant.address] = self.tokens

        event_filter = self.web3.eth.filter(filter_params)
        events = event_filter.get_all_entries()
        for event in events:
            log = self.receipt_log_mapper.web3_dict_to_receipt_log(event)
            eth_event = self.event_extractor.extract_event_from_log(log, self.event_subscriber)
            if eth_event is not None:
                eth_event_dict = self.event_mapper.eth_event_to_dict(eth_event)
                # logger.info("eth_event_dict ")
                # logger.info(eth_event_dict)
                self._update_wallet(eth_event_dict)
                self.item_exporter.export_item(eth_event_dict)

        self.web3.eth.uninstallFilter(event_filter.filter_id)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _update_wallet(self, eth_event_dict):
        wallets = []
        contract_address = eth_event_dict.get(TokenConstant.contract_address)
        block_num = eth_event_dict.get(TransactionConstant.block_number)
        for address_field in self.address_name_field:
            address = eth_event_dict.get(address_field)

            if self._is_lending:
                asset_address = get_asset_address(eth_event_dict)
                balance, pre_balance, supply, borrow, unit_token = self.ethLendingService.get_lending_info(
                    contract_address, address, block_num,
                    self.token_type, asset_address)
                if balance != None and supply != None and borrow != None:
                    wallet = get_wallet_dict(address, balance, pre_balance, block_num, unit_token)
                    wallet_append_lending_info(wallet, supply, borrow)
                    wallets.append(wallet)

                    self.wallet_filter.set(address, wallet)

        eth_event_dict[TransactionConstant.wallets] = wallets

        return eth_event_dict

    def get_cache(self):
        return self.eth_events_dict_cache

    def clean_cache(self):
        self.eth_events_dict_cache = []


def get_asset_address(eth_event_dict):
    list_asset_name = EventLendingConstant.AssetName
    for asset_name in list_asset_name:
        if eth_event_dict.get(asset_name):
            return eth_event_dict.get(asset_name)

    return None
