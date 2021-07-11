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
from config.constant import EventConstant
from config.event_lending_constant import EventLendingConstant
from data_storage.wallet_filter_storage import WalletFilterMemoryStorage
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.jobs.export_events_job import ExportEventsJob
from ethereumetl.jobs.export_tokens_job import clean_user_provided_content
from ethereumetl.mappers.event_mapper import EthEventMapper
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.service.eth_lending_service import EthLendingService
from ethereumetl.service.eth_token_service import EthTokenService
from ethereumetl.service.event_extractor import EthEventExtractor
from utils.utils import validate_range

logger = logging.getLogger(__name__)


class ExportSubscriberEventsJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            web3,
            item_exporter,
            max_workers,
            subscriber_events,
            tokens=None,
            ethTokenService=None,
            ethLendingService=None
    ):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.web3 = web3
        self.tokens = tokens
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.thread_local_proxy = web3
        self.max_workers = int(max_workers/4)
        self.batch_work_executor = BatchWorkExecutor(batch_size, self.max_workers)

        self.receipt_log_mapper = EthReceiptLogMapper()
        self.event_mapper = EthEventMapper()
        self.event_extractor = EthEventExtractor()
        self.subscriber_events = subscriber_events
        self.topic = ""
        self.event_subscriber = None
        self.address_name_field = []

        self.eth_events_dict_cache = []
        if ethTokenService:
            self.ethTokenService = ethTokenService
        else:
            self.ethTokenService = EthTokenService(web3, clean_user_provided_content)

        if ethLendingService:
            self.ethLendingService = ethLendingService
        else:
            self.ethLendingService = EthLendingService(web3, clean_user_provided_content)

    def _start(self):
        self.wallet_filter = WalletFilterMemoryStorage.getInstance()
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            self.subscriber_events,
            self._export_batch,
            total_items=len(self.subscriber_events)
        )
        # self._export_batch(subscriber_events=self.subscriber_events)

    def _export_batch(self, subscriber_events):
        for subscriber_event in subscriber_events:
            has_get_balance = subscriber_event.get(EventConstant.isLending)
            job = ExportEventsJob(
                start_block=self.start_block,
                end_block=self.end_block,
                batch_size=self.batch_size,
                web3=self.thread_local_proxy,
                item_exporter=self.item_exporter,
                max_workers=self.max_workers,
                subscriber_event=subscriber_event,
                is_lending=has_get_balance,
                tokens=self.tokens,
                ethTokenService=self.ethTokenService,
                ethLendingService=self.ethLendingService
            )
            job.run()

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

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
