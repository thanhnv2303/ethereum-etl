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
from ethereumetl.mappers.event_mapper import EthEventMapper
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.service.eth_event_service import get_topic_filter, get_list_params_in_order, EventSubscriber
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
            tokens=None):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

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
        self._init_events_subscription()

    def _init_events_subscription(self):
        event_abi = self.subscriber_event
        if event_abi.get("type") == "event":
            method_signature_hash = get_topic_filter(event_abi)
            list_params_in_order = get_list_params_in_order(event_abi)
            event_name = event_abi.get("name")
            event_subscriber = EventSubscriber(method_signature_hash, event_name, list_params_in_order)
            self.event_subscriber = event_subscriber
            self.topic = method_signature_hash

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        assert len(block_number_batch) > 0
        # https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getfilterlogs
        filter_params = {
            'fromBlock': block_number_batch[0],
            'toBlock': block_number_batch[-1],
            'topics': [self.topic]
        }
        if self.tokens is not None and len(self.tokens) > 0:
            filter_params['address'] = self.tokens

        event_filter = self.web3.eth.filter(filter_params)
        events = event_filter.get_all_entries()
        for event in events:
            log = self.receipt_log_mapper.web3_dict_to_receipt_log(event)
            eth_event = self.event_extractor.extract_event_from_log(log, self.event_subscriber)
            if eth_event is not None:
                self.item_exporter.export_item(self.event_mapper.eth_event_to_dict(eth_event))

        self.web3.eth.uninstallFilter(event_filter.filter_id)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
