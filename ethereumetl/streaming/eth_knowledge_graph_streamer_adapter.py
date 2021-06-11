import os

from web3 import Web3
from web3.middleware import geth_poa_middleware

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from ethereumetl.cli.export_knowledge_graph_needed import get_partitions
from ethereumetl.jobs.export_knowledge_graph_needed_common import export_knowledge_graph_needed_with_item_exporter
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator


class EthKnowledgeGraphStreamerAdapter:

    def __init__(
            self,
            provider_uri,
            batch_web3_provider,
            item_exporter=ConsoleItemExporter(),
            tokens_filter_file="artifacts/token_filter",
            event_abi_dir="artifacts/event-abi",
            tokens=None,
            entity_types=None,
            batch_size=100,
            max_workers=8,
            provider_uris=None,
            first_time=True
    ):
        # self.batch_web3_provider = batch_web3_provider
        self.provider_uri = provider_uri
        self.batch_web3_provider = batch_web3_provider
        self.w3 = Web3(batch_web3_provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()

        # change all path from this project root
        cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"
        self.tokens_filter_file = cur_path + tokens_filter_file
        self.tokens = tokens
        self.provider_uris = provider_uris
        self.event_abi_dir = event_abi_dir
        self.first_time = first_time

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        return int(self.w3.eth.blockNumber)

    def export_all(self, start_block, end_block):
        partition_batch_size = 10000
        partitions = get_partitions(str(start_block), str(end_block), partition_batch_size, self.provider_uri)
        item_exporter = self.item_exporter
        with open(self.tokens_filter_file, "r") as file:
            tokens_list = file.read().splitlines()
            tokens = []
            for token in tokens_list:
                tokens.append(Web3.toChecksumAddress(token))
            export_knowledge_graph_needed_with_item_exporter(partitions, self.provider_uri, self.max_workers,
                                                             self.batch_size,
                                                             item_exporter,
                                                             event_abi_dir=self.event_abi_dir,
                                                             tokens=tokens,
                                                             provider_uris=self.provider_uris,
                                                             first_time=self.first_time,
                                                             w3 = self.w3
                                                             )

    def close(self):
        self.item_exporter.close()
