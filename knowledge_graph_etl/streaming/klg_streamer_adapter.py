import os

from web3 import Web3
from web3.middleware import geth_poa_middleware

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator
from knowledge_graph_etl.exporter.database.database import Database


class KLGStreamerAdapter:
    def __init__(
            self,
            batch_web3_provider,
            item_exporter=ConsoleItemExporter(),
            database=Database(),
            batch_size=100,
            max_workers=5,
            entity_types=tuple(EntityType.ALL_FOR_STREAMING),
            tokens_filter_file="../../artifacts/token_filter"
    ):
        # self.batch_web3_provider = batch_web3_provider
        self.batch_web3_provider = batch_web3_provider
        self.w3 = Web3(batch_web3_provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.database = database
        cur_path = os.path.dirname(os.path.realpath(__file__))
        self.tokens_filter_file = cur_path + "/" + tokens_filter_file

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        return int(self.w3.eth.getBlock("latest").number)

    def export_all(self, start_block, end_block):
        with open(self.tokens_filter_file, "r") as file:
            tokens = file.read().splitlines()
        print(start_block)
        print(end_block)
        print("tokens:::::::::::::")
        # print(tokens)
        # self.item_exporter.export_items(tokens)

    def close(self):
        self.item_exporter.close()
