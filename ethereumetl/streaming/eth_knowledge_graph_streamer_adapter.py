import os

from web3 import Web3
from web3.middleware import geth_poa_middleware

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.cli.export_knowledge_graph_needed import get_partitions
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_knowledge_graph_needed_common import export_knowledge_graph_needed_with_item_exporter
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.jobs.export_tokens_job import ExportTokensJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator
from ethereumetl.thread_local_proxy import ThreadLocalProxy


class EthKnowledgeGraphStreamerAdapter:
    def __init__(
            self,
            provider_uri,
            batch_web3_provider,
            item_exporter=ConsoleItemExporter(),
            tokens_filter_file="../../artifacts/token_filter",
            tokens=None,
            batch_size=100,
            max_workers=8,
            provider_uris=None,
            entity_types=tuple(EntityType.ALL_FOR_STREAMING)):
        # self.batch_web3_provider = batch_web3_provider
        self.provider_uri = provider_uri
        self.batch_web3_provider = batch_web3_provider
        self.w3 = Web3(batch_web3_provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()

        cur_path = os.path.dirname(os.path.realpath(__file__))
        self.tokens_filter_file = cur_path + "/" + tokens_filter_file
        self.tokens = tokens
        self.provider_uris = provider_uris

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):

        return int(self.w3.eth.getBlock("latest").number)

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
                                                             item_exporter, tokens=tokens,
                                                             provider_uris=self.provider_uris)


    def close(self):
        self.item_exporter.close()
