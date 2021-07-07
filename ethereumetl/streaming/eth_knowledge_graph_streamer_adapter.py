import os

from web3 import Web3
from web3.middleware import geth_poa_middleware

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.databasse.mongo_db import Database
from config.config import FilterConfig
from config.constant import EthKnowledgeGraphStreamerAdapterConstant, WalletConstant
from data_storage.wallet_filter_storage import WalletFilterMemoryStorage
from ethereumetl.jobs.export_knowledge_graph_needed_common import export_klg_with_item_exporter
from ethereumetl.service.eth_lending_service import EthLendingService
from ethereumetl.service.eth_token_service import EthTokenService
from services.partition_service import get_partitions
from utils.boolean_utils import to_bool


class EthKnowledgeGraphStreamerAdapter:

    def __init__(
            self,
            provider_uri,
            batch_web3_provider,
            item_exporter=ConsoleItemExporter(),
            tokens_filter_file=EthKnowledgeGraphStreamerAdapterConstant.tokens_filter_file_default,
            event_abi_dir=EthKnowledgeGraphStreamerAdapterConstant.event_abi_dir_default,
            tokens=None,
            batch_size=EthKnowledgeGraphStreamerAdapterConstant.batch_size_default,
            max_workers=EthKnowledgeGraphStreamerAdapterConstant.max_workers_default,
            provider_uris=None
    ):

        self.provider_uri = provider_uri
        self.batch_web3_provider = batch_web3_provider
        self.w3 = Web3(batch_web3_provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers

        # change all path from this project root
        cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"
        self.tokens_filter_file = cur_path + tokens_filter_file
        self.tokens = tokens
        self.provider_uris = provider_uris
        self.event_abi_dir = event_abi_dir
        self.ethTokenService = EthTokenService(self.w3, clean_user_provided_content)
        self.ethLendingService = EthLendingService(self.w3, clean_user_provided_content)
        self.filter_for_lending = to_bool(FilterConfig.FILTER_FOR_LENDING)
        if self.filter_for_lending:
            self.get_wallet_filter()

    def open(self):
        self.item_exporter.open()

    def get_wallet_filter(self):
        self.database = Database()
        self.wallet_filter = WalletFilterMemoryStorage.getInstance()

        wallets = self.database.get_all_wallet()
        for wallet in wallets:
            address = wallet.get(WalletConstant.address)
            self.wallet_filter.set(address, wallet)

    def get_current_block_number(self):
        return int(self.w3.eth.blockNumber)

    def export_all(self, start_block, end_block):
        partition_batch_size = EthKnowledgeGraphStreamerAdapterConstant.partition_batch_size_default
        partitions = get_partitions(str(start_block), str(end_block), partition_batch_size, self.provider_uri)
        item_exporter = self.item_exporter
        with open(self.tokens_filter_file, "r") as file:
            tokens_list = file.read().splitlines()
            tokens = []
            for token in tokens_list:
                tokens.append(Web3.toChecksumAddress(token))
            export_klg_with_item_exporter(partitions, self.provider_uri, self.max_workers,
                                          self.batch_size,
                                          item_exporter,
                                          event_abi_dir=self.event_abi_dir,
                                          tokens=tokens,
                                          provider_uris=self.provider_uris,
                                          w3=self.w3,
                                          ethTokenService=self.ethTokenService,
                                          ethLendingService=self.ethLendingService
                                          )

    def close(self):
        self.item_exporter.close()


ASCII_0 = 0


def clean_user_provided_content(content):
    if isinstance(content, str):
        # This prevents this error in BigQuery
        # Error while reading data, error message: Error detected while parsing row starting at position: 9999.
        # Error: Bad character (ASCII 0) encountered.
        return content.translate({ASCII_0: None})
    else:
        return content
