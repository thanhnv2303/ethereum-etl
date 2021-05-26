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
            max_workers=5,
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
                                                             item_exporter, tokens=tokens)

    def _export_blocks_and_transactions(self, start_block, end_block):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(item_types=['block', 'transaction'])
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION)
        )
        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items('block')
        transactions = blocks_and_transactions_item_exporter.get_items('transaction')
        return blocks, transactions

    def _export_receipts_and_logs(self, transactions):
        exporter = InMemoryItemExporter(item_types=['receipt', 'log'])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(transaction['hash'] for transaction in transactions),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG)
        )
        job.run()
        receipts = exporter.get_items('receipt')
        logs = exporter.get_items('log')
        return receipts, logs

    def _extract_token_transfers(self, logs):
        exporter = InMemoryItemExporter(item_types=['token_transfer'])
        job = ExtractTokenTransfersJob(
            logs_iterable=logs,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter)
        job.run()
        token_transfers = exporter.get_items('token_transfer')
        return token_transfers

    def _export_token_transfers(self, start_block, end_block):
        exporter = InMemoryItemExporter(item_types=['token_transfer'])
        job = ExportTokenTransfersJob(
            start_block=start_block,
            end_block=end_block,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter)
        job.run()
        token_transfers = exporter.get_items('token_transfer')
        return token_transfers

    def _export_traces(self, start_block, end_block):
        exporter = InMemoryItemExporter(item_types=['trace'])
        job = ExportTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        traces = exporter.get_items('trace')
        return traces

    def _export_contracts(self, traces):
        exporter = InMemoryItemExporter(item_types=['contract'])
        job = ExtractContractsJob(
            traces_iterable=traces,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        contracts = exporter.get_items('contract')
        return contracts

    def _extract_tokens(self, contracts):
        exporter = InMemoryItemExporter(item_types=["token"])
        job = ExtractTokensJob(
            contracts_iterable=contracts,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        tokens = exporter.get_items("token")
        print("tokens")
        return tokens

    def _extract_tokens_from_transfer(self, token_transfers):
        exporter = InMemoryItemExporter(item_types=['token'])
        token_address = set()

        for token_transfer in token_transfers:
            token_address.add(token_transfer.get("token_address"))

        job = ExportTokensJob(
            token_addresses_iterable=token_address,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter
        )
        job.run()
        tokens = exporter.get_items('token')
        return tokens

    def _should_export(self, entity_type):
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(EntityType.LOG)

        if entity_type == EntityType.RECEIPT:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(EntityType.TOKEN_TRANSFER)

        if entity_type == EntityType.LOG:
            return EntityType.LOG in self.entity_types or self._should_export(EntityType.TOKEN_TRANSFER)

        if entity_type == EntityType.TOKEN_TRANSFER:
            return EntityType.TOKEN_TRANSFER in self.entity_types

        if entity_type == EntityType.TRACE:
            return EntityType.TRACE in self.entity_types or self._should_export(EntityType.CONTRACT)

        if entity_type == EntityType.CONTRACT:
            return EntityType.CONTRACT in self.entity_types or self._should_export(EntityType.TOKEN)

        if entity_type == EntityType.TOKEN:
            return EntityType.TOKEN in self.entity_types

        raise ValueError('Unexpected entity type ' + entity_type)

    def calculate_item_ids(self, items):
        for item in items:
            item['item_id'] = self.item_id_calculator.calculate(item)

    def calculate_item_timestamps(self, items):
        for item in items:
            item['item_timestamp'] = self.item_timestamp_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()
