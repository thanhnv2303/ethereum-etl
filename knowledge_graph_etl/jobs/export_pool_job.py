from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.token_mapper import EthTokenMapper
from knowledge_graph_etl.services.eth_token_type_service import EthTokenTypeService


class ExportPoolJob(BaseJob):
    def __init__(self, web3, item_exporter, token_addresses_iterable, max_workers):
        self.item_exporter = item_exporter
        self.token_addresses_iterable = token_addresses_iterable
        self.batch_work_executor = BatchWorkExecutor(1, max_workers)

        self.token_service = EthTokenTypeService(web3, clean_user_provided_content)
        self.token_mapper = EthTokenMapper()
        self.tokens_cache = []

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.token_addresses_iterable, self._export_tokens)

    def _export_tokens(self, token_addresses):
        for token_address in token_addresses:
            self._export_token(token_address)

    def _export_token(self, token_address, block_number=None):
        token = self.token_service.get_token(token_address)
        token.block_number = block_number
        token_dict = self.token_mapper.token_to_dict(token)
        self.tokens_cache.append(token_dict)
        self.item_exporter.export_item(token_dict)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def get_cache(self):
        return self.tokens_cache

    def clean_cache(self):
        self.tokens_cache = []


ASCII_0 = 0


def clean_user_provided_content(content):
    if isinstance(content, str):
        # This prevents this error in BigQuery
        # Error while reading data, error message: Error detected while parsing row starting at position: 9999.
        # Error: Bad character (ASCII 0) encountered.
        return content.translate({ASCII_0: None})
    else:
        return content
