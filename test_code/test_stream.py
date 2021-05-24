import logging

from blockchainetl.streaming.streaming_utils import configure_logging, configure_signals
from ethereumetl.cli.stream import parse_entity_types, validate_entity_types, pick_random_provider_uri
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

last_synced_block_file = "./data_test_stream/last_synced_block.txt"
lag = 0
log_file = "./data_test_stream/logs.txt"
entity_types = ','.join(EntityType.ALL_TOKEN)
output = None
provider_uri = "https://bsc-dataseed.binance.org/"
batch_size = 100
max_workers = 5

start_block = 858560
# start_block = None
period_seconds = 10
pid_file = None
block_batch_size = 10

configure_logging(log_file)
configure_signals()
entity_types = parse_entity_types(entity_types)
validate_entity_types(entity_types, output)

from ethereumetl.streaming.item_exporter_creator import create_item_exporter
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from blockchainetl.streaming.streamer import Streamer

# TODO: Implement fallback mechanism for provider uris instead of picking randomly
provider_uri = pick_random_provider_uri(provider_uri)
logging.info('Using ' + provider_uri)

streamer_adapter = EthStreamerAdapter(
    batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
    item_exporter=create_item_exporter(output),
    batch_size=batch_size,
    max_workers=max_workers,
    entity_types=entity_types
)
streamer = Streamer(
    blockchain_streamer_adapter=streamer_adapter,
    last_synced_block_file=last_synced_block_file,
    lag=lag,
    start_block=start_block,
    period_seconds=period_seconds,
    block_batch_size=block_batch_size,
    pid_file=pid_file
)
streamer.stream()