import logging

from blockchainetl.streaming.streaming_utils import configure_logging, configure_signals
from ethereumetl.cli.stream import parse_entity_types, validate_entity_types, pick_random_provider_uri
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from knowledge_graph_etl.streaming.klg_streamer import Klg_Streamer
from knowledge_graph_etl.streaming.klg_streamer_adapter import KLGStreamerAdapter

last_synced_block_file = "./last_synced_block.txt"
lag = 0
# log_file = "./logs.txt"
log_file = None
entity_types = ','.join(EntityType.ALL_TOKEN)
output = None
provider_uri = "http://25.19.185.225:8545"
# provider_uri = "https://bsc-dataseed.binance.org/"
batch_size = 128
max_workers = 8

from os import path

if path.exists(last_synced_block_file):
    start_block = None
else:
    start_block = 2472670
    # start_block = 4378466
# start_block = None
period_seconds = 30
pid_file = None
block_batch_size = 16

configure_logging(log_file)
configure_signals()
entity_types = parse_entity_types(entity_types)
validate_entity_types(entity_types, output)

from ethereumetl.streaming.item_exporter_creator import create_item_exporter

# TODO: Implement fallback mechanism for provider uris instead of picking randomly
provider_uri = pick_random_provider_uri(provider_uri)
logging.info('Using ' + provider_uri)

streamer_adapter = KLGStreamerAdapter(
    batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
    item_exporter=create_item_exporter(output),
    batch_size=batch_size,
    max_workers=max_workers,
    entity_types=entity_types
)
streamer = Klg_Streamer(
    blockchain_streamer_adapter=streamer_adapter,
    last_synced_block_file=last_synced_block_file,
    lag=lag,
    start_block=start_block,
    period_seconds=period_seconds,
    block_batch_size=block_batch_size,
    pid_file=pid_file
)
streamer.stream()