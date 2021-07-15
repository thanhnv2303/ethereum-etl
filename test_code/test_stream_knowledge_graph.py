import logging

from blockchainetl.streaming.streaming_utils import configure_logging, configure_signals
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_knowledge_graph_streamer_adapter import EthKnowledgeGraphStreamerAdapter
from ethereumetl.thread_local_proxy import ThreadLocalProxy

last_synced_block_file = "../data/last_synced_block.txt"
lag = 0
# log_file = "./data_test_stream/logs.txt"

output = None
provider_uri = "https://bsc-dataseed.binance.org/"
provider_uri = "http://25.19.185.225:8545"
provider_uri = "https://speedy-nodes-nyc.moralis.io/cd00f2fddfd96dc8ed17bf2a/bsc/mainnet/archive"
batch_size = 128
max_workers = 8

start_block = 9099895
# start_block = None
period_seconds = 10
pid_file = None
block_batch_size = 16

# configure_logging(log_file)
configure_signals()

from ethereumetl.streaming.item_exporter_creator import create_item_exporter
from blockchainetl.streaming.streamer import Streamer

# TODO: Implement fallback mechanism for provider uris instead of picking randomly
logging.info('Using ' + provider_uri)

streamer_adapter = EthKnowledgeGraphStreamerAdapter(
    provider_uri=provider_uri,
    batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
    item_exporter=create_item_exporter(output),
    batch_size=batch_size,
    max_workers=max_workers,
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
