import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, './'))

import logging
from os import path

from config.config import BuildKnowledgeGraph
from ethereumetl.service.eth_service import get_latest_block
from blockchainetl.streaming.streaming_utils import configure_signals, configure_logging
from ethereumetl.cli.stream import pick_random_provider_uri
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_knowledge_graph_streamer_adapter import EthKnowledgeGraphStreamerAdapter
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.streaming.item_exporter_creator import create_item_exporter
from blockchainetl.streaming.streamer import Streamer

if __name__ == '__main__':
    ### get environment variables

    log_file = str(BuildKnowledgeGraph.LOG_FILE)
    provider_uri = str(BuildKnowledgeGraph.PROVIDER_URI)
    lag = int(BuildKnowledgeGraph.LAG)
    batch_size = int(BuildKnowledgeGraph.BATCH_SIZE)
    max_workers = int(BuildKnowledgeGraph.MAX_WORKERS)
    start_block = int(BuildKnowledgeGraph.START_BLOCK)
    period_seconds = int(BuildKnowledgeGraph.PERIOD_SECONDS)
    pid_file = str(BuildKnowledgeGraph.PID_FILE)
    block_batch_size = int(BuildKnowledgeGraph.BLOCK_BATCH_SIZE)
    tokens_filter_file = str(BuildKnowledgeGraph.TOKENS_FILTER_FILE)
    event_abi_dir = str(BuildKnowledgeGraph.EVENT_ABI_DIR)

    # configure_logging(log_file)
    configure_signals()
    if log_file:
        configure_logging(log_file)

    cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../"

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    provider_uris = [uri.strip() for uri in provider_uri.split(',')]
    # check provider is can connect
    output = "knowledge_graph"

    provider_uri = pick_random_provider_uri(provider_uri)

    last_synced_block_file = cur_path + "data/last_synced_block.txt"
    if path.exists(last_synced_block_file):
        start_block = None
    elif not start_block:
        start_block = get_latest_block(provider_uri)

    logging.info('Using ' + provider_uri)

    streamer_adapter = EthKnowledgeGraphStreamerAdapter(
        provider_uri=provider_uri,
        tokens_filter_file=tokens_filter_file,
        tokens=None,
        event_abi_dir=event_abi_dir,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        item_exporter=create_item_exporter(output),
        batch_size=batch_size,
        max_workers=max_workers,
        provider_uris=provider_uris
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
