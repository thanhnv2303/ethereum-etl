import os
import sys

from services.log_services import config_log

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, './'))

from ethereumetl.service.eth_service import get_latest_block
import logging
from os import path

from blockchainetl.streaming.streaming_utils import configure_signals
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.eth_knowledge_graph_streamer_adapter import EthKnowledgeGraphStreamerAdapter
from ethereumetl.thread_local_proxy import ThreadLocalProxy

if __name__ == '__main__':

    last_synced_block_file = "./data/last_synced_block.txt"
    lag = 0
    # log_file = "./data_stream/logs.txt"
    output = "knowledge_graph"
    from os.path import expanduser

    home = expanduser("~")
    geth_ipc_file = home + "/bsc-full-sync/node/geth.ipc"

    if not os.path.exists(geth_ipc_file):
        # provider_uri = "http://25.19.185.225:8545"
        # provider_uri = "https://bsc-dataseed.binance.org/"
        # provider_uri = "https://bsc-dataseed1.defibit.io/"
        # provider_uri = "https://bsc-dataseed1.ninicoin.io/"
        # provider_uri = "https://bsc-dataseed2.defibit.io/"
        # provider_uri = "https://bsc-dataseed4.binance.org/"
        provider_uri = "https://speedy-nodes-nyc.moralis.io/cd00f2fddfd96dc8ed17bf2a/eth/mainnet"
        # provider_uri = "wss://speedy-nodes-nyc.moralis.io/cd00f2fddfd96dc8ed17bf2a/eth/mainnet/ws"
        # provider_uri =  "https://data-seed-prebsc-1-s1.binance.org:8545/"
    else:
        provider_uri = "file:///" + geth_ipc_file + ",http://35.240.140.92:8545"

    batch_size = 4
    max_workers = 8

    # start_block = 4678378
    if path.exists(last_synced_block_file):
        start_block = None
    else:
        start_block = get_latest_block(provider_uri)
        # start_block = 7771629
    period_seconds = 2
    pid_file = None
    block_batch_size = 32

    # configure_logging(log_file)
    configure_signals()

    from ethereumetl.streaming.item_exporter_creator import create_item_exporter
    from blockchainetl.streaming.streamer import Streamer

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    provider_uris = [uri.strip() for uri in provider_uri.split(',')]
    config_log()
    logging.info('Using ' + provider_uri)
    tokens_filter_file = "artifacts/smart_contract_filter/token_filter_ether"
    streamer_adapter = EthKnowledgeGraphStreamerAdapter(
        provider_uri=provider_uri,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        item_exporter=create_item_exporter(output),
        batch_size=batch_size,
        max_workers=max_workers,
        tokens_filter_file=tokens_filter_file,
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
