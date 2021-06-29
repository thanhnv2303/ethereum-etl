# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import datetime
import json
import logging
import os
from time import time

from web3 import Web3

from config.constant import EthKnowledgeGraphStreamerAdapterConstant, EventConstant, TimeUpdateConstant
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_events_job import ExportEventsJob
from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.jobs.export_tokens_job import ExportTokensJob
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

logger = logging.getLogger('export_knowledge_graph_needed')


def is_log_filter_supported(provider_uri):
    return 'infura' not in provider_uri


def export_klg_with_item_exporter(partitions, provider_uri, max_workers, batch_size,
                                  item_exporter,
                                  event_abi_dir=EthKnowledgeGraphStreamerAdapterConstant.event_abi_dir_default,
                                  tokens=None,
                                  provider_uris=None,
                                  first_time=True,
                                  w3=None,
                                  ethTokenService=None,
                                  ethLendingService=None
                                  ):
    if not w3:
        w3 = Web3(get_provider_from_uri(provider_uri))
    latest_block_num = w3.eth.blockNumber
    thread_local_proxy = ThreadLocalProxy(lambda: w3)

    for batch_start_block, batch_end_block, partition_dir in partitions:
        # # # start # # #
        start_time = time()
        padded_batch_start_block = str(batch_start_block).zfill(8)
        padded_batch_end_block = str(batch_end_block).zfill(8)

        block_range = '{padded_batch_start_block}-{padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )

        # # # blocks_and_transactions # # #
        job = ExportBlocksJob(
            start_block=batch_start_block,
            end_block=batch_end_block,
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
            max_workers=max_workers,
            item_exporter=item_exporter,
            latest_block=latest_block_num,
            provider_uris=provider_uris
        )
        job.run()

        # # # token_transfers # # #
        if is_log_filter_supported(provider_uri):
            job = ExportTokenTransfersJob(
                start_block=batch_start_block,
                end_block=batch_end_block,
                batch_size=batch_size,
                w3=w3,
                item_exporter=item_exporter,
                max_workers=max_workers,
                tokens=tokens,
                latest_block=latest_block_num,
                provider_uris=provider_uris,
                ethTokenService=ethTokenService
            )
            job.run()

        # # # events in artifacts/event-abi # # #

        dir_path = event_abi_dir
        cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"
        for root, dirs, files in os.walk(cur_path + dir_path):

            for filename in files:
                file_path = cur_path + dir_path + "/" + filename

                with open(file_path) as json_file:
                    subscriber_event = json.load(json_file)

                has_get_balance = subscriber_event.get(EventConstant.isLending)

                if is_log_filter_supported(provider_uri):
                    job = ExportEventsJob(
                        start_block=batch_start_block,
                        end_block=batch_end_block,
                        batch_size=batch_size,
                        web3=thread_local_proxy,
                        item_exporter=item_exporter,
                        max_workers=max_workers,
                        subscriber_event=subscriber_event,
                        is_lending=has_get_balance,
                        tokens=tokens,
                        ethTokenService=ethTokenService,
                        ethLendingService=ethLendingService
                    )
                    job.run()

        # # # # tokens # # #
        now = datetime.datetime.now()
        if (now.hour == TimeUpdateConstant.token_update_hour and now.minute < TimeUpdateConstant.token_update_minute) \
                or first_time:
            job = ExportTokensJob(
                token_addresses_iterable=tokens,
                web3=thread_local_proxy,
                item_exporter=item_exporter,
                max_workers=max_workers,
                ethTokenService=ethTokenService
            )
            job.run()
            first_time = False
        job.clean_cache()

        # # # finish # # #
        # shutil.rmtree(os.path.dirname(cache_output_dir))
        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=block_range,
            time_diff=time_diff,
        ))
