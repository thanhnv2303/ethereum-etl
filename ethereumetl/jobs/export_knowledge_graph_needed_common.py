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

from config.constant import EthKnowledgeGraphStreamerAdapterConstant, TestPerformanceConstant, \
    MemoryStorageKeyConstant
from data_storage.memory_storage import MemoryStorage
from data_storage.memory_storage_test_performance import MemoryStoragePerformance
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_subscriber_events_job import ExportSubscriberEventsJob
from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.jobs.export_tokens_job import ExportTokensJob
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from services.time_service import round_timestamp_to_date

logger = logging.getLogger('export_knowledge_graph_needed')


def is_log_filter_supported(provider_uri):
    return 'infura' not in provider_uri


def export_klg_with_item_exporter(partitions, provider_uri, max_workers, batch_size,
                                  item_exporter,
                                  event_abi_dir=EthKnowledgeGraphStreamerAdapterConstant.event_abi_dir_default,
                                  tokens=None,
                                  provider_uris=None,
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
        job_start_time = time()
        padded_batch_start_block = str(batch_start_block).zfill(8)
        padded_batch_end_block = str(batch_end_block).zfill(8)

        """
        init local storage to calculate time test performance
        """
        local_storage = MemoryStoragePerformance.getInstance()
        local_storage.set(TestPerformanceConstant.get_transfer_filter_time, 0)
        local_storage.set(TestPerformanceConstant.get_event_filter_time, 0)
        local_storage.set(TestPerformanceConstant.get_balance_smart_contract_time, 0)
        local_storage.set(TestPerformanceConstant.get_balance_time, 0)
        local_storage.set(TestPerformanceConstant.get_block_by_number_json, 0)
        local_storage.set(TestPerformanceConstant.get_lending_info_trava_time, 0)
        local_storage.set(TestPerformanceConstant.get_lending_info_vtoken_time, 0)
        local_storage.set(TestPerformanceConstant.read_mongo_time, 0)
        local_storage.set(TestPerformanceConstant.write_mongo_time, 0)
        local_storage.set(TestPerformanceConstant.transaction_number, 0)
        local_storage.set(TestPerformanceConstant.transaction_handler_time, 0)

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
        # token_set = set()
        start_transfer = time()
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
            # token_transfers_dict = job.get_cache()
            # token_addresses = extract_dict_key_to_list(token_transfers_dict, "contract_address")
            # token_set.update(token_addresses)
            logger.info(f"time to export transfer {time() - start_transfer}s")
        # print("token set in transfer")
        # print(token_set)
        # # # events in artifacts/event-abi # # #

        dir_path = event_abi_dir
        cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"
        subscriber_events = []
        for root, dirs, files in os.walk(cur_path + dir_path):

            for filename in files:
                file_path = cur_path + dir_path + "/" + filename

                with open(file_path) as json_file:
                    subscriber_event = json.load(json_file)
                    subscriber_events.append(subscriber_event)
                # has_get_balance = subscriber_event.get(EventConstant.isLending)

                # if is_log_filter_supported(provider_uri):
                #     # add_fields_to_export = []
                #     # for input in inputs:
                #     #     if input:
                #     #         add_fields_to_export.append(input.get(EventConstant.name))
                #
                #     job = ExportEventsJob(
                #         start_block=batch_start_block,
                #         end_block=batch_end_block,
                #         batch_size=batch_size,
                #         web3=thread_local_proxy,
                #         item_exporter=item_exporter,
                #         max_workers=max_workers,
                #         subscriber_event=subscriber_event,
                #         is_lending=has_get_balance,
                #         tokens=tokens,
                #         ethTokenService=ethTokenService,
                #         ethLendingService=ethLendingService
                #     )
                #     job.run()
        start_export_event = time()
        job = ExportSubscriberEventsJob(
            batch_start_block,
            batch_end_block,
            batch_size,
            thread_local_proxy,
            item_exporter,
            max_workers,
            subscriber_events,
            tokens=tokens,
            ethTokenService=ethTokenService,
            ethLendingService=ethLendingService
        )
        job.run()
        logger.info(f"time to extract all events {time() - start_export_event}s")
        # # # # tokens # # #
        now = datetime.datetime.now()

        start_time = time()
        checkpoint_storage = MemoryStorage.getInstance()

        checkpoint = checkpoint_storage.get(MemoryStorageKeyConstant.checkpoint)
        timestamp = round(start_time)
        timestamp_day = round_timestamp_to_date(timestamp)
        if not checkpoint or checkpoint != timestamp_day:
            job = ExportTokensJob(
                token_addresses_iterable=tokens,
                web3=thread_local_proxy,
                item_exporter=item_exporter,
                max_workers=max_workers,
                ethTokenService=ethTokenService
            )
            job.run()

            checkpoint_storage.set(MemoryStorageKeyConstant.checkpoint, timestamp_day)
        # print("token exported")
        # print(job.get_cache())
        job.clean_cache()

        """
        Show total time to call from provider
        """
        if local_storage.get_calculate_performance():
            get_transfer_filter_time = local_storage.get(TestPerformanceConstant.get_transfer_filter_time)
            get_event_filter_time = local_storage.get(TestPerformanceConstant.get_event_filter_time)
            get_balance_smart_contract_time = local_storage.get(TestPerformanceConstant.get_balance_smart_contract_time)
            get_balance_time = local_storage.get(TestPerformanceConstant.get_balance_time)
            get_block_by_number_json = local_storage.get(TestPerformanceConstant.get_block_by_number_json)
            get_lending_info_trava_time = local_storage.get(TestPerformanceConstant.get_lending_info_trava_time)
            get_lending_info_vtoken_time = local_storage.get(TestPerformanceConstant.get_lending_info_vtoken_time)
            read_mongo_time = local_storage.get(TestPerformanceConstant.read_mongo_time)
            write_mongo_time = local_storage.get(TestPerformanceConstant.write_mongo_time)
            transaction_handler_time = local_storage.get(TestPerformanceConstant.transaction_handler_time)
            transaction_number = local_storage.get(TestPerformanceConstant.transaction_number)
            total_time_call_provider = get_transfer_filter_time + \
                                       get_event_filter_time + \
                                       get_balance_smart_contract_time + \
                                       get_balance_time + get_block_by_number_json + \
                                       get_lending_info_trava_time + \
                                       get_lending_info_vtoken_time
            total_time = total_time_call_provider + read_mongo_time + write_mongo_time + transaction_handler_time
            logger.info(f"Exporting blocks {block_range} get_transfer_filter_time take {get_transfer_filter_time}")
            logger.info(f"Exporting blocks {block_range} get_event_filter_time take {get_event_filter_time}")
            logger.info(
                f"Exporting blocks {block_range} get_balance_smart_contract_time take {get_balance_smart_contract_time}")
            logger.info(f"Exporting blocks {block_range} get_balance_time take {get_balance_time}")
            logger.info(f"Exporting blocks {block_range} get_block_by_number_json take {get_block_by_number_json}")
            logger.info(
                f"Exporting blocks {block_range} get_lending_info_trava_time take {get_lending_info_trava_time}")
            logger.info(
                f"Exporting blocks {block_range} get_lending_info_vtoken_time take {get_lending_info_vtoken_time}")
            logger.info(f"Exporting blocks {block_range} total time call provider take {total_time_call_provider}")
            logger.info(f"Exporting blocks {block_range} read_mongo_time take {read_mongo_time}")
            logger.info(f"Exporting blocks {block_range} write_mongo_time take {write_mongo_time}")
            logger.info(f"Exporting blocks {block_range} transaction_handler_time take {transaction_handler_time}")
            logger.info(f"Exporting blocks {block_range} transaction_number take {transaction_number}")
            logger.info(f"Exporting blocks {block_range} total time to process {total_time}")

        # # # finish # # #
        # shutil.rmtree(os.path.dirname(cache_output_dir))
        end_time = time()
        time_diff = end_time - job_start_time
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=block_range,
            time_diff=time_diff,
        ))
