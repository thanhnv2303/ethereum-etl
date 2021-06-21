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


import csv
import datetime
import json
import logging
import os
import shutil
from time import time

from web3 import Web3

from blockchainetl.file_utils import smart_open
from config.constant import EthKnowledgeGraphStreamerAdapterConstant, EventConstant, TimeUpdateConstant
from data_storage.memory_storage import MemoryStorage
from ethereumetl.csv_utils import set_max_field_size_limit
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_events_job import ExportEventsJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.jobs.export_tokens_job import ExportTokensJob
from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import blocks_and_transactions_item_exporter
from ethereumetl.jobs.exporters.event_item_exporter import event_item_exporter
from ethereumetl.jobs.exporters.receipts_and_logs_item_exporter import receipts_and_logs_item_exporter
from ethereumetl.jobs.exporters.token_transfers_item_exporter import token_transfers_item_exporter
from ethereumetl.jobs.exporters.tokens_item_exporter import tokens_item_exporter
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

logger = logging.getLogger('export_knowledge_graph_needed')


def is_log_filter_supported(provider_uri):
    return 'infura' not in provider_uri


def extract_csv_column_unique(input, output, column):
    set_max_field_size_limit()

    with smart_open(input, 'r') as input_file, smart_open(output, 'w') as output_file:
        reader = csv.DictReader(input_file)
        seen = set()  # set for fast O(1) amortized lookup
        for row in reader:
            if row[column] in seen:
                continue
            seen.add(row[column])
            output_file.write(row[column] + '\n')


def extract_dict_key_to_list(dict_list, key):
    return [a_dict[key] for a_dict in dict_list]


def export_knowledge_graph_needed_common(partitions, output_dir, provider_uri, max_workers, batch_size):
    for batch_start_block, batch_end_block, partition_dir in partitions:
        # # # start # # #

        start_time = time()

        padded_batch_start_block = str(batch_start_block).zfill(8)
        padded_batch_end_block = str(batch_end_block).zfill(8)
        block_range = '{padded_batch_start_block}-{padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )
        file_name_suffix = '{padded_batch_start_block}_{padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )

        # # # blocks_and_transactions # # #

        blocks_output_dir = '{output_dir}/blocks{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )
        os.makedirs(os.path.dirname(blocks_output_dir), exist_ok=True)

        transactions_output_dir = '{output_dir}/transactions{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )
        os.makedirs(os.path.dirname(transactions_output_dir), exist_ok=True)

        blocks_file = '{blocks_output_dir}/blocks_{file_name_suffix}.csv'.format(
            blocks_output_dir=blocks_output_dir,
            file_name_suffix=file_name_suffix,
        )
        transactions_file = '{transactions_output_dir}/transactions_{file_name_suffix}.csv'.format(
            transactions_output_dir=transactions_output_dir,
            file_name_suffix=file_name_suffix,
        )
        logger.info('Exporting blocks {block_range} to {blocks_file}'.format(
            block_range=block_range,
            blocks_file=blocks_file,
        ))
        logger.info('Exporting transactions from blocks {block_range} to {transactions_file}'.format(
            block_range=block_range,
            transactions_file=transactions_file,
        ))

        job = ExportBlocksJob(
            start_block=batch_start_block,
            end_block=batch_end_block,
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
            max_workers=max_workers,
            item_exporter=blocks_and_transactions_item_exporter(blocks_file, transactions_file),
            export_blocks=blocks_file is not None,
            export_transactions=transactions_file is not None)
        job.run()

        # # # token_transfers # # #

        token_transfers_file = None
        if is_log_filter_supported(provider_uri):
            token_transfers_output_dir = '{output_dir}/token_transfers{partition_dir}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
            )
            os.makedirs(os.path.dirname(token_transfers_output_dir), exist_ok=True)

            token_transfers_file = '{token_transfers_output_dir}/token_transfers_{file_name_suffix}.csv'.format(
                token_transfers_output_dir=token_transfers_output_dir,
                file_name_suffix=file_name_suffix,
            )
            logger.info('Exporting ERC20 transfers from blocks {block_range} to {token_transfers_file}'.format(
                block_range=block_range,
                token_transfers_file=token_transfers_file,
            ))

            job = ExportTokenTransfersJob(
                start_block=batch_start_block,
                end_block=batch_end_block,
                batch_size=batch_size,
                w3=ThreadLocalProxy(lambda: Web3(get_provider_from_uri(provider_uri))),
                item_exporter=token_transfers_item_exporter(token_transfers_file),
                max_workers=max_workers)
            job.run()

        # # # events in artifacts/event-abi # # #
        dir_path = "../../artifacts/event-abi"
        cur_path = os.path.dirname(os.path.realpath(__file__))
        for root, dirs, files in os.walk(cur_path + "/" + dir_path):

            for filename in files:
                file_path = cur_path + "/" + dir_path + "/" + filename

                with open(file_path) as json_file:
                    subscriber_event = json.load(json_file)

                event_name = subscriber_event.get("name")
                save_name = subscriber_event.get("saveName")
                if not save_name:
                    save_name = event_name
                inputs = subscriber_event.get("inputs")
                if is_log_filter_supported(provider_uri):
                    events_output_dir = '{output_dir}/events{partition_dir}'.format(
                        output_dir=output_dir,
                        partition_dir=partition_dir,
                    )
                    os.makedirs(os.path.dirname(events_output_dir), exist_ok=True)

                    event_file = '{token_transfers_output_dir}/{save_name}_{file_name_suffix}.csv'.format(
                        token_transfers_output_dir=events_output_dir,
                        save_name=save_name,
                        file_name_suffix=file_name_suffix,
                    )
                    logger.info(
                        'Exporting event {event_name} from blocks {block_range} to {token_transfers_file}'.format(
                            event_name=event_name,
                            block_range=block_range,
                            token_transfers_file=event_file,
                        ))
                    add_fields_to_export = []
                    for input in inputs:
                        if input:
                            add_fields_to_export.append(input.get("name"))

                    job = ExportEventsJob(
                        start_block=batch_start_block,
                        end_block=batch_end_block,
                        batch_size=batch_size,
                        web3=ThreadLocalProxy(lambda: Web3(get_provider_from_uri(provider_uri))),
                        item_exporter=event_item_exporter(event_file, add_fields_to_export),
                        max_workers=max_workers,
                        subscriber_event=subscriber_event,
                    )
                    job.run()

        # # # receipts_and_logs # # #

        cache_output_dir = '{output_dir}/.tmp{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )
        os.makedirs(os.path.dirname(cache_output_dir), exist_ok=True)

        transaction_hashes_file = '{cache_output_dir}/transaction_hashes_{file_name_suffix}.csv'.format(
            cache_output_dir=cache_output_dir,
            file_name_suffix=file_name_suffix,
        )
        logger.info('Extracting hash column from transaction file {transactions_file}'.format(
            transactions_file=transactions_file,
        ))
        extract_csv_column_unique(transactions_file, transaction_hashes_file, 'hash')

        receipts_output_dir = '{output_dir}/receipts{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )
        os.makedirs(os.path.dirname(receipts_output_dir), exist_ok=True)

        logs_output_dir = '{output_dir}/logs{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )
        os.makedirs(os.path.dirname(logs_output_dir), exist_ok=True)

        receipts_file = '{receipts_output_dir}/receipts_{file_name_suffix}.csv'.format(
            receipts_output_dir=receipts_output_dir,
            file_name_suffix=file_name_suffix,
        )
        logs_file = '{logs_output_dir}/logs_{file_name_suffix}.csv'.format(
            logs_output_dir=logs_output_dir,
            file_name_suffix=file_name_suffix,
        )
        logger.info('Exporting receipts and logs from blocks {block_range} to {receipts_file} and {logs_file}'.format(
            block_range=block_range,
            receipts_file=receipts_file,
            logs_file=logs_file,
        ))

        with smart_open(transaction_hashes_file, 'r') as transaction_hashes:
            job = ExportReceiptsJob(
                transaction_hashes_iterable=(transaction_hash.strip() for transaction_hash in transaction_hashes),
                batch_size=batch_size,
                batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
                max_workers=max_workers,
                item_exporter=receipts_and_logs_item_exporter(receipts_file, logs_file),
                export_receipts=receipts_file is not None,
                export_logs=logs_file is not None)
            job.run()

        # # # tokens # # #

        if token_transfers_file is not None:
            token_addresses_file = '{cache_output_dir}/token_addresses_{file_name_suffix}'.format(
                cache_output_dir=cache_output_dir,
                file_name_suffix=file_name_suffix,
            )
            logger.info('Extracting token_address from token_transfers file {token_transfers_file}'.format(
                token_transfers_file=token_transfers_file,
            ))
            extract_csv_column_unique(token_transfers_file, token_addresses_file, 'token_address')

            tokens_output_dir = '{output_dir}/tokens{partition_dir}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
            )
            os.makedirs(os.path.dirname(tokens_output_dir), exist_ok=True)

            tokens_file = '{tokens_output_dir}/tokens_{file_name_suffix}.csv'.format(
                tokens_output_dir=tokens_output_dir,
                file_name_suffix=file_name_suffix,
            )
            logger.info('Exporting tokens from blocks {block_range} to {tokens_file}'.format(
                block_range=block_range,
                tokens_file=tokens_file,
            ))

            with smart_open(token_addresses_file, 'r') as token_addresses:
                job = ExportTokensJob(
                    token_addresses_iterable=(token_address.strip() for token_address in token_addresses),
                    web3=ThreadLocalProxy(lambda: Web3(get_provider_from_uri(provider_uri))),
                    item_exporter=tokens_item_exporter(tokens_file),
                    max_workers=max_workers)
                job.run()

        # # # finish # # #
        shutil.rmtree(os.path.dirname(cache_output_dir))
        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=block_range,
            time_diff=time_diff,
        ))


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
    # provider_uris = [uri.strip() for uri in provider_uri.split(',')]
    # thread_local_proxys =[]
    # for provider in provider_uris:
    #     w3 = Web3(get_provider_from_uri(provider_uri))
    #     latest_block_num = w3.eth.blockNumber
    #     thread_local_proxy = ThreadLocalProxy(lambda: w3)
    #     thread_local_proxys.append(thread_local_proxy)
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

        # list_transactions_dict = job.get_cache()
        # transaction_hashes = extract_dict_key_to_list(list_transactions_dict, "hash")
        # print("transaction_hashes-------------------------------")
        # print(transaction_hashes)
        # # # token_transfers # # #
        # token_set = set()
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

        # print("token set in transfer")
        # print(token_set)
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
                    # add_fields_to_export = []
                    # for input in inputs:
                    #     if input:
                    #         add_fields_to_export.append(input.get(EventConstant.name))

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
                    # event_dicts = job.get_cache()
                    # contract_address = extract_dict_key_to_list(event_dicts, "contract_address")
                    # token_set.update(contract_address)

        # # # receipts_and_logs # # #
        # print("token set after get contact")
        # print(token_set)

        # job = ExportReceiptsJob(
        #     # transaction_hashes_iterable=(transaction_hash.strip() for transaction_hash in transaction_hashes),
        #     transaction_hashes_iterable=transaction_hashes,
        #     batch_size=batch_size,
        #     batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        #     max_workers=max_workers,
        #     item_exporter=item_exporter, )
        # job.run()

        # # # # tokens # # #
        now = datetime.datetime.now()
        memomory_storage = MemoryStorage.getInstance()
        if (now.hour == TimeUpdateConstant.token_update_hour and now.minute < TimeUpdateConstant.token_update_minute) \
                or memomory_storage.get("first_time"):
            job = ExportTokensJob(
                token_addresses_iterable=tokens,
                web3=thread_local_proxy,
                item_exporter=item_exporter,
                max_workers=max_workers,
                ethTokenService=ethTokenService
            )
            job.run()
            memomory_storage.set("first_time", False)
        # print("token exported")
        # print(job.get_cache())
        job.clean_cache()

        # # # finish # # #
        # shutil.rmtree(os.path.dirname(cache_output_dir))
        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=block_range,
            time_diff=time_diff,
        ))
