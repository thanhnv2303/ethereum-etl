import logging

from ethereumetl.service.eth_event_service import EthEvent
from ethereumetl.service.token_transfer_extractor import split_to_words, word_to_address
from ethereumetl.utils import to_normalized_address, hex_to_dec

logger = logging.getLogger(__name__)


class EthEventExtractor(object):
    def extract_event_from_log(self, receipt_log, event_subscriber):
        topics = receipt_log.topics
        if topics is None or len(topics) < 1:
            logger.warning("Topics are empty in log {} of transaction {}".format(receipt_log.log_index,
                                                                                 receipt_log.transaction_hash))
            return None

        if event_subscriber.topic_hash == topics[0]:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            list_params_in_order = event_subscriber.list_params_in_order
            # if the number of topics and fields in data part != len(list_params_in_order), then it's a weird event
            num_params = len(list_params_in_order)
            topics_with_data = topics_with_data[1:]
            if len(topics_with_data) != num_params:
                logger.warning("The number of topics and data parts is not equal to {} in log {} of transaction {}"
                               .format(str(num_params), receipt_log.log_index, receipt_log.transaction_hash))
                return None

            event = EthEvent()
            event.contract_address = to_normalized_address(receipt_log.address)
            event.transaction_hash = receipt_log.transaction_hash
            event.log_index = receipt_log.log_index
            event.block_number = receipt_log.block_number
            event.event_type = event_subscriber.name
            for i in range(num_params):
                param_i = list_params_in_order[i]
                name = param_i.get("name")
                type = param_i.get("type")
                data = topics_with_data[i]
                event.params[name] = str(decode_data_by_type(data, type))
            return event

        return None


def decode_data_by_type(data, type):
    if is_integers(type):
        return hex_to_dec(data)
    elif type == "address":
        return word_to_address(data)
    else:
        return data


def is_integers(type):
    return type == "uint256" or type == "uinit128" or type == "uinit64" or type == "uinit32" or type == "uinit16" or type == "uinit8" or type == "uinit" \
           or type == "int256" or type == "init128" or type == "init64" or type == "init32" or type == "init16" or type == "init8" or type == "init"
