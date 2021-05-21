from eth_utils import keccak


def get_topic_filter(event_abi):
    input_string = event_abi.get("name") + "("
    for input in event_abi.get("inputs"):
        input_string += input.get("type") + ","
    input_string = input_string[:-1] + ")"
    hash = keccak(text=input_string)
    return '0x' + hash.hex()


def get_list_params_in_order(event_abi):
    indexed = []
    non_indexed = []
    for input in event_abi.get("inputs"):
        if input.get("indexed"):
            indexed.append(input)
        else:
            non_indexed.append(input)
    return indexed + non_indexed


class EventSubscriber:
    def __init__(self, topic_hash, name, list_params_in_order):
        self.topic_hash = topic_hash
        self.name = name
        self.list_params_in_order = list_params_in_order


class EthEvent(object):
    def __init__(self):
        self.contract_address = None
        self.transaction_hash = None
        self.log_index = None
        self.block_number = None
        self.params = {}
        self.event_type = None
