from config.constant import ExportItemConstant, ExportItemTypeConstant, EventConstant, TransactionConstant
from ethereumetl.service.eth_event_service import EthEvent


class EthEventMapper(object):
    def eth_event_to_dict(self, eth_event: EthEvent):
        d1 = {
            ExportItemConstant.type: ExportItemTypeConstant.event,
            EventConstant.event_type: eth_event.event_type,
            EventConstant.contract_address: eth_event.contract_address,
            TransactionConstant.transaction_hash: eth_event.transaction_hash,
            EventConstant.log_index: eth_event.log_index,
            TransactionConstant.block_number: eth_event.block_number,
        }
        d2 = eth_event.params
        return {**d1, **d2}
