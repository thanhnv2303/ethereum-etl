class BlockConstant:
    gas_limit = "gas_limit"
    gas_used = "gas_used"


class TransactionConstant:
    gas = "gas"
    gas_price = "gas_price"
    value = "value"
    input = "input"
    hash = "hash"
    transaction_hash = "transaction_hash"
    wallets = "wallets"
    block_number = "block_number"
    from_address = "from_address"
    to_address = "to_address"


class TokenConstant:
    value = "value"
    contract_address = "contract_address"
    type = "type"
    native_token = "0x"
    event_type = "event_type"
    total_supply = "total_supply"
    address = "address"
    symbol = "symbol"
    decimals = 'decimals'
    name = "name"
    block_number = "block_number"


class TokenTypeConstant:
    Transfer = "Transfer"


class WalletConstant:
    address = "address"
    at_block_number = "at_block_number"
    address_nowhere = '0x0000000000000000000000000000000000000000'
    balance = "new_balance_of_concerning_token"
    pre_balance = "old_balance_of_concerning_token"
    balances = "balance"
    supply = "supply"
    borrow = "borrow"


class ExportItemConstant:
    type = "type"


class ExportItemTypeConstant:
    transaction = "transaction"
    block = "block"
    token_transfer = "token_transfer"
    event = "event"
    token = "token"


class LoggerConstant:
    KnowledgeGraphExporter = "KnowledgeGraphExporter"
    ExportBlocksJob = "ExportBlocksJob"
    EthService = "EthService"
    EthLendingService = "EthLendingService"


class EthKnowledgeGraphStreamerAdapterConstant:
    tokens_filter_file_default = "artifacts/token_filter"
    event_abi_dir_default = "artifacts/event-abi"
    batch_size_default = 100
    max_workers_default = 8
    partition_batch_size_default = 1000


class EventConstant:
    name = "name"
    saveName = "saveName"
    isLending = "isLending"
    inputs = "inputs"
    type = "type"
    event = "event"
    log_index = "log_index"
    event_type = "event_type"
    contract_address = "contract_address"


class EventInputConstant:
    name = "name"
    type = "type"
    address = "address"
    indexed = "indexed"


class EventFilterConstant:
    fromBlock = "fromBlock"
    toBlock = "toBlock"
    topics = "topics"


class TimeUpdateConstant:
    token_update_hour = 3
    token_update_minute = 5


class MongoIndexConstant:
    tx_id = "tx_id"
    transfer_tx_id = "transfer_tx_id"
    transfer_block_number = "transfer_block_number"
    wallet_address = "wallet_address"

class LendingTypeConfig:
    lendingType ="lendingType"
    VTOKEN = "VTOKEN"
    TRAVA = "TRAVA"
    ERC20 = "ERC20"
