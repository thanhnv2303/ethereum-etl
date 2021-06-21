import time

from config.constant import WalletConstant, TokenConstant
from ethereumetl.service.eth_service import EthService
from ethereumetl.service.eth_token_service import EthTokenService


def get_balance_at_block(wallet_storage, ethService: EthService, address, block_number):
    _wallet = wallet_storage.get(address)
    token_address = TokenConstant.native_token
    timestamp = int(time.time())
    if not _wallet:
        _wallet = {}
    if _wallet.get(WalletConstant.balance) and _wallet.get(WalletConstant.balance).get(token_address) and _wallet.get(
            WalletConstant.update_checkpoint) > timestamp:
        balance = _wallet.get(token_address)
    else:
        balance = ethService.get_balance(address, block_number)
        _wallet[WalletConstant.update_checkpoint] = timestamp + WalletConstant.update_checkpoint_next_time

    return balance, _wallet


def get_balance_at_block_smart_contract(wallet_storage, ethService: EthTokenService, address, token_address,
                                        block_number):
    _wallet = wallet_storage.get(address)
    timestamp = int(time.time())
    if not _wallet:
        _wallet = {WalletConstant.address: address}
    if _wallet.get(WalletConstant.balance) and _wallet.get(WalletConstant.balance).get(token_address) and _wallet.get(
            WalletConstant.update_checkpoint) > timestamp:
        balance = _wallet.get(token_address)
    else:
        balance = ethService.get_balance(token_address, address, block_number)
        _wallet[WalletConstant.update_checkpoint] = timestamp + WalletConstant.update_checkpoint_next_time

    return balance, _wallet


def update_balance_to_cache(wallet_storage, _wallet, token_address, balance):
    if not _wallet.get(WalletConstant.balance):
        _wallet[WalletConstant.balance] = {}
    _wallet[WalletConstant.balance][token_address] = balance
    wallet_storage.set(_wallet.get(WalletConstant.address), _wallet)
