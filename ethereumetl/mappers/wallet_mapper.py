from config.constant import WalletConstant, TransactionConstant


def get_wallet_dict(address, balance, pre_balance, block_number, token_address="0x"):
    return {
        WalletConstant.address: address,
        WalletConstant.balance: str(balance),
        WalletConstant.pre_balance: str(pre_balance),
        TransactionConstant.block_number: block_number
    }


def wallet_append_lending_info(wallet, supply, borrow):
    wallet[WalletConstant.supply] = str(supply)
    wallet[WalletConstant.borrow] = str(borrow)
