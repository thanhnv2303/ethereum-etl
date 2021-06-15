from config.constant import WalletConstant, TransactionConstant


def get_wallet_dict(address, balance, pre_balance, block_number, token_address="0x"):
    return {
        WalletConstant.address: address,
        WalletConstant.balance: balance,
        WalletConstant.pre_balance: pre_balance,
        TransactionConstant.block_number: block_number
    }
