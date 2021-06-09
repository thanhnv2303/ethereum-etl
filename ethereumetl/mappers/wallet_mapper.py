def get_wallet_dict(address, balance, pre_balance, block_number, token_address="0x"):
    return {
        "address": address,
        "balance": balance,
        "pre_balance": pre_balance,
        "block_number": block_number
    }
