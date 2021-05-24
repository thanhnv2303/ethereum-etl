def get_wallet_dict(address, balance, block_number, token_address="0x"):
    return {
        "address": address,
        "balance": balance,
        "block_number": block_number
    }
