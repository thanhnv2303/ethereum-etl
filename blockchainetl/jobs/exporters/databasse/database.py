class Database(object):
    """Manages connection to  database and makes async queries
    """

    def __init__(self):
        self._conn = None

        # self._create_index()

    def _create_index(self):
        pass

    def update_block(self, block):
        pass

    def update_transaction(self, tx):
        pass

    def update_transaction_transfer(self, tx):
        pass

    def update_wallet(self, wallet):
        return

    def replace_wallet(self, wallet):
        return

    def get_wallet(self, address):
        return

    def insert_to_token_collection(self, token_address, event):
        return

    def update_token(self, token):
        return
