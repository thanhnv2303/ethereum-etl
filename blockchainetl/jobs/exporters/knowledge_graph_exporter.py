# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from blockchainetl.jobs.exporters.databasse.mongo_db import Database


class KnowledgeGraphExporter:
    def __init__(self):
        self.mapping_handler = {
            "transaction": self._transaction_handler,
            "block": self._block_handler,
            "token_transfer": self._token_transfer_handler,
            "event": self._event_handler,
            "token": self._token_handler
        }
        self.data_base = Database()

    def open(self):
        pass

    def export_items(self, items):
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        handler = self.mapping_handler.get(item.get("type"))
        if handler:
            handler(item)

    def close(self):
        pass

    def _block_handler(self, item):

        item["gas_limit"] = str(item.get("gas_limit"))
        item["gas_used"] = str(item.get("gas_used"))
        self.data_base.update_block(item)

    def _transaction_handler(self, item):
        item["gas"] = str(item.get("gas"))
        item["gas_price"] = str(item.get("gas_price"))
        item["value"] = str(item.get("value"))
        if item.get("input") == "0x":
            item["transaction_hash"] = item.pop("hash")
            self._update_wallet_and_item(item, "0x")
            self.data_base.update_transaction_transfer(item)
        self.data_base.update_transaction(item)

    def _token_transfer_handler(self, item):
        item["value"] = str(item.get("value"))
        token_address = item.get("token_address")
        self._update_wallet_and_item(item, token_address)
        self.data_base.insert_to_token_collection(token_address, item)

    def _event_handler(self, item):
        item["value"] = str(item.get("value"))
        contract_address = item.get("contract_address")
        self._update_wallet_and_item(item, contract_address)
        self.data_base.insert_to_token_collection(contract_address, item)

    def _token_handler(self, item):
        item["total_supply"] = str(item.get("total_supply"))
        self.data_base.update_token(item)

    def _update_wallet_and_item(self, item, balance_address):

        for wallet in item.get("wallets"):
            address = wallet.get("address")
            wallet_in_db = self.data_base.get_wallet(address)

            balances = wallet_in_db.get("balances")
            if not balances:
                balances = {}
            wallet["balance"] = str(wallet.get("balance"))
            wallet["pre_balance"] = str(wallet.get("pre_balance"))
            balances[balance_address] = wallet.get("balance")
            wallet_in_db["balances"] = balances
            wallet["balances"] = balances

            txs = wallet_in_db.get("transactions")
            if not txs:
                txs = []
            if item.get("transaction_hash") not in txs:
                txs.append(item.get("transaction_hash"))

            wallet_in_db["transactions"] = txs
            wallet_in_db["at_block_number"] = item.get("block_number")

            self.data_base.update_wallet(wallet_in_db)
