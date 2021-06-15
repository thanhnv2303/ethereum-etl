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
import logging
import time

from blockchainetl.jobs.exporters.databasse.mongo_db import Database
from config.constant import BlockConstant, TransactionConstant, TokenConstant, TokenTypeConstant, WalletConstant, \
    ExportItemConstant, ExportItemTypeConstant, LoggerConstant

logger = logging.getLogger(LoggerConstant.KnowledgeGraphExporter)


class KnowledgeGraphExporter:

    def __init__(self):
        self.mapping_handler = {
            ExportItemTypeConstant.transaction: self._transaction_handler,
            ExportItemTypeConstant.block: self._block_handler,
            ExportItemTypeConstant.token_transfer: self._token_transfer_handler,
            ExportItemTypeConstant.event: self._event_handler,
            ExportItemTypeConstant.token: self._token_handler
        }
        self.data_base = Database()

    def open(self):
        pass

    def export_items(self, items):
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        handler = self.mapping_handler.get(item.get(ExportItemConstant.type))
        if handler:
            handler(item)

    def close(self):
        pass

    def _block_handler(self, item):
        item[BlockConstant.gas_limit] = str(item.get(BlockConstant.gas_limit))
        item[BlockConstant.gas_used] = str(item.get(BlockConstant.gas_used))
        self.data_base.update_block(item)

    def _transaction_handler(self, item):
        item[TransactionConstant.gas] = str(item.get(TransactionConstant.gas))
        item[TransactionConstant.gas_price] = str(item.get(TransactionConstant.gas_price))
        item[TransactionConstant.value] = str(item.get(TransactionConstant.value))
        if item.get(TransactionConstant.input) == TokenConstant.native_token:
            item[TransactionConstant.transaction_hash] = item.pop(TransactionConstant.hash)
            self._update_wallet_and_item(item, TokenConstant.native_token)
            self.data_base.update_transaction_transfer(item)
        self.data_base.update_transaction(item)

    def _token_transfer_handler(self, item):
        item[TokenConstant.value] = str(item.get(TokenConstant.value))
        token_address = item.get(TokenConstant.contract_address)
        item[TokenConstant.type] = TokenTypeConstant.Transfer
        # start_time = time.time()
        self._update_wallet_and_item(item, token_address)
        # logger.debug(f"Time to update wallet item in event {time.time() - start_time}")
        start_time = time.time()
        self.data_base.insert_to_token_collection(token_address, item)
        logger.debug(f"Time to insert_to_token_collection item in event {time.time() - start_time}")

    def _event_handler(self, item):
        item[TokenConstant.value] = str(item.get(TokenConstant.value))
        contract_address = item.get(TokenConstant.contract_address)
        item[TokenConstant.type] = item.pop(TokenConstant.event_type)
        start_time = time.time()
        self._update_wallet_and_item(item, contract_address)
        logger.debug(f"Time to update wallet item in event {time.time() - start_time}")
        self.data_base.insert_to_token_collection(contract_address, item)

    def _token_handler(self, item):
        item[TokenConstant.total_supply] = str(item.get(TokenConstant.total_supply))
        self.data_base.update_token(item)

    def _update_wallet_and_item(self, item, balance_address):
        # start_time_all = time.time()
        if not item.get(TransactionConstant.wallets):
            return
        for wallet in item.get(TransactionConstant.wallets):
            address = wallet.get(WalletConstant.address)
            start_time = time.time()
            wallet_in_db = self.data_base.get_wallet(address)
            logger.debug(f"Time to get wallet in db{time.time() - start_time}")
            balances = wallet_in_db.get(WalletConstant.balances)
            if not balances:
                balances = {}
            wallet[TransactionConstant.balance] = str(wallet.get(TransactionConstant.balance))
            wallet[TransactionConstant.pre_balance] = str(wallet.get(TransactionConstant.pre_balance))
            balances[balance_address] = wallet.get(TransactionConstant.balance)
            wallet_in_db[WalletConstant.balances] = balances
            wallet[TransactionConstant.balances] = balances
            # print("in  balance_address",balance_address)

            ## add transfer native token to lending info
            # account_info = {
            #     "balance": str(wallet["balance"]),
            #     "supply": "0",
            #     "borrow": "0",
            #     "block_number": item.get("block_number")
            # }
            # contract_address = "0x"
            #
            # lending_infos = wallet_in_db.get("lending_infos")
            # if not lending_infos:
            #     lending_infos = {contract_address: [account_info]}
            # lending_infos_token = lending_infos.get(contract_address)
            # if not lending_infos_token:
            #     lending_infos[contract_address] = [account_info]
            # else:
            #     i = len(lending_infos_token) - 1
            #     while i >= 0:
            #         if lending_infos_token[i].get("block_number") < account_info.get("block_number"):
            #             lending_infos_token.insert(i + 1, account_info)
            #             break
            #         elif lending_infos_token[i].get("block_number") == account_info.get("block_number"):
            #             break
            #         i = i - 1
            #     if i < 0:
            #         lending_infos_token.insert(0, account_info)
            #     wallet_in_db["lending_infos"] = lending_infos
            #
            # if not wallet_in_db.get("lending_info"):
            #     wallet_in_db["lending_info"] = {}
            #     wallet_in_db["lending_info"][contract_address] = lending_infos[contract_address][-1]
            # else:
            #     wallet_in_db["lending_info"][contract_address] = lending_infos[contract_address][-1]

            # print("wallet_in_db ------------------------",wallet_in_db)
            # txs = wallet_in_db.get("transactions")
            # if not txs:
            #     txs = set()
            # else:
            #     txs = set(txs)
            #
            # txs.add(item.get("transaction_hash"))
            #
            # wallet_in_db["transactions"] = list(txs)
            wallet_in_db[WalletConstant.at_block_number] = item.get(TransactionConstant.block_number)
            start_time = time.time()
            self.data_base.replace_wallet(wallet_in_db)
            logger.debug(f"time to replace_wallet wallet in db{time.time() - start_time}")

        # logger.debug(f"Time to _update_wallet_and_item {time.time() - start_time_all}")
