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
        # logger.info(f"Time to insert_to_token_collection item in event {time.time() - start_time}")

    def _event_handler(self, item):
        item[TokenConstant.value] = str(item.get(TokenConstant.value))
        contract_address = item.get(TokenConstant.contract_address)
        item[TokenConstant.type] = item.pop(TokenConstant.event_type)
        start_time = time.time()
        self._update_wallet_and_item(item, contract_address)
        # logger.info(f"Time to update wallet item in event {time.time() - start_time}")
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
            # logger.info(f"Time to get wallet in db{time.time() - start_time}")
            balances = wallet_in_db.get(WalletConstant.balances)
            supply = wallet_in_db.get(WalletConstant.supply)
            borrow = wallet_in_db.get(WalletConstant.borrow)
            if not balances:
                balances = {}
            if not supply:
                supply = {}
            if not borrow:
                borrow = {}
            unit_token = wallet.get(WalletConstant.unit_token)
            if not unit_token:
                unit_token = balance_address

            wallet[WalletConstant.balance] = str(wallet.get(WalletConstant.balance))
            wallet[WalletConstant.pre_balance] = str(wallet.get(WalletConstant.pre_balance))

            balances[unit_token] = wallet.get(WalletConstant.balance)
            if wallet.get(WalletConstant.supply):
                supply[unit_token] = wallet.get(WalletConstant.supply)
            if wallet.get(WalletConstant.borrow):
                borrow[unit_token] = wallet.get(WalletConstant.borrow)

            wallet_in_db[WalletConstant.balances] = balances
            wallet_in_db[WalletConstant.supply] = supply
            wallet_in_db[WalletConstant.borrow] = borrow

            wallet[WalletConstant.balances] = balances
            wallet[WalletConstant.supply] = supply
            wallet[WalletConstant.borrow] = borrow

            wallet_in_db[WalletConstant.at_block_number] = item.get(TransactionConstant.block_number)
            start_time = time.time()
            self.data_base.replace_wallet(wallet_in_db)
            # logger.debug(f"time to replace_wallet wallet in db{time.time() - start_time}")

        # logger.debug(f"Time to _update_wallet_and_item {time.time() - start_time_all}")
