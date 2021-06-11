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
import datetime
import logging

from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.mappers.receipt_log_mapper import EthReceiptLogMapper
from ethereumetl.mappers.token_transfer_mapper import EthTokenTransferMapper
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor

logger = logging.getLogger('export_lending_graph_job')


class ExtractLendingKnowledgeGraphJob(BaseJob):
    def __init__(
            self,
            token_list,
            batch_size,
            max_workers,
            item_exporter,
            at_block,
            vtokens,
            credit_score_service,
            token_service,
            database,
            latest_block=None
    ):
        self.token_list = token_list

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.receipt_log_mapper = EthReceiptLogMapper()
        self.token_transfer_mapper = EthTokenTransferMapper()
        self.token_transfer_extractor = EthTokenTransferExtractor()

        self.database = database
        self.vtokens = vtokens
        self.at_block = at_block
        self.event_handler_map = {
            "Mint": self._mint_handler,
            "Borrow": self._borrow_handler,
            "RepayBorrow": self._repay_handler,
            "Redeem": self._redeem_handler,
            "LiquidateBorrow": self._liquidate_handler,
            "Transfer": self._transfer_handler
        }
        self.credit_score_service = credit_score_service
        self.token_service = token_service

        self.latest_block = latest_block
        if latest_block:
            self.block_thread_hole = int(latest_block * 0.9)
        else:
            self.block_thread_hole = 0

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.token_list, self._extract_landing_events)

    def _extract_landing_events(self, token_list):
        for token_address in token_list:
            self._extract_landing_event(token_address)

    def _extract_landing_event(self, contract_address):
        now = datetime.datetime.now()
        contract_address = contract_address.lower()
        token = self.database.get_token(contract_address)
        self.token = token
        ### update tokens

        ### add config just update token at 3 a.m

        # if contract_collection in vtokens:
        if (now.hour == 3 and now.minute < 5) and (contract_address in self.vtokens):

            token_info = self.token_service.get_token(contract_address, self.token_service.VTOKEN)
            if token:
                token["supply"] = str(token_info.get("total_supply"))
                token["borrow"] = str(token_info.get("total_borrow"))

                self.database.update_token(token)

                lending_pool = self.database.generate_lending_pool_dict_for_klg(token.get("address"),
                                                                                token.get("name"),
                                                                                token.get("borrow"),
                                                                                token.get("supply"), self.at_block)
                self.database.neo4j_update_lending_token(lending_pool, token.get("symbol"))

        ### update wallet and transaction
        events = self.database.get_event_at_block_num(contract_address, self.at_block)
        for event in events:
            handler = self.event_handler_map[event.get("type")]
            if handler:
                handler(contract_address, event, self.at_block)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()

    def _mint_handler(self, contract_address, event, at_block):
        tx_id = event.get("transaction_hash")
        event_id = event.get("_id")
        account_address = event.get("minter")
        accumulate_amount = event.get("mintAmount")
        typ = "Mint"
        if accumulate_amount:
            wallet = self._update_wallet_accumulate(account_address, typ, accumulate_amount, contract_address, at_block,
                                                    tx_id,
                                                    event_id)
            self._update_wallet_neo4j(wallet)

        self._update_tx_event_neo4j(account_address, contract_address, tx_id, accumulate_amount, typ)

    def _borrow_handler(self, contract_address, event, at_block):
        tx_id = event.get("transaction_hash")
        event_id = event.get("_id")
        account_address = event.get("borrower")
        accumulate_amount = event.get("borrowAmount")
        typ = "Borrow"
        if accumulate_amount:
            wallet = self._update_wallet_accumulate(account_address, typ, accumulate_amount, contract_address, at_block,
                                                    tx_id,
                                                    event_id)
            self._update_wallet_neo4j(wallet)
        self._update_tx_event_neo4j(account_address, contract_address, tx_id, accumulate_amount, typ)

    def _repay_handler(self, contract_address, event, at_block):
        tx_id = event.get("transaction_hash")
        event_id = event.get("_id")
        account_address = event.get("borrower")
        accumulate_amount = event.get("repayAmount")
        typ = "RepayBorrow"
        if accumulate_amount:
            wallet = self._update_wallet_accumulate(account_address, typ, accumulate_amount, contract_address, at_block,
                                                    tx_id,
                                                    event_id)
            self._update_wallet_neo4j(wallet)
        self._update_tx_event_neo4j(account_address, contract_address, tx_id, account_address, typ)

    def _redeem_handler(self, contract_address, event, at_block):
        tx_id = event.get("transaction_hash")
        event_id = event.get("_id")
        account_address = event.get("redeemer")
        accumulate_amount = event.get("redeemAmount")
        typ = "Redeem"
        if accumulate_amount:
            wallet = self._update_wallet_accumulate(account_address, typ, accumulate_amount, contract_address, at_block,
                                                    tx_id,
                                                    event_id)
            self._update_wallet_neo4j(wallet)
        self._update_tx_event_neo4j(account_address, contract_address, tx_id, accumulate_amount, typ)

    def _liquidate_handler(self, contract_address, event, at_block):
        tx_id = event.get("transaction_hash")
        event_id = event.get("_id")
        account_address = event.get("liquidator")
        accumulate_amount = event.get("repayAmount")
        typ = "LiquidateBorrow-liquidator"
        if accumulate_amount:
            wallet = self._update_wallet_accumulate(account_address, typ, accumulate_amount, contract_address, at_block,
                                                    tx_id,
                                                    event_id)

            self._update_wallet_neo4j(wallet)
            account_address = event.get("borrower")
            accumulate_amount = 1
            typ = "LiquidateBorrow-borrower"

            if accumulate_amount:
                wallet = self._update_wallet_accumulate(account_address, typ, accumulate_amount, contract_address,
                                                        at_block,
                                                        tx_id)
                self._update_wallet_neo4j(wallet)
        return

    def _transfer_handler(self, contract_address, event, at_block):

        from_address = event.get("from_address")
        to_address = event.get("to_address")
        typ = "Transfer"
        tx_id = event.get("transaction_hash")
        event_id = event.get("_id")
        token = self.database.get_token(contract_address)
        amount = event.get("value")
        if token:
            symbol = token.get("symbol")
        else:
            symbol = "???"
        ###update wallet from and to address

        # from_wallet = self.database.get_wallet(from_address)
        # self._update_lending_info(from_wallet, contract_address, at_block)
        # self.database.update_wallet(from_wallet)
        # to_wallet = self.database.get_wallet(to_address)
        # self._update_lending_info(to_wallet, contract_address, at_block)
        # self.database.update_wallet(to_wallet)

        ### add accumulate with transfer
        if amount:
            ### type accumulate is TransferFrom

            wallet = self._update_wallet_accumulate(from_address, typ + "From", amount, contract_address, at_block,
                                                    tx_id,
                                                    event_id)
            if wallet:
                self._update_wallet_neo4j(wallet)
            wallet = self._update_wallet_accumulate(to_address, typ + "To", amount, contract_address, at_block,
                                                    tx_id,
                                                    event_id)
            if wallet:
                self._update_wallet_neo4j(wallet)

        link_dict = self.database.generate_link_dict_for_klg(from_address, to_address, tx_id, amount, symbol,
                                                             typ)
        self.database.neo4j_update_link(link_dict)

    def _update_wallet_accumulate(self, wallet_address, typ, accumulate_amount, contract_address, at_block, tx_id,
                                  event_id=None):
        # logger.info("update accumulate for" + wallet_address + "with type:" + typ)
        if (typ == "TransferFrom" or typ == "TransferTo") and self.block_thread_hole and int(
                at_block) < self.block_thread_hole:
            return
        wallet = self.database.get_wallet(wallet_address)
        accumulate_history = wallet.get("accumulate_history")
        if not accumulate_history:
            accumulate_history = {}
            accumulate = {}
            wallet["accumulate_history"] = accumulate_history
            wallet["accumulate"] = accumulate
        event_accumulate_history = accumulate_history.get(typ)

        if not event_accumulate_history:
            event_accumulate_history = {}
            accumulate_history[typ] = event_accumulate_history
            wallet["accumulate"][typ] = {}
        # mint_amount = event.get("mintAmount")
        self._handler_accumulate(contract_address, event_accumulate_history, accumulate_amount, at_block, tx_id,
                                 event_id)

        wallet["accumulate"][typ][contract_address] = event_accumulate_history[contract_address][-1]

        # add lending info to wallet
        self._update_lending_info(wallet, contract_address, at_block)
        now = datetime.datetime.now()
        if (typ != "TransferFrom" or typ != "TransferTo") and (now.hour == 3 and now.minute < 5):
            credit_score = self.credit_score_service.get_credit_score(wallet_address)
            # print("wallet at " + wallet_address + " update credit score :" + str(credit_score))
            wallet["credit_score"] = credit_score

        self.database.update_wallet(wallet)
        return wallet

    def _handler_accumulate(self, contract_address, accumulate_history, amount, at_block, tx_id, event_id):
        amount_key_accumulate = "accumulate_amount"
        accumulate_current = {
            amount_key_accumulate: str(amount),
            "amount": str(amount),
            "block_number": at_block,
            "transaction_hash": tx_id,
            "event_id": event_id

        }
        amount = int(amount)
        if not accumulate_history.get(contract_address):
            ### if not exit create new one history and current accumulate
            accumulate_history[contract_address] = [accumulate_current]

        else:
            ### else find value accumulate at nearest less block and update accumulate history
            accumulate_at_contract = accumulate_history.get(contract_address)
            accumulate_at_contract_len = len(accumulate_at_contract)
            i = accumulate_at_contract_len - 1
            while i >= 0:
                accumulate_at_block = accumulate_at_contract[i]
                if at_block >= accumulate_at_block.get("block_number"):
                    k = i

                    while k >= 0 and accumulate_at_contract[k].get("block_number") == at_block:
                        if accumulate_at_contract[k].get("event_id") == event_id:
                            return
                        k = k - 1

                    point_update_amount = int(accumulate_at_block.get(amount_key_accumulate))
                    accumulate_current[amount_key_accumulate] = str(amount + point_update_amount)

                    accumulate_at_contract.insert(i + 1, accumulate_current)
                    if i < accumulate_at_contract_len - 1:
                        for j in (i + 2, accumulate_at_contract_len):
                            accumulate_at_contract[j][amount_key_accumulate] = str(
                                int(accumulate_at_contract[j].get(amount_key_accumulate)) + amount
                            )

                    break
                i = i - 1
            if i < 0:
                accumulate_history[contract_address] = [accumulate_current]

    def _update_lending_info(self, wallet, contract_address, at_block):
        ### get balance, supply, borrow
        account_info = self.token_service.get_account_info(wallet.get("address"), contract_address,
                                                           self.token_service.VTOKEN, block_identifier=at_block)
        lending_infos = wallet.get("lending_infos")
        if not lending_infos:
            lending_infos = {contract_address: [account_info]}
        lending_infos_token = lending_infos.get(contract_address)
        if not lending_infos_token:
            lending_infos[contract_address] = [account_info]
        else:
            i = len(lending_infos_token) - 1
            while i >= 0:
                if lending_infos_token[i].get("block_number") < account_info.get("block_number"):
                    lending_infos_token.insert(i + 1, account_info)
                    break
                elif lending_infos_token[i].get("block_number") == account_info.get("block_number"):
                    break
                i = i - 1
            if i < 0:
                lending_infos_token.insert(0, account_info)
            wallet["lending_infos"] = lending_infos

        if not wallet.get("lending_info"):
            wallet["lending_info"] = {}
            wallet["lending_info"][contract_address] = lending_infos[contract_address][-1]
        else:
            wallet["lending_info"][contract_address] = lending_infos[contract_address][-1]

    def _update_wallet_neo4j(self, wallet):
        account_address = wallet.get("address")
        lending_info = wallet.get("lending_info")
        token = self.token
        balance = None
        supply = None
        borrow = None
        block_number = wallet.get("at_block_number")
        credit_score = wallet.get("credit_score")
        balances = wallet.get("balances")
        if balances:
            balance = balances.get(self.token.get("address"))

        if token:
            symbol = token.get("symbol")
        else:
            symbol = "???"
        if lending_info:
            lending_info_at_pool = lending_info.get(self.token.get("address"))
            if lending_info_at_pool:
                balance = lending_info_at_pool.get("balance")
                supply = lending_info_at_pool.get("supply")
                borrow = lending_info_at_pool.get("borrow")
                block_number = lending_info_at_pool.get("block_number")
        wallet_dict = self.database.generate_wallet_dict_for_klg(address=account_address, balance=balance,
                                                                 supply=supply, borrow=borrow,
                                                                 credit_score=credit_score, block_number=block_number)
        self.database.neo4j_update_wallet_token(wallet_dict, symbol)

    def _update_tx_event_neo4j(self, account_address, contract_address, tx_id, accumulate_amount, typ):
        from_address = account_address
        to_address = contract_address
        if typ == "Borrow" or typ == "Redeem":
            from_address = contract_address
            to_address = account_address

        token = self.token
        if token:
            symbol = token.get("symbol")
        else:
            symbol = "???"
        link_dict = self.database.generate_link_dict_for_klg(from_address, to_address, tx_id, accumulate_amount, symbol,
                                                             typ)
        self.database.neo4j_update_link(link_dict)
