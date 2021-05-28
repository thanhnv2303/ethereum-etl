import datetime
import logging
import os
from time import time

from web3 import Web3
from web3.middleware import geth_poa_middleware

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator
from knowledge_graph_etl.exporter.database.database import Database
from knowledge_graph_etl.services.credit_score_service import CreditScoreService
from knowledge_graph_etl.services.eth_token_type_service import EthTokenTypeService, clean_user_provided_content

logger = logging.getLogger('export_lending_graph')


class KLGStreamerAdapter:
    def __init__(
            self,
            batch_web3_provider,
            item_exporter=ConsoleItemExporter(),
            database=Database(),
            batch_size=100,
            max_workers=5,
            entity_types=tuple(EntityType.ALL_FOR_STREAMING),
            tokens_filter_file="../../artifacts/token_filter",
            v_tokens_filter_file="../../artifacts/vToken_filter",
    ):
        # self.batch_web3_provider = batch_web3_provider
        self.batch_web3_provider = batch_web3_provider
        self.w3 = Web3(batch_web3_provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.database = database
        cur_path = os.path.dirname(os.path.realpath(__file__))
        self.tokens_filter_file = cur_path + "/" + tokens_filter_file
        self.v_tokens_filter_file = cur_path + "/" + v_tokens_filter_file
        self.event_handler_map = {
            "Mint": self._mint_handler,
            "Borrow": self._borrow_handler,
            "RepayBorrow": self._repay_handler,
            "Redeem": self._redeem_handler,
            "LiquidateBorrow": self._liquidate_handler,
            "Transfer": self._transfer_handler
        }
        self.credit_score_service = CreditScoreService(database)
        self.token_service = EthTokenTypeService(self.w3, clean_user_provided_content)

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        latest_block = self.database.mongo_blocks.find_one(sort=[("number", -1)])  # for MAX
        return latest_block.get("number") - 16

    def export_all(self, start_block, end_block):
        start_time = time()
        now = datetime.datetime.now()
        # tokens = VENUS_TOKEN
        # vtokens = VTOKEN
        tokens = []
        vtokens = []
        with open(self.tokens_filter_file, "r") as file:
            tokens = file.read().splitlines()
        with open(self.v_tokens_filter_file, "r") as file:
            v_tokens = file.read().splitlines()
            for token in v_tokens:
                vtokens.append(token.lower())

        ## update token market info at 3h - 3h5m
        if now.hour == 3 and now.minute < 5:
            self.credit_score_service.update_token_market_info()

        for block in range(start_block, end_block + 1):
            for token in tokens:
                contract_collection = token.lower()
                token = self.database.get_token(contract_collection)
                self.token = token
                ### update tokens

                ### add config just update token at 3 a.m

                # if contract_collection in vtokens:
                if (now.hour == 3) and (contract_collection in vtokens):

                    token_info = self.token_service.get_token(contract_collection, self.token_service.VTOKEN)
                    if token:
                        token["supply"] = str(token_info.get("total_supply"))
                        token["borrow"] = str(token_info.get("total_borrow"))

                        self.database.update_token(token)

                        lending_pool = self.database.generate_lending_pool_dict_for_klg(token.get("address"),
                                                                                        token.get("name"),
                                                                                        token.get("borrow"),
                                                                                        token.get("supply"), block)
                        self.database.neo4j_update_lending_token(lending_pool, token.get("symbol"))
                ### update wallet and transaction
                events = self.database.get_event_at_block_num(contract_collection, block)
                for event in events:
                    handler = self.event_handler_map[event.get("type")]
                    if handler:
                        handler(contract_collection, event, block)
        # print(tokens)
        # self.item_exporter.export_items(tokens)

        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=(end_block - start_block + 1),
            time_diff=time_diff,
        ))

    def close(self):
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
        token = self.database.get_token(contract_address)
        amount = event.get("value")
        if token:
            symbol = token.get("symbol")
        else:
            symbol = "???"
        ###update wallet from and to address

        # from_wallet = self.database.get_wallet(from_address)
        # self._update_wallet_neo4j(from_wallet)
        # to_wallet = self.database.get_wallet(to_address)
        # self._update_wallet_neo4j(to_wallet)

        link_dict = self.database.generate_link_dict_for_klg(from_address, to_address, tx_id, amount, symbol,
                                                             typ)
        self.database.neo4j_update_link(link_dict)

    def _update_wallet_accumulate(self, wallet_address, typ, accumulate_amount, contract_address, at_block, tx_id,
                                  event_id=None):
        print("update accumulate for" + wallet_address + "with type:" + typ)
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

        ### get balance, supply, borrow
        account_info = self.token_service.get_account_info(wallet.get("address"), contract_address,
                                                           self.token_service.VTOKEN)
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


        credit_score = self.credit_score_service.get_credit_score(wallet_address)
        print("wallet at " + wallet_address + "update credit score :"+ str(credit_score))
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


def get_nearest_less_key(dict_, search_key):
    try:
        result = max(key for key in map(int, dict_.keys()) if key <= search_key)
        return result
    except:
        return


def get_nearest_key(dict_, search_key):
    try:
        result = min(dict_.keys(), key=lambda key: abs(key - search_key))
        return result
    except:
        return
