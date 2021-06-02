from py2neo import Graph
from pymongo import MongoClient

from config.config import MongoDBConfig, Neo4jConfig


class Database(object):
    """Manages connection to  database and makes async queries
    """

    def __init__(self):
        self._conn = None
        url = f"mongodb://{MongoDBConfig.NAME}:{MongoDBConfig.PASSWORD}@{MongoDBConfig.HOST}:{MongoDBConfig.PORT}"
        self.mongo = MongoClient(url)
        self.mongo_db = self.mongo[MongoDBConfig.DATABASE]
        self.mongo_transactions = self.mongo_db[MongoDBConfig.TRANSACTIONS]
        self.mongo_transactions_transfer = self.mongo_db[MongoDBConfig.TRANSACTIONS_TRANSFER]
        self.mongo_wallet = self.mongo_db[MongoDBConfig.WALLET]
        self.mongo_tokens = self.mongo_db[MongoDBConfig.TOKENS]
        self.mongo_blocks = self.mongo_db[MongoDBConfig.BLOCKS]
        self.mongo_token_collection_dict = {}

        self._graph = Graph(Neo4jConfig.bolt, auth=(Neo4jConfig.username, Neo4jConfig.password))

        # self._create_index()

    def _create_index(self):
        self.mongo_transactions.create_index([("hash", "hashed")])
        self.mongo_transactions_transfer.create_index([("hash", "hashed")])
        self.mongo_transactions_transfer.create_index([("block_num", -1)])
        self.mongo_wallet.create_index([("address", "hashed")])
        # self.mongo_pool.create_index([("address", "hashed")])

    def update_block(self, block):
        self.mongo_blocks.insert_one(block)

    def update_transaction(self, tx):
        self.mongo_transactions.insert_one(tx)

    def update_transaction_transfer(self, tx):
        self.mongo_transactions_transfer.insert_one(tx)

    def update_wallet(self, wallet):
        key = {'address': wallet['address']}
        data = {"$set": wallet}

        self.mongo_wallet.update_one(key, data, upsert=True)

    def get_wallet(self, address):
        key = {"address": address}
        wallet = self.mongo_wallet.find_one(key)
        if not wallet:
            wallet = {
                "address": address,
            }
            self.update_wallet(wallet)
        return wallet

    def insert_to_token_collection(self, token_address, event):
        if not self.mongo_token_collection_dict.get(token_address):
            self.mongo_token_collection_dict[token_address] = self.mongo_db[token_address]
            self.mongo_token_collection_dict[token_address].create_index([("transaction_hash", "hashed")])
            self.mongo_token_collection_dict[token_address].create_index([("block_num", "hashed")])

        self.mongo_token_collection_dict[token_address].insert_one(event)

    def update_token(self, token):
        key = {'address': token['address']}
        data = {"$set": token}

        res = self.mongo_tokens.update_one(key, data, upsert=True)

    def get_token(self, address):
        key = {'address': address}

        return self.mongo_tokens.find_one(key)

    def get_event_at_block_num(self, contract_address, block_num):
        key = {'block_number': block_num}

        return self.mongo_db[contract_address].find(key)

    def neo4j_update_wallet_token(self, wallet, token):
        match = self._graph.run("match (p:WALLET {address:$address}) return p ", address=wallet.get("address")).data()
        if not match:
            create = self._graph.run("merge (p:WALLET { address:$address }) "
                                     "set p.balance" + token + "=$balance,"
                                                               "p.credit_score=$credit_score,"
                                                               "p.block_number = $block_number,"
                                                               "p.borrow" + token + "=$borrow,"
                                                                                    "p.supply" + token + "=$supply "
                                                                                                         "return p",
                                     address=wallet.get("address"),
                                     balance=wallet.get("balance"),
                                     credit_score=wallet.get("credit_score"),
                                     borrow=wallet.get("borrow"),
                                     block_number=wallet.get("block_number"),
                                     supply=wallet.get("supply")).data()
        else:
            create = self._graph.run("match (p:WALLET { address:$address }) "
                                     "set "
                                     "p.balance" + token + "=$balance,"
                                                           "p.credit_score=$credit_score,"
                                                           "p.block_number = $block_number,"
                                                           "p.borrow" + token + "=$borrow,"
                                                                                "p.supply" + token + "=$supply "
                                                                                                     "return p",
                                     address=wallet.get("address"),
                                     balance=wallet.get("balance" + token),
                                     credit_score=wallet.get("credit_score"),
                                     borrow=wallet.get("borrow" + token),
                                     block_number=wallet.get("block_number"),
                                     supply=wallet.get("supply" + token)).data()
        return create[0]["p"]

    def neo4j_update_lending_token(self, lending_pool, token):

        match = self._graph.run("match (p:LENDING {address:$address}) return p ",
                                address=lending_pool.get("address")).data()
        if not match:
            create = self._graph.run("merge (p:LENDING { address:$address }) "
                                     "set p.block_number = $block_number,"
                                     "p.name =\'" + lending_pool.get("name") + "\',"
                                                                               "p.borrow" + token + "=$borrow,"
                                                                                                    "p.supply" + token + "=$supply "
                                                                                                                         "return p",
                                     address=lending_pool.get("address"),
                                     borrow=lending_pool.get("borrow"),
                                     block_number=lending_pool.get("block_number"),
                                     supply=lending_pool.get("supply")).data()
        else:
            create = self._graph.run("match (p:LENDING { address:$address }) "
                                     "set  p.block_number = $block_number,p.borrow" + token + "=$borrow,"
                                                                                              "p.supply" + token + "=$supply "
                                                                                                                   "return p",
                                     address=lending_pool.get("address"),
                                     borrow=lending_pool.get("borrow"),
                                     block_number=lending_pool.get("block_number"),
                                     supply=lending_pool.get("supply")).data()
        return create[0]["p"]

    def neo4j_update_token(self, token):

        create = self._graph.run("match (p { address:$address }) "
                                 "set p.price=$price,"
                                 "p.credit_score=$credit_score,"
                                 "p.market_rank=$market_rank,"
                                 "p.market_cap=$market_cap "
                                 "return p",
                                 address=token.get("address"),
                                 price=token.get("price"),
                                 credit_score=token.get("credit_score"),
                                 market_rank=token.get("market_rank"),
                                 market_cap=token.get("market_cap")
                                 ).data()
        return create

    def neo4j_update_link(self, tx):

        merge = self._graph.run("match (p {address: $from_address }),(e {address:$to_address})"
                                " MERGE (p)-[r:"
                                + tx.get("label") +
                                " { tx_id:$tx_id, amount:$amount, token:$token }]->(e) return p,r,e",
                                from_address=tx.get("from_address"),
                                to_address=tx.get("to_address"),
                                tx_id=tx.get("tx_id"),
                                amount=tx.get("amount"),
                                token=tx.get("token")).data()
        return merge

    def generate_lending_pool_dict_for_klg(self, address, name, borrow, supply, block_number):
        return {
            "address": address,
            "name": name,
            "borrow": borrow,
            "supply": supply,
            "block_number": block_number
        }

    def generate_wallet_dict_for_klg(self, address, balance, borrow=None, supply=None, credit_score=None,
                                     block_number=None):
        return {
            "address": address,
            "balance": balance,
            "supply": supply,
            "borrow": borrow,
            "credit_score": credit_score,
            "block_number": block_number
        }

    def generate_link_dict_for_klg(self, from_address, to_address, tx_id, amount, token, label):
        return {
            "from_address": from_address,
            "to_address": to_address,
            "tx_id": tx_id,
            "amount": amount,
            "token": token,
            "label": label
        }
