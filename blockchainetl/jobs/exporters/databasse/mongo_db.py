import logging
import sys
import time

from pymongo import MongoClient

from config.config import MongoDBConfig

logger = logging.getLogger("Database")


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

    def replace_wallet(self, wallet):
        stat_time = time.time()
        key = {'address': wallet['address']}
        data = {"$set": wallet}

        self.mongo_wallet.replace_one(key, wallet, upsert=False)
        logger.info(f"Wallet size {sys.getsizeof(wallet)}")
        logger.info(f"time to update wallet {time.time() - stat_time}")

    def get_wallet(self, address):
        start_time = time.time()
        key = {"address": address}
        wallet = self.mongo_wallet.find_one(key)
        if not wallet:
            wallet = {
                "address": address,
            }
            self.update_wallet(wallet)
        logger.info(f"Time to get wallet {time.time() - start_time}")
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
