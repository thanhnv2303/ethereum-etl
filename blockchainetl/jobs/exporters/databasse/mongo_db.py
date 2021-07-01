import logging
import time

from pymongo import MongoClient

from config.config import MongoDBConfig
from config.constant import MongoIndexConstant, TestPerformanceConstant
from data_storage.memory_storage_test_performance import MemoryStoragePerformance

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

        self.local_storage = MemoryStoragePerformance.getInstance()

        self._create_index()

    def _create_index(self):

        if MongoIndexConstant.tx_id not in self.mongo_transactions.index_information():
            self.mongo_transactions.create_index([("hash", "hashed")], name=MongoIndexConstant.tx_id)
        if MongoIndexConstant.transfer_tx_id not in self.mongo_transactions_transfer.index_information():
            self.mongo_transactions_transfer.create_index([("hash", "hashed")], name=MongoIndexConstant.transfer_tx_id)
        if MongoIndexConstant.transfer_block_number not in self.mongo_transactions_transfer.index_information():
            self.mongo_transactions_transfer.create_index([("block_number", "hashed")],
                                                          name=MongoIndexConstant.transfer_block_number)
        if MongoIndexConstant.wallet_address not in self.mongo_wallet.index_information():
            self.mongo_wallet.create_index([("address", "hashed")], name=MongoIndexConstant.wallet_address)
        # self.mongo_pool.create_index([("address", "hashed")])

    def update_block(self, block):
        c_time = self.local_storage.get(TestPerformanceConstant.write_mongo_time)
        start = time.time()
        self.mongo_blocks.insert_one(block)
        self.local_storage.set(TestPerformanceConstant.write_mongo_time, c_time + time.time() - start)

    def update_transaction(self, tx):
        c_time = self.local_storage.get(TestPerformanceConstant.write_mongo_time)
        start = time.time()
        self.mongo_transactions.insert_one(tx)
        self.local_storage.set(TestPerformanceConstant.write_mongo_time, c_time + time.time() - start)

    def update_transaction_transfer(self, tx):
        c_time = self.local_storage.get(TestPerformanceConstant.write_mongo_time)
        start = time.time()
        self.mongo_transactions_transfer.insert_one(tx)
        self.local_storage.set(TestPerformanceConstant.write_mongo_time, c_time + time.time() - start)

    def update_wallet(self, wallet):
        key = {'address': wallet['address']}
        data = {"$set": wallet}
        c_time = self.local_storage.get(TestPerformanceConstant.write_mongo_time)
        start = time.time()
        self.mongo_wallet.update_one(key, data, upsert=True)
        self.local_storage.set(TestPerformanceConstant.write_mongo_time, c_time + time.time() - start)

    def replace_wallet(self, wallet):
        # stat_time = time.time()
        c_time = self.local_storage.get(TestPerformanceConstant.write_mongo_time)
        start = time.time()
        key = {'address': wallet['address']}
        data = {"$set": wallet}

        self.mongo_wallet.replace_one(key, wallet, upsert=False)

        self.local_storage.set(TestPerformanceConstant.write_mongo_time, c_time + time.time() - start)
        # logger.debug(f"Wallet size {sys.getsizeof(wallet)}")
        # logger.debug(f"time to update wallet {time.time() - stat_time}")

    def get_wallet(self, address):
        c_time = self.local_storage.get(TestPerformanceConstant.read_mongo_time)
        start = time.time()
        key = {"address": address}
        wallet = self.mongo_wallet.find_one(key)
        if not wallet:
            wallet = {
                "address": address,
            }
            self.update_wallet(wallet)
        # logger.debug(f"Time to get wallet {time.time() - start_time}")
        self.local_storage.set(TestPerformanceConstant.read_mongo_time, c_time + time.time() - start)
        return wallet

    def insert_to_token_collection(self, token_address, event):
        c_time = self.local_storage.get(TestPerformanceConstant.write_mongo_time)
        start = time.time()
        if not self.mongo_token_collection_dict.get(token_address):
            self.mongo_token_collection_dict[token_address] = self.mongo_db[token_address]
            self.mongo_token_collection_dict[token_address].create_index([("transaction_hash", "hashed")])
            self.mongo_token_collection_dict[token_address].create_index([("block_number", "hashed")])

        self.mongo_token_collection_dict[token_address].insert_one(event)
        self.local_storage.set(TestPerformanceConstant.write_mongo_time, c_time + time.time() - start)

    def update_token(self, token):
        c_time = self.local_storage.get(TestPerformanceConstant.write_mongo_time)
        start = time.time()
        key = {'address': token['address']}
        data = {"$set": token}

        res = self.mongo_tokens.update_one(key, data, upsert=True)
        self.local_storage.set(TestPerformanceConstant.write_mongo_time, c_time + time.time() - start)

    def get_all_wallet(self):
        c_time = self.local_storage.get(TestPerformanceConstant.read_mongo_time)
        start = time.time()
        result = self.mongo_wallet.find({})
        self.local_storage.set(TestPerformanceConstant.read_mongo_time, c_time + time.time() - start)
        return result
