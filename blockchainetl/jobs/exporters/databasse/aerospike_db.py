import hashlib
import logging

import aerospike

from config.config import AerospikeDBConfig

logger = logging.getLogger("AerospikeDB")


class Database(object):
    """Manages connection to  database and makes async queries
    """

    def __init__(self):

        config = {
            'hosts': [(AerospikeDBConfig.HOST, int(AerospikeDBConfig.PORT))]
        }
        self.namespace = AerospikeDBConfig.NAMESPACE
        try:
            self.client = aerospike.client(config).connect()
        except Exception as e:
            import sys
            print(e)
            print("failed to connect to the cluster with", config['hosts'])
            sys.exit(1)

    def close(self):
        self.client.close()

    def _create_index(self):
        pass

    def update_block(self, block):
        key = block.get("number")
        key = str(key)
        address = (self.namespace, AerospikeDBConfig.BLOCK_SET, key)
        try:
            self.client.put(address, block)
        except Exception as e:
            logger.error(e)

    def update_transaction(self, tx):
        key = tx.get("hash")
        key = self._standard_key(key)
        address = (self.namespace, AerospikeDBConfig.TRANSACTION_SET, key)
        try:
            self.client.put(address, tx)
        except Exception as e:
            logger.error(e)

    def update_transaction_transfer(self, tx):
        key = tx.get("hash")
        key = self._standard_key(key)
        address = (self.namespace, AerospikeDBConfig.TRANSACTION_TRANSFER_SET, key)
        try:
            self.client.put(address, tx)
        except Exception as e:
            logger.error(e)

    def update_wallet(self, wallet):
        key = wallet.get("address")
        key = self._standard_key(key)
        address = (self.namespace, AerospikeDBConfig.WALLET_SET, key)
        try:
            self.client.put(address, wallet)
        except Exception as e:
            logger.error(e)

    def replace_wallet(self, wallet):
        key = wallet.get("address")
        key = self._standard_key(key)
        address = (self.namespace, AerospikeDBConfig.WALLET_SET, key)
        try:
            self.client.put(address, wallet)
        except Exception as e:
            logger.error(e)

    def get_wallet(self, wallet_address):
        key = wallet_address
        key = self._standard_key(key)
        try:
            (key, metadata, record) = self.client.get(key)
            return record
        except Exception as e:
            logger.error(e)
            record = {
                "address": wallet_address
            }
            return record

    def insert_to_token_collection(self, token_address, event):
        key = event.get("transaction_hash") + "_" + str(event.get("log_index"))
        key = self._standard_key(key)
        token_address_key = self._standard_key(token_address)
        address = (self.namespace, token_address_key, key)
        try:
            self.client.put(address, event)

        except Exception as e:
            logger.error(e)

    def update_token(self, token):
        key = token.get("address")
        key = self._standard_key(key)
        address = (self.namespace, AerospikeDBConfig.TOKEN_SET, key)
        try:
            self.client.put(address, token)

        except Exception as e:
            logger.error(e)

    def _standard_key(self, key):
        key = str(key)
        return hashlib.sha512(key.encode('utf-8')).hexdigest()[:13]
        # return key
