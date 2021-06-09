import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, './'))

from knowledge_graph_etl.exporter.database.database import Database

# username = os.environ['MONGO_USERNAME']
# password = os.environ['MONGO_PASSWORD']
# host = os.environ['MONGO_HOST']
# port = os.environ['MONGO_PORT']
#
# print(username)
# print(password)
#
# url = f"mongodb://{username}:{password}@{host}:{port}"
# mongo = MongoClient(url)
#
# print(mongo.server_info())
# mongo_db = mongo[MongoDBConfig.DATABASE]
# mongo_transactions = mongo_db[MongoDBConfig.TRANSACTIONS]
# tx = mongo_transactions.find_one()
# print(tx)
database = Database()
token = database.get_token("0xe9e7cea3dedca5984780bafc599bd69add087d56")
print(token)
