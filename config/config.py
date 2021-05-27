"""
Module for the configurations of system
"""


class Neo4jConfig:
    bolt = "bolt://0.0.0.0:7687"
    username = "neo4j"
    password = "trava_pass"


class Config:
    HOST = '0.0.0.0'
    PORT = 8000


class MongoDBConfig:
    NAME = "just_for_dev"
    PASSWORD = "password_for_dev"
    HOST = "localhost"
    PORT = "27027"
    DATABASE = "EXTRACT_DATA_KNOWLEDGE_GRAPH"
    TRANSACTIONS = "TRANSACTIONS"
    TRANSACTIONS_TRANSFER = "TRANSACTIONS_TRANSFER"
    WALLET = "WALLET"
    POOL = "POOL"
    BLOCKS = "BLOCKS"
    TOKENS = "TOKENS"


class Neo4jConfig:
    bolt = "bolt://0.0.0.0:7687"
    username = "neo4j"
    password = "klg_pass"
