"""
Module for the configurations of system
"""
import os


class Neo4jConfig:
    bolt = "bolt://0.0.0.0:7687"
    username = "neo4j"
    password = "trava_pass"


class Config:
    HOST = '0.0.0.0'
    PORT = 8000


class MongoDBConfig:
    NAME = os.environ.get("MONGO_USERNAME") or "just_for_dev"
    PASSWORD = os.environ.get("MONGO_PASSWORD") or "password_for_dev"
    HOST = os.environ.get("MONGO_HOST") or "localhost"
    # HOST = "25.19.185.225"
    PORT = os.environ.get("MONGO_PORT") or "27027"
    DATABASE = "EXTRACT_DATA_KNOWLEDGE_GRAPH"
    TRANSACTIONS = "TRANSACTIONS"
    TRANSACTIONS_TRANSFER = "TRANSACTIONS_TRANSFER"
    WALLET = "WALLET"
    POOL = "POOL"
    BLOCKS = "BLOCKS"
    TOKENS = "TOKENS"


class Neo4jConfig:
    BOLT = "bolt://0.0.0.0:7687"
    HOST = os.environ.get("NEO4J_HOST") or "0.0.0.0"
    BOTH_PORT = os.environ.get("NEO4J_PORT") or 7687
    HTTP_PORT = os.environ.get("NEO4J_PORT") or 7474
    HTTPS_PORT = os.environ.get("NEO4J_PORT") or 7473
    NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME") or "neo4j"
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD") or "klg_pass"
