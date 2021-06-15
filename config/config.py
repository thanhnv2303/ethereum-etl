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
    DATABASE = "extract_data_knowledge_graph"
    TRANSACTIONS = "transactions"
    TRANSACTIONS_TRANSFER = "native_transfer_transactions"
    WALLET = "wallets"
    POOL = "pool"
    BLOCKS = "blocks"
    TOKENS = "tokens"


class Neo4jConfig:
    BOLT = "bolt://0.0.0.0:7687"
    HOST = os.environ.get("NEO4J_HOST") or "0.0.0.0"
    BOTH_PORT = os.environ.get("NEO4J_PORT") or 7687
    HTTP_PORT = os.environ.get("NEO4J_PORT") or 7474
    HTTPS_PORT = os.environ.get("NEO4J_PORT") or 7473
    NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME") or "neo4j"
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD") or "klg_pass"


class BuildKnowledgeGraphConfig:
    LOG_FILE = os.environ.get("KNOWLEDGE_GRAPH_LOG_FILE")
    PROVIDER_URI = os.environ.get("KNOWLEDGE_GRAPH_PROVIDER_URI")
    LAG = os.environ.get("KNOWLEDGE_GRAPH_LAG") or 0
    BATCH_SIZE = os.environ.get("KNOWLEDGE_GRAPH_BATCH_SIZE") or 64
    MAX_WORKERS = os.environ.get("KNOWLEDGE_GRAPH_MAX_WORKERS") or 8
    START_BLOCK = os.environ.get("KNOWLEDGE_GRAPH_START_BLOCK")
    PERIOD_SECONDS = os.environ.get("KNOWLEDGE_GRAPH_PERIOD_SECONDS") or 10
    PID_FILE = os.environ.get("KNOWLEDGE_GRAPH_PID_FILE") or None
    BLOCK_BATCH_SIZE = os.environ.get("KNOWLEDGE_GRAPH_BLOCK_BATCH_SIZE") or 24
    TOKENS_FILTER_FILE = os.environ.get("KNOWLEDGE_GRAPH_TOKENS_FILTER_FILE") or "artifacts/token_filter"
    EVENT_ABI_DIR = os.environ.get("KNOWLEDGE_GRAPH_EVENT_ABI_DIR") or "artifacts/event-abi"


class KLGLendingStreamerAdapterConfig:
    LOG_FILE = os.environ.get("KNOWLEDGE_GRAPH_LENDING_LOG_FILE")
    PROVIDER_URI = os.environ.get("KNOWLEDGE_GRAPH_LENDING_PROVIDER_URI")
    LAG = os.environ.get("KNOWLEDGE_GRAPH_LENDING_LAG") or 0
    BATCH_SIZE = os.environ.get("KNOWLEDGE_GRAPH_LENDING_BATCH_SIZE") or 64
    MAX_WORKERS = os.environ.get("KNOWLEDGE_GRAPH_LENDING_MAX_WORKERS") or 8
    START_BLOCK = os.environ.get("KNOWLEDGE_GRAPH_LENDING_START_BLOCK")
    PERIOD_SECONDS = os.environ.get("KNOWLEDGE_GRAPH_LENDING_PERIOD_SECONDS") or 10
    PID_FILE = os.environ.get("KNOWLEDGE_GRAPH_LENDING_PID_FILE") or None
    BLOCK_BATCH_SIZE = os.environ.get("KNOWLEDGE_GRAPH_LENDING_BLOCK_BATCH_SIZE") or 24
    TOKENS_FILTER_FILE = os.environ.get("KNOWLEDGE_GRAPH_LENDING_TOKENS_FILTER_FILE") or "artifacts/token_filter"
    V_TOKENS_FILTER_FILE = os.environ.get("KNOWLEDGE_GRAPH_LENDING_V_TOKENS_FILTER_FILE") or "artifacts/vToken_filter"
    EVENT_ABI_DIR = os.environ.get("KNOWLEDGE_GRAPH_LENDING_EVENT_ABI_DIR") or "artifacts/event-abi"
    LIST_TOKEN_FILTER = os.environ.get(
        "KNOWLEDGE_GRAPH_LENDING_LIST_TOKEN_FILTER") or "artifacts/token_credit_info/listToken.txt"
    TOKEN_INFO = os.environ.get("KNOWLEDGE_GRAPH_LENDING_TOKEN_INFO") or "artifacts/token_credit_info/infoToken.json"
