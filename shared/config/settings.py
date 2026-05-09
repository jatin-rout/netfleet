import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent


def load_yaml(filename: str) -> dict:
    filepath = BASE_DIR / filename
    with open(filepath, "r") as f:
        return yaml.safe_load(f)


class MongoConfig:
    URI = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017"
    )
    DB_NAME = os.getenv(
        "MONGO_DB_NAME",
        "netfleet"
    )
    COLLECTIONS = {
        "devices": "devices",
        "jobs": "jobs",
        "job_executions": "job_executions",
        "device_stats": "device_stats",
        "discovery_backup": "discovery_backup",
        "operation_results": "operation_results"
    }


class RedisConfig:
    HOST = os.getenv("REDIS_HOST", "localhost")
    PORT = int(os.getenv("REDIS_PORT", 6379))
    DB = int(os.getenv("REDIS_DB", 0))

    QUEUES = {
        "higher": "queue_higher_segments",
        "lower": "queue_lower_segments",
        "raw_results": "queue_raw_results",
        "normalized": "queue_normalized_records",
        "db_insert": "queue_db_insert",
        "rag_raw": "queue_rag_raw",
        "job_events": "queue_job_events",
        "job_trigger": "queue_job_trigger",
    }

    CACHE_KEYS = {
        "job_progress": "job_progress:{execution_id}",
        "component_health": "health:{component}",
        "device_ip_map": "device_ip:{identity}"
    }

    CACHE_TIMEOUT = int(
        os.getenv("CACHE_TIMEOUT_MINUTES", 30)
    ) * 60


class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )
    CONSUMER_GROUP = os.getenv(
        "KAFKA_CONSUMER_GROUP",
        "netfleet-workers"
    )
    TOPICS = {
        "higher": "netfleet.higher.segments",
        "lower": "netfleet.lower.segments",
        "raw_results": "netfleet.raw.results",
        "normalized": "netfleet.normalized.records",
        "db_insert": "netfleet.db.insert",
        "discovery_events": "netfleet.discovery.events",
        "rag_raw": "netfleet.rag.raw",
        "job_events": "netfleet.job.events",
        "job_trigger": "netfleet.job.trigger",
    }


class CollectorConfig:
    THREAD_POOL_SIZE = int(
        os.getenv("THREAD_POOL_SIZE", 150)
    )
    MAX_INSTANCES = int(
        os.getenv("MAX_INSTANCES", 6)
    )
    DEVICE_TIMEOUT = int(
        os.getenv("DEVICE_TIMEOUT", 30)
    )
    AUTH_RETRY_COUNT = int(
        os.getenv("AUTH_RETRY_COUNT", 2)
    )
    ERROR_THRESHOLD = int(
        os.getenv("ERROR_THRESHOLD", 100)
    )


class NormalizerConfig:
    TEXTFSM_TEMPLATES_PATH = os.getenv(
        "TEXTFSM_TEMPLATES_PATH",
        "/app/templates/textfsm"
    )
    ERROR_THRESHOLD = int(
        os.getenv(
            "NORMALIZER_ERROR_THRESHOLD",
            100
        )
    )
    BATCH_SIZE = int(
        os.getenv("NORMALIZER_BATCH_SIZE", 50)
    )


class OrchestratorConfig:
    JOB_CHECK_INTERVAL = int(
        os.getenv("JOB_CHECK_INTERVAL", 30)
    )
    QUEUE_POLL_INTERVAL = int(
        os.getenv("QUEUE_POLL_INTERVAL", 5)
    )
    JOB_TIMEOUT_MINUTES = int(
        os.getenv("JOB_TIMEOUT_MINUTES", 120)
    )


class InventoryConfig:
    DELTA_THRESHOLD = float(
        os.getenv("DELTA_THRESHOLD_PERCENTAGE", 10)
    )
    PRIMARY_DB_URI = os.getenv(
        "PRIMARY_DB_URI",
        "jdbc:hive2://localhost:10000"
    )
    SECONDARY_FILES_PATH = os.getenv(
        "SECONDARY_FILES_PATH",
        "/data/inventory/files"
    )
    INVENTORY_MODE = os.getenv(
        "INVENTORY_MODE",
        "cron"
    )


class SimulatorConfig:
    HOST = os.getenv("SIMULATOR_HOST", "localhost")
    PORT = int(os.getenv("SIMULATOR_PORT", 8888))
    DEVICE_COUNT = int(
        os.getenv("SIMULATOR_DEVICE_COUNT", 100)
    )


class SegmentConfig:
    HIGHER_SEGMENTS = ["Tier1", "Tier2", "Tier3"]
    LOWER_SEGMENTS = ["Edge", "Field"]

    PRIORITY_MAP = {
        "Tier1": "HIGH",
        "Tier2": "HIGH",
        "Tier3": "HIGH",
        "Edge": "STANDARD",
        "Field": "STANDARD"
    }

    QUEUE_MAP_REDIS = {
        "Tier1": "queue_higher_segments",
        "Tier2": "queue_higher_segments",
        "Tier3": "queue_higher_segments",
        "Edge": "queue_lower_segments",
        "Field": "queue_lower_segments"
    }

    QUEUE_MAP_KAFKA = {
        "Tier1": "netfleet.higher.segments",
        "Tier2": "netfleet.higher.segments",
        "Tier3": "netfleet.higher.segments",
        "Edge": "netfleet.lower.segments",
        "Field": "netfleet.lower.segments"
    }

    IDENTITY_MAP = {
        "Tier1": "serial_number",
        "Tier2": "serial_number",
        "Tier3": "serial_number",
        "Edge": "mac_address",
        "Field": "mac_address"
    }

    VENDOR_MAP = {
        "Tier1": ["cisco_ios", "huawei_vrp",
                  "juniper_junos"],
        "Tier2": ["cisco_ios", "huawei_vrp"],
        "Tier3": ["cisco_ios", "huawei_vrp",
                  "juniper_junos"],
        "Edge": ["bdcom", "zte_zxros",
                 "utstarcom"],
        "Field": ["bdcom", "utstarcom"]
    }


class TransportConfig:
    MODE = os.getenv("TRANSPORT_MODE", "redis")


class QdrantConfig:
    HOST = os.getenv("QDRANT_HOST", "localhost")
    PORT = int(os.getenv("QDRANT_PORT", 6333))
    COLLECTION = os.getenv(
        "QDRANT_COLLECTION",
        "netfleet_device_output"
    )


class RAGConfig:
    EMBEDDING_MODEL = os.getenv(
        "EMBEDDING_MODEL",
        "all-MiniLM-L6-v2"
    )
    CHUNK_TOKENS = int(
        os.getenv("EMBEDDING_CHUNK_TOKENS", 512)
    )
    BATCH_SIZE = int(os.getenv("RAG_BATCH_SIZE", 20))


class NLQueryConfig:
    MAX_RESULTS = int(
        os.getenv("NL_QUERY_MAX_RESULTS", 50)
    )
    TIMEOUT_SECONDS = int(
        os.getenv("NL_QUERY_TIMEOUT_SECONDS", 30)
    )
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


SEGMENTS = load_yaml("segments.yaml")
JOBS = load_yaml("jobs.yaml")
OPERATIONS = load_yaml("operations.yaml")
