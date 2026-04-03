import os
from typing import Optional, List
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from shared.utils.logger import get_logger

logger = get_logger("mongo_client")


class MongoDBClient:
    _instance: Optional["MongoDBClient"] = None
    _client: Optional[MongoClient] = None
    _db: Optional[Database] = None

    def __new__(cls) -> "MongoDBClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            uri = os.getenv(
                "MONGO_URI",
                "mongodb://localhost:27017"
            )
            self._client = MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                maxPoolSize=50,
                minPoolSize=5
            )
            db_name = os.getenv(
                "MONGO_DB_NAME",
                "netfleet"
            )
            self._db = self._client[db_name]
            self._ensure_indexes()
            logger.info(
                f"MongoDB client initialized — "
                f"db: {db_name}"
            )

    def _ensure_indexes(self):
        try:
            self._db["devices"].create_index(
                [("ip_address", ASCENDING)],
                unique=True
            )
            self._db["devices"].create_index(
                [("segment", ASCENDING),
                 ("status", ASCENDING)]
            )
            self._db["devices"].create_index(
                [("mac_address", ASCENDING)]
            )
            self._db["devices"].create_index(
                [("serial_number", ASCENDING)]
            )
            self._db["job_executions"].create_index(
                [("execution_id", ASCENDING)],
                unique=True
            )
            self._db["job_executions"].create_index(
                [("job_id", ASCENDING),
                 ("status", ASCENDING)]
            )
            self._db["operation_results"].create_index(
                [("execution_id", ASCENDING),
                 ("device_id", ASCENDING)]
            )
            logger.info("MongoDB indexes ensured")
        except Exception as e:
            logger.error(
                f"MongoDB index creation failed: {e}"
            )

    def get_collection(
        self,
        name: str
    ) -> Collection:
        return self._db[name]

    def insert_many(
        self,
        collection_name: str,
        documents: List[dict],
        ordered: bool = False
    ) -> int:
        try:
            collection = self.get_collection(
                collection_name
            )
            result = collection.insert_many(
                documents,
                ordered=ordered
            )
            inserted = len(result.inserted_ids)
            logger.info(
                f"Inserted {inserted} docs "
                f"into {collection_name}"
            )
            return inserted
        except Exception as e:
            logger.error(
                f"MongoDB insert_many failed: {e}"
            )
            raise

    def find_many(
        self,
        collection_name: str,
        query: dict,
        projection: Optional[dict] = None,
        limit: int = 0
    ) -> List[dict]:
        try:
            collection = self.get_collection(
                collection_name
            )
            cursor = collection.find(
                query,
                projection
            )
            if limit:
                cursor = cursor.limit(limit)
            return list(cursor)
        except Exception as e:
            logger.error(
                f"MongoDB find_many failed: {e}"
            )
            raise

    def find_one(
        self,
        collection_name: str,
        query: dict
    ) -> Optional[dict]:
        try:
            collection = self.get_collection(
                collection_name
            )
            return collection.find_one(query)
        except Exception as e:
            logger.error(
                f"MongoDB find_one failed: {e}"
            )
            raise

    def update_one(
        self,
        collection_name: str,
        query: dict,
        update: dict,
        upsert: bool = False
    ) -> int:
        try:
            collection = self.get_collection(
                collection_name
            )
            result = collection.update_one(
                query,
                update,
                upsert=upsert
            )
            return result.modified_count
        except Exception as e:
            logger.error(
                f"MongoDB update_one failed: {e}"
            )
            raise

    def count_documents(
        self,
        collection_name: str,
        query: dict
    ) -> int:
        try:
            collection = self.get_collection(
                collection_name
            )
            return collection.count_documents(query)
        except Exception as e:
            logger.error(
                f"MongoDB count failed: {e}"
            )
            raise

    def truncate_collection(
        self,
        collection_name: str
    ) -> int:
        try:
            collection = self.get_collection(
                collection_name
            )
            result = collection.delete_many({})
            logger.warning(
                f"Truncated {collection_name} — "
                f"deleted {result.deleted_count} docs"
            )
            return result.deleted_count
        except Exception as e:
            logger.error(
                f"MongoDB truncate failed: {e}"
            )
            raise

    def backup_collection(
        self,
        source: str,
        backup: str
    ) -> int:
        try:
            documents = self.find_many(source, {})
            if not documents:
                return 0
            self.truncate_collection(backup)
            return self.insert_many(backup, documents)
        except Exception as e:
            logger.error(
                f"MongoDB backup failed: {e}"
            )
            raise

    def health_check(self) -> bool:
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False
