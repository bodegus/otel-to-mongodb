"""MongoDB client for dual database operations."""

import os
from datetime import UTC, datetime
from typing import Any, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, OperationFailure

logger = structlog.get_logger()


class MongoDBClient:
    """MongoDB client with local and cloud database support."""

    def __init__(self):
        # Configuration from environment
        self.local_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.local_db_name = os.getenv("MONGODB_DATABASE", "otel_db")
        self.cloud_uri = os.getenv("CLOUD_MONGODB_URI")
        self.cloud_db_name = os.getenv("CLOUD_MONGODB_DATABASE")
        self.enable_cloud_sync = bool(os.getenv("ENABLE_CLOUD_SYNC", "false"))

        self.local_client: Optional[AsyncIOMotorClient] = None
        self.cloud_client: Optional[AsyncIOMotorClient] = None

    async def connect(self):
        """Connect to MongoDB instances."""
        logger.info("Connecting to MongoDB", local_uri=self.local_uri)

        # Connect to local database (required)
        self.local_client = AsyncIOMotorClient(self.local_uri)
        try:
            await self.local_client.admin.command("ping")
            logger.info("Connected to local MongoDB")
        except (ConnectionFailure, OperationFailure) as e:
            logger.error("Failed to connect to local MongoDB", error=str(e))
            raise

        # Connect to cloud database (optional)
        if self.enable_cloud_sync and self.cloud_uri:
            try:
                self.cloud_client = AsyncIOMotorClient(self.cloud_uri)
                await self.cloud_client.admin.command("ping")
                logger.info("Connected to cloud MongoDB")
            except (ConnectionFailure, OperationFailure) as e:
                logger.warning("Failed to connect to cloud MongoDB", error=str(e))
                self.cloud_client = None
                raise

    async def disconnect(self):
        """Disconnect from MongoDB instances."""
        if self.local_client:
            self.local_client.close()
        if self.cloud_client:
            self.cloud_client.close()
        logger.info("Disconnected from MongoDB")

    async def write_telemetry_data(
        self, data: dict[str, Any], data_type: str, request_id: str
    ) -> dict[str, Any]:
        """Write telemetry data to databases."""
        document = {
            **data,
            "data_type": data_type,
            "request_id": request_id,
            "created_at": datetime.now(UTC).isoformat(),
            "cloud_synced": False,
            "sync_attempts": 0,
        }

        result = {"local_success": False, "cloud_success": False, "document_id": None}

        # Write to local database (required)
        try:
            local_db = self.local_client[self.local_db_name]
            local_collection = local_db[data_type]
            local_result = await local_collection.insert_one(document)
            result["local_success"] = True
            result["document_id"] = str(local_result.inserted_id)
            logger.info(
                "Wrote to local database", data_type=data_type, document_id=result["document_id"]
            )
        except Exception as e:
            logger.error("Failed to write to local database", error=str(e))
            raise

        # Write to cloud database (optional)
        if self.cloud_client:
            try:
                cloud_db = self.cloud_client[self.cloud_db_name]
                cloud_collection = cloud_db[data_type]
                cloud_document = {**document, "cloud_synced": True}
                await cloud_collection.insert_one(cloud_document)
                result["cloud_success"] = True

                # Mark local document as synced
                await local_collection.update_one(
                    {"_id": local_result.inserted_id},
                    {
                        "$set": {
                            "cloud_synced": True,
                            "synced_at": datetime.now(UTC).isoformat(),
                        }
                    },
                )
                logger.info("Wrote to cloud database", data_type=data_type)
            except Exception as e:
                logger.warning("Failed to write to cloud database", error=str(e))
                # Add to failed queue for retry
                await self._add_to_failed_queue(document, data_type, str(e))

        return result

    async def _add_to_failed_queue(self, document: dict[str, Any], data_type: str, error: str):
        """Add failed sync to retry queue."""
        try:
            failed_collection = self.local_client[self.local_db_name][f"failed_{data_type}"]
            failed_document = {
                **document,
                "failed_at": datetime.now(UTC).isoformat(),
                "error": error,
                "retry_count": 0,
            }
            await failed_collection.insert_one(failed_document)
            logger.info("Added to failed queue", data_type=data_type)
        except Exception as e:
            logger.error("Failed to add to failed queue", error=str(e))

    async def health_check(self) -> dict[str, Any]:
        """Check health of database connections."""
        health = {
            "local": {"connected": False, "error": None},
            "cloud": {"connected": False, "error": None, "enabled": self.enable_cloud_sync},
        }

        # Check local database
        try:
            if self.local_client:
                await self.local_client.admin.command("ping")
                health["local"]["connected"] = True
        except (ConnectionFailure, OperationFailure) as e:
            health["local"]["error"] = str(e)

        # Check cloud database
        if self.enable_cloud_sync and self.cloud_client:
            try:
                await self.cloud_client.admin.command("ping")
                health["cloud"]["connected"] = True
            except (ConnectionFailure, OperationFailure) as e:
                health["cloud"]["error"] = str(e)

        return health


# Global client instance
_mongodb_client: Optional[MongoDBClient] = None


def get_mongodb_client() -> MongoDBClient:
    """Get MongoDB client dependency."""
    global _mongodb_client
    if _mongodb_client is None:
        _mongodb_client = MongoDBClient()
    return _mongodb_client
