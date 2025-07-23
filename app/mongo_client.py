"""MongoDB client for dual database operations."""

import os
from datetime import UTC, datetime
from typing import Any

import structlog
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, OperationFailure


logger = structlog.get_logger()


class MongoDBClient:
    """MongoDB client with primary and secondary database support."""

    def __init__(self):
        # Configuration from environment - both optional
        self.primary_uri = os.getenv("PRIMARY_MONGODB_URI")
        self.secondary_uri = os.getenv("SECONDARY_MONGODB_URI")
        self.db_name = os.getenv("MONGODB_DATABASE", "otel_db")

        self.primary_client: AsyncIOMotorClient | None = None
        self.secondary_client: AsyncIOMotorClient | None = None

    async def connect(self) -> None:
        """Connect to available MongoDB instances."""
        logger.info("Connecting to MongoDB instances")

        # Connect to primary database if configured
        if self.primary_uri:
            try:
                self.primary_client = AsyncIOMotorClient(self.primary_uri)
                await self.primary_client.admin.command("ping")
                logger.info("Connected to primary MongoDB", uri=self.primary_uri)
            except (ConnectionFailure, OperationFailure) as e:
                logger.error("Failed to connect to primary MongoDB", error=str(e))
                self.primary_client = None

        # Connect to secondary database if configured
        if self.secondary_uri:
            try:
                self.secondary_client = AsyncIOMotorClient(self.secondary_uri)
                await self.secondary_client.admin.command("ping")
                logger.info("Connected to secondary MongoDB", uri=self.secondary_uri)
            except (ConnectionFailure, OperationFailure) as e:
                logger.error("Failed to connect to secondary MongoDB", error=str(e))
                self.secondary_client = None

        # Ensure at least one database is available
        if not self.primary_client and not self.secondary_client:
            raise ConnectionError("No MongoDB databases available")

    async def disconnect(self) -> None:
        """Disconnect from MongoDB instances."""
        if self.primary_client:
            self.primary_client.close()
        if self.secondary_client:
            self.secondary_client.close()
        logger.info("Disconnected from MongoDB")

    async def write_telemetry_data(
        self, data: dict[str, Any], data_type: str, request_id: str | None = None
    ) -> dict[str, Any]:
        """Write telemetry data to available databases."""
        document = {
            **data,
            "data_type": data_type,
            "request_id": request_id,
            "created_at": datetime.now(UTC).isoformat(),
        }

        results = []

        # Try primary database if available
        if self.primary_client:
            result = await self._write_to_database(
                self.primary_client, "primary", document, data_type
            )
            results.append(result)

        # Try secondary database if available
        if self.secondary_client:
            result = await self._write_to_database(
                self.secondary_client, "secondary", document, data_type
            )
            results.append(result)

        return self._combine_results(results)

    async def _write_to_database(
        self, client: AsyncIOMotorClient, db_type: str, document: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """Write to a specific database."""
        try:
            collection = client[self.db_name][data_type]
            result = await collection.insert_one(document)
            document_id = str(result.inserted_id)

            logger.info(
                "Successfully wrote to database",
                db_type=db_type,
                data_type=data_type,
                document_id=document_id,
            )

            return {
                "success": True,
                "db_type": db_type,
                "document_id": document_id,
                "error": None,
            }
        except Exception as e:
            logger.warning(
                "Failed to write to database",
                db_type=db_type,
                data_type=data_type,
                error=str(e),
            )
            return {
                "success": False,
                "db_type": db_type,
                "document_id": None,
                "error": str(e),
            }

    def _combine_results(self, results: list[dict[str, Any]]) -> dict[str, Any]:
        """Combine write results from multiple databases."""
        if not results:
            return {"success": False, "error": "No databases available"}

        # Extract results by database type
        primary_result = next((r for r in results if r["db_type"] == "primary"), None)
        secondary_result = next((r for r in results if r["db_type"] == "secondary"), None)

        # Success if ANY database succeeded
        any_success = any(r["success"] for r in results)

        # Use primary document_id if available, otherwise secondary
        document_id = None
        if primary_result and primary_result["success"]:
            document_id = primary_result["document_id"]
        elif secondary_result and secondary_result["success"]:
            document_id = secondary_result["document_id"]

        return {
            "success": any_success,
            "primary_success": primary_result["success"] if primary_result else None,
            "secondary_success": secondary_result["success"] if secondary_result else None,
            "document_id": document_id,
            "errors": [r["error"] for r in results if r["error"]],
        }

    async def health_check(self) -> dict[str, Any]:
        """Check health of database connections."""
        health = {
            "primary": {"connected": False, "error": None, "configured": bool(self.primary_uri)},
            "secondary": {
                "connected": False,
                "error": None,
                "configured": bool(self.secondary_uri),
            },
        }

        # Check primary database
        if self.primary_client:
            try:
                await self.primary_client.admin.command("ping")
                health["primary"]["connected"] = True
            except (ConnectionFailure, OperationFailure) as e:
                health["primary"]["error"] = str(e)

        # Check secondary database
        if self.secondary_client:
            try:
                await self.secondary_client.admin.command("ping")
                health["secondary"]["connected"] = True
            except (ConnectionFailure, OperationFailure) as e:
                health["secondary"]["error"] = str(e)

        return health


# Global client instance
_mongodb_client: MongoDBClient | None = None


def get_mongodb_client() -> MongoDBClient:
    """Get MongoDB client dependency."""
    global _mongodb_client
    if _mongodb_client is None:
        _mongodb_client = MongoDBClient()
    return _mongodb_client
