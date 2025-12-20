"""MongoDB client with time series collections for OTEL data."""

import os
import re
from datetime import UTC, datetime
from typing import Any

import structlog
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, OperationFailure


logger = structlog.get_logger()


def _mask_uri_password(uri: str) -> str:
    """Mask password in MongoDB URI for safe logging."""
    if not uri:
        return uri
    pattern = r"(mongodb(?:\+srv)?://[^:]+:)([^@]+)(@.*)"
    return re.sub(pattern, r"\1*****\3", uri)


class MongoDBClient:
    """MongoDB client with primary/secondary failover using time series collections."""

    def __init__(self):
        # Configuration from environment
        self.primary_uri = os.getenv("PRIMARY_MONGODB_URI")
        self.secondary_uri = os.getenv("SECONDARY_MONGODB_URI")
        self.db_name = os.getenv("MONGODB_DATABASE", "otel_db_ts")
        self.granularity = "minutes"

        # Masked URIs for safe logging
        self.primary_logged_uri = _mask_uri_password(self.primary_uri) if self.primary_uri else None
        self.secondary_logged_uri = (
            _mask_uri_password(self.secondary_uri) if self.secondary_uri else None
        )

        self.primary_client: AsyncIOMotorClient | None = None
        self.secondary_client: AsyncIOMotorClient | None = None

        # Track setup status
        self.primary_setup_complete = False
        self.secondary_setup_complete = False

    async def connect(self) -> None:
        """Connect to available MongoDB instances."""
        logger.info("Connecting to MongoDB instances")

        if self.primary_uri:
            try:
                self.primary_client = AsyncIOMotorClient(self.primary_uri)
                await self.primary_client.admin.command("ping")
                logger.info("Connected to primary MongoDB", uri=self.primary_logged_uri)
                await self._ensure_database_setup(self.primary_client, "primary")
                self.primary_setup_complete = True
            except (ConnectionFailure, OperationFailure) as e:
                logger.error("Failed to connect to primary MongoDB", error=str(e))
                self.primary_client = None

        if self.secondary_uri:
            try:
                self.secondary_client = AsyncIOMotorClient(self.secondary_uri)
                await self.secondary_client.admin.command("ping")
                logger.info("Connected to secondary MongoDB", uri=self.secondary_logged_uri)
                await self._ensure_database_setup(self.secondary_client, "secondary")
                self.secondary_setup_complete = True
            except (ConnectionFailure, OperationFailure) as e:
                logger.error("Failed to connect to secondary MongoDB", error=str(e))
                self.secondary_client = None

        if not self.primary_client and not self.secondary_client:
            raise ConnectionError("No MongoDB databases available")

    async def _ensure_database_setup(self, client: AsyncIOMotorClient, db_type: str) -> None:
        """Ensure time series database and collections exist."""
        try:
            logger.info("Setting up database", db_type=db_type, database=self.db_name)

            database = client[self.db_name]
            collections = ["traces", "metrics", "logs"]
            existing = await database.list_collection_names()

            for collection_name in collections:
                if collection_name not in existing:
                    await self._create_timeseries_collection(database, collection_name, db_type)

            logger.info("Database setup completed", db_type=db_type, collections=collections)

        except Exception as e:
            logger.warning("Database setup failed", db_type=db_type, error=str(e))

    async def _create_timeseries_collection(
        self, database, collection_name: str, db_type: str
    ) -> None:
        """Create a time series collection."""
        try:
            await database.create_collection(
                collection_name,
                timeseries={
                    "timeField": "created_at",
                    "granularity": self.granularity,
                },
            )
            logger.info(
                "Created time series collection",
                collection=collection_name,
                db_type=db_type,
                granularity=self.granularity,
            )
        except Exception as e:
            if "already exists" not in str(e).lower():
                logger.warning(
                    "Failed to create time series collection",
                    collection=collection_name,
                    error=str(e),
                )

    async def _ensure_setup_on_write(self, client: AsyncIOMotorClient, db_type: str) -> None:
        """Ensure database setup before writing, if not already completed."""
        setup_complete = (
            self.primary_setup_complete if db_type == "primary" else self.secondary_setup_complete
        )
        if not setup_complete:
            await self._ensure_database_setup(client, db_type)
            if db_type == "primary":
                self.primary_setup_complete = True
            else:
                self.secondary_setup_complete = True

    async def _validate_connection(self, client: AsyncIOMotorClient, db_type: str) -> bool:
        """Validate that a database connection is still active."""
        try:
            await client.admin.command("ping")
            return True
        except Exception as e:
            logger.warning("Database connection validation failed", db_type=db_type, error=str(e))
            return False

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
        now = datetime.now(UTC)

        document = {
            **data,
            "data_type": data_type,
            "request_id": request_id,
            "created_at": now,  # Native datetime for time series timeField
        }

        results = []

        logger.debug(
            "Write attempt",
            primary_client_exists=bool(self.primary_client),
            secondary_client_exists=bool(self.secondary_client),
            data_type=data_type,
            request_id=request_id,
        )

        if self.primary_client:
            if await self._validate_connection(self.primary_client, "primary"):
                result = await self._write_to_database(
                    self.primary_client, "primary", document, data_type
                )
                results.append(result)
            else:
                logger.warning("Primary database connection lost, skipping write")

        if self.secondary_client:
            if await self._validate_connection(self.secondary_client, "secondary"):
                result = await self._write_to_database(
                    self.secondary_client, "secondary", document, data_type
                )
                results.append(result)
            else:
                logger.warning("Secondary database connection lost, skipping write")

        return self._combine_results(results)

    async def _write_to_database(
        self, client: AsyncIOMotorClient, db_type: str, document: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """Write to a specific database."""
        try:
            await self._ensure_setup_on_write(client, db_type)

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
                "error": f"{type(e).__name__}: {str(e)}",
            }

    def _combine_results(self, results: list[dict[str, Any]]) -> dict[str, Any]:
        """Combine write results from multiple databases."""
        if not results:
            return {"success": False, "error": "No databases available"}

        primary_result = next((r for r in results if r["db_type"] == "primary"), None)
        secondary_result = next((r for r in results if r["db_type"] == "secondary"), None)

        any_success = any(r["success"] for r in results)

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
            "database": self.db_name,
            "granularity": self.granularity,
        }

        if self.primary_client:
            try:
                await self.primary_client.admin.command("ping")
                health["primary"]["connected"] = True
            except (ConnectionFailure, OperationFailure) as e:
                health["primary"]["error"] = str(e)

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
