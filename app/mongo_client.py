"""MongoDB client for dual database operations."""

import os
import re
from datetime import UTC, datetime
from typing import Any

import structlog
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import WriteConcern
from pymongo.errors import ConnectionFailure, OperationFailure


logger = structlog.get_logger()


def _mask_uri_password(uri: str) -> str:
    """
    Mask password in MongoDB URI for safe logging.

    Replaces password component with '*****' in connection strings.
    """
    if not uri:
        return uri

    # Pattern matches mongodb:// or mongodb+srv:// with username:password@
    # Captures: protocol://username:password@rest
    pattern = r"(mongodb(?:\+srv)?://[^:]+:)([^@]+)(@.*)"
    return re.sub(pattern, r"\1*****\3", uri)


class MongoDBClient:
    """MongoDB client with primary and secondary database support."""

    def __init__(self):
        # Configuration from environment - both optional
        self.primary_uri = os.getenv("PRIMARY_MONGODB_URI")
        self.secondary_uri = os.getenv("SECONDARY_MONGODB_URI")
        self.db_name = os.getenv("MONGODB_DATABASE", "otel_db")

        # Create masked URIs for safe logging
        self.primary_logged_uri = _mask_uri_password(self.primary_uri) if self.primary_uri else None
        self.secondary_logged_uri = (
            _mask_uri_password(self.secondary_uri) if self.secondary_uri else None
        )

        self.primary_client: AsyncIOMotorClient | None = None
        self.secondary_client: AsyncIOMotorClient | None = None

        # Track database setup status
        self.primary_setup_complete = False
        self.secondary_setup_complete = False

    async def connect(self) -> None:
        """Connect to available MongoDB instances."""
        logger.info("Connecting to MongoDB instances")

        # Connect to primary database if configured
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

        # Connect to secondary database if configured
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

        # Ensure at least one database is available
        if not self.primary_client and not self.secondary_client:
            raise ConnectionError("No MongoDB databases available")

    async def _ensure_database_setup(self, client: AsyncIOMotorClient, db_type: str) -> None:
        """Ensure database, collections, and indexes exist."""
        try:
            logger.info("Setting up database structure", db_type=db_type, database=self.db_name)

            database = client[self.db_name]
            otel_collections = ["traces", "metrics", "logs"]

            # Create collections and ensure indexes
            for collection_name in otel_collections:
                collection = database[collection_name]
                await self._ensure_indexes(collection, collection_name, db_type)

            logger.info("Database setup completed", db_type=db_type, collections=otel_collections)

        except Exception as e:
            logger.warning(
                "Database setup failed, continuing without setup", db_type=db_type, error=str(e)
            )

    async def _ensure_indexes(self, collection, collection_name: str, db_type: str) -> None:
        """Ensure required indexes exist on collection."""
        try:
            # Create index on created_at field for time-based queries
            await collection.create_index(
                "created_at", background=True, name=f"{collection_name}_created_at_idx"
            )
            logger.debug("Created index on created_at", db_type=db_type, collection=collection_name)
        except Exception as e:
            logger.warning(
                "Failed to create index", db_type=db_type, collection=collection_name, error=str(e)
            )

    async def _ensure_database_setup_on_write(
        self, client: AsyncIOMotorClient, db_type: str
    ) -> None:
        """Ensure database setup before writing, if not already completed."""
        setup_complete = (
            self.primary_setup_complete if db_type == "primary" else self.secondary_setup_complete
        )

        if not setup_complete:
            await self._ensure_database_setup(client, db_type)
            # Mark as complete
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
        document = {
            **data,
            "data_type": data_type,
            "request_id": request_id,
            "created_at": datetime.now(UTC).isoformat(),
        }

        results = []

        # Debug connection status
        logger.debug(
            "Write attempt - connection status",
            primary_client_exists=bool(self.primary_client),
            secondary_client_exists=bool(self.secondary_client),
            data_type=data_type,
            request_id=request_id,
        )

        # Validate and try primary database
        if self.primary_client:
            if await self._validate_connection(self.primary_client, "primary"):
                result = await self._write_to_database(
                    self.primary_client, "primary", document, data_type
                )
                results.append(result)
            else:
                logger.warning("Primary database connection lost, skipping write")

        # Validate and try secondary database
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
            # Ensure database setup on first write if not done during connection
            await self._ensure_database_setup_on_write(client, db_type)

            # Get database with write concern to ensure data is fully persisted
            write_concern = WriteConcern(w="majority", j=True)
            database = client.get_database(self.db_name, write_concern=write_concern)
            collection = database[data_type]
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
            error_details = {
                "exception_type": type(e).__name__,
                "error_message": str(e),
                "db_type": db_type,
                "data_type": data_type,
            }
            logger.warning(
                "Failed to write to database",
                **error_details,
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
