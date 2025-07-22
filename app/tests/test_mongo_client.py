"""Tests for MongoDB client."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.mongo_client import MongoDBClient


@pytest.mark.asyncio
class TestMongoDBClient:
    """Test MongoDB client functionality."""

    @pytest.fixture
    def mongo_client(self):
        """MongoDB client instance."""
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://localhost:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            return MongoDBClient()

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_primary_success(self, mock_motor_client, mongo_client):
        """Test successful primary connection."""
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        mock_motor_client.return_value = mock_client

        await mongo_client.connect()

        assert mongo_client.primary_client == mock_client
        mock_client.admin.command.assert_called_with("ping")

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_no_databases_available(self, mongo_client):
        """Test connection failure when no databases are available."""
        # Mock environment with no URIs
        with patch.dict("os.environ", {}, clear=True):
            mongo_client_no_uri = MongoDBClient()

        with pytest.raises(ConnectionError, match="No MongoDB databases available"):
            await mongo_client_no_uri.connect()

    async def test_write_telemetry_data_success(self, mongo_client):
        """Test successful telemetry data write to primary database."""
        # Mock primary client
        mock_primary_client = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        mock_primary_client.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_collection.insert_one.return_value = mock_insert_result

        mongo_client.primary_client = mock_primary_client
        mongo_client.secondary_client = None  # No secondary database

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is True
        assert result["primary_success"] is True
        assert result["secondary_success"] is None
        assert result["document_id"] == "test_id_123"

        # Verify insert was called
        mock_collection.insert_one.assert_called_once()
        call_args = mock_collection.insert_one.call_args[0][0]
        assert call_args["test"] == "data"
        assert call_args["data_type"] == "traces"
        assert call_args["request_id"] == "test-123"

    async def test_write_telemetry_data_both_databases(self, mongo_client):
        """Test telemetry data write to both primary and secondary databases."""
        # Mock both clients
        mock_primary_client = AsyncMock()
        mock_secondary_client = AsyncMock()
        mock_primary_collection = AsyncMock()
        mock_secondary_collection = AsyncMock()

        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        # Setup primary client
        mock_primary_client.__getitem__.return_value.__getitem__.return_value = (
            mock_primary_collection
        )
        mock_primary_collection.insert_one.return_value = mock_insert_result

        # Setup secondary client
        mock_secondary_client.__getitem__.return_value.__getitem__.return_value = (
            mock_secondary_collection
        )
        mock_secondary_collection.insert_one.return_value = mock_insert_result

        mongo_client.primary_client = mock_primary_client
        mongo_client.secondary_client = mock_secondary_client

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is True
        assert result["primary_success"] is True
        assert result["secondary_success"] is True

        # Verify both inserts were called
        mock_primary_collection.insert_one.assert_called_once()
        mock_secondary_collection.insert_one.assert_called_once()

    async def test_health_check_healthy(self, mongo_client):
        """Test health check with healthy primary database."""
        mock_primary_client = AsyncMock()
        mock_primary_client.admin.command = AsyncMock(return_value={"ok": 1})

        mongo_client.primary_client = mock_primary_client
        mongo_client.secondary_client = None

        health = await mongo_client.health_check()

        assert health["primary"]["connected"] is True
        assert health["primary"]["error"] is None
        assert health["secondary"]["connected"] is False
        assert health["secondary"]["configured"] is False

    async def test_health_check_primary_unhealthy(self, mongo_client):
        """Test health check with unhealthy primary database."""
        from pymongo.errors import ConnectionFailure

        mock_primary_client = AsyncMock()
        mock_primary_client.admin.command = AsyncMock(
            side_effect=ConnectionFailure("Connection lost")
        )

        mongo_client.primary_client = mock_primary_client

        health = await mongo_client.health_check()

        assert health["primary"]["connected"] is False
        assert "Connection lost" in health["primary"]["error"]
