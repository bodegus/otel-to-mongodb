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
                "MONGODB_URI": "mongodb://localhost:27017",
                "MONGODB_DATABASE": "test_db",
                "ENABLE_CLOUD_SYNC": "false",
            },
        ):
            return MongoDBClient()

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_local_success(self, mock_motor_client, mongo_client):
        """Test successful local connection."""
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        mock_motor_client.return_value = mock_client

        await mongo_client.connect()

        assert mongo_client.local_client == mock_client
        mock_client.admin.command.assert_called_with("ping")

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_local_failure(self, mock_motor_client, mongo_client):
        """Test local connection failure."""
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(side_effect=Exception("Connection failed"))
        mock_motor_client.return_value = mock_client

        with pytest.raises(Exception):
            await mongo_client.connect()

    async def test_write_telemetry_data_success(self, mongo_client):
        """Test successful telemetry data write."""
        # Mock local client
        mock_local_client = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        mock_local_client.__getitem__.return_value.__getitem__.return_value = (
            mock_collection  # noqa: E501
        )
        mock_collection.insert_one.return_value = mock_insert_result
        mock_collection.update_one = AsyncMock()

        mongo_client.local_client = mock_local_client
        mongo_client.cloud_client = None  # No cloud sync

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["local_success"] is True
        assert result["cloud_success"] is False
        assert result["document_id"] == "test_id_123"

        # Verify insert was called
        mock_collection.insert_one.assert_called_once()
        call_args = mock_collection.insert_one.call_args[0][0]
        assert call_args["test"] == "data"
        assert call_args["data_type"] == "traces"
        assert call_args["request_id"] == "test-123"

    async def test_write_telemetry_data_with_cloud_sync(self, mongo_client):
        """Test telemetry data write with cloud sync."""
        # Mock both clients
        mock_local_client = AsyncMock()
        mock_cloud_client = AsyncMock()
        mock_local_collection = AsyncMock()
        mock_cloud_collection = AsyncMock()

        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        # Setup local client
        mock_local_client.__getitem__.return_value.__getitem__.return_value = (
            mock_local_collection  # noqa: E501
        )
        mock_local_collection.insert_one.return_value = mock_insert_result
        mock_local_collection.update_one = AsyncMock()

        # Setup cloud client
        mock_cloud_client.__getitem__.return_value.__getitem__.return_value = (
            mock_cloud_collection  # noqa: E501
        )
        mock_cloud_collection.insert_one = AsyncMock()

        mongo_client.local_client = mock_local_client
        mongo_client.cloud_client = mock_cloud_client
        mongo_client.cloud_db_name = "cloud_test_db"

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["local_success"] is True
        assert result["cloud_success"] is True

        # Verify both inserts were called
        mock_local_collection.insert_one.assert_called_once()
        mock_cloud_collection.insert_one.assert_called_once()
        mock_local_collection.update_one.assert_called_once()

    async def test_health_check_healthy(self, mongo_client):
        """Test health check with healthy databases."""
        mock_local_client = AsyncMock()
        mock_local_client.admin.command = AsyncMock(return_value={"ok": 1})

        mongo_client.local_client = mock_local_client
        mongo_client.cloud_client = None
        mongo_client.enable_cloud_sync = False

        health = await mongo_client.health_check()

        assert health["local"]["connected"] is True
        assert health["local"]["error"] is None
        assert health["cloud"]["connected"] is False
        assert health["cloud"]["enabled"] is False

    async def test_health_check_local_unhealthy(self, mongo_client):
        """Test health check with unhealthy local database."""
        from pymongo.errors import ConnectionFailure

        mock_local_client = AsyncMock()
        mock_local_client.admin.command = AsyncMock(
            side_effect=ConnectionFailure("Connection lost")
        )

        mongo_client.local_client = mock_local_client

        health = await mongo_client.health_check()

        assert health["local"]["connected"] is False
        assert "Connection lost" in health["local"]["error"]
