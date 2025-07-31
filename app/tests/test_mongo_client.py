"""Test MongoDB client functionality with comprehensive edge case coverage."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pymongo.errors import ConnectionFailure, OperationFailure

from app.mongo_client import MongoDBClient, get_mongodb_client


@pytest.mark.unit
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

    @pytest.mark.unit
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
    async def test_connect_primary_failure(self, mock_motor_client):
        """Test primary connection failure with graceful degradation."""
        # Setup environment for both primary and secondary
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://localhost:27017",
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            mongo_client = MongoDBClient()

        # Mock primary failure, secondary success
        mock_primary_client = AsyncMock()
        mock_secondary_client = AsyncMock()

        mock_primary_client.admin.command = AsyncMock(
            side_effect=ConnectionFailure("Primary connection failed")
        )
        mock_secondary_client.admin.command = AsyncMock(return_value={"ok": 1})

        # Return different clients for different URIs
        def side_effect(uri):
            if "localhost" in uri:
                return mock_primary_client
            return mock_secondary_client

        mock_motor_client.side_effect = side_effect

        await mongo_client.connect()

        # Primary should be None due to failure, secondary should be connected
        assert mongo_client.primary_client is None
        assert mongo_client.secondary_client == mock_secondary_client

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_secondary_success(self, mock_motor_client):
        """Test secondary connection success."""
        with patch.dict(
            "os.environ",
            {
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            mongo_client = MongoDBClient()

        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        mock_motor_client.return_value = mock_client

        await mongo_client.connect()

        assert mongo_client.secondary_client == mock_client
        assert mongo_client.primary_client is None

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_secondary_failure(self, mock_motor_client):
        """Test secondary connection failure."""
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://primary:27017",
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            mongo_client = MongoDBClient()

        # Mock primary success, secondary failure
        mock_primary_client = AsyncMock()
        mock_secondary_client = AsyncMock()

        mock_primary_client.admin.command = AsyncMock(return_value={"ok": 1})
        mock_secondary_client.admin.command = AsyncMock(
            side_effect=OperationFailure("Secondary connection failed")
        )

        def side_effect(uri):
            if "primary" in uri:
                return mock_primary_client
            return mock_secondary_client

        mock_motor_client.side_effect = side_effect

        await mongo_client.connect()

        # Primary should be connected, secondary should be None due to failure
        assert mongo_client.primary_client == mock_primary_client
        assert mongo_client.secondary_client is None

    async def test_connect_no_databases_available(self, mongo_client):
        """Test connection failure when no databases are available."""
        # Mock environment with no URIs
        with patch.dict("os.environ", {}, clear=True):
            mongo_client_no_uri = MongoDBClient()

        with pytest.raises(ConnectionError, match="No MongoDB databases available"):
            await mongo_client_no_uri.connect()

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_both_fail_raises_error(self, mock_motor_client):
        """Test that ConnectionError is raised when both databases fail to connect."""
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://primary:27017",
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            mongo_client = MongoDBClient()

        # Mock both clients to fail
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(side_effect=ConnectionFailure("Connection failed"))
        mock_motor_client.return_value = mock_client

        with pytest.raises(ConnectionError, match="No MongoDB databases available"):
            await mongo_client.connect()

    async def test_disconnect_both_clients(self, mongo_client):
        """Test disconnecting from both databases."""
        # Mock both clients with MagicMock for close() to avoid warnings
        mock_primary = MagicMock()
        mock_secondary = MagicMock()

        mongo_client.primary_client = mock_primary
        mongo_client.secondary_client = mock_secondary

        await mongo_client.disconnect()

        mock_primary.close.assert_called_once()
        mock_secondary.close.assert_called_once()

    async def test_disconnect_partial_clients(self, mongo_client):
        """Test disconnecting when only some clients are available."""
        # Mock only primary client with MagicMock for close() to avoid warnings
        mock_primary = MagicMock()
        mongo_client.primary_client = mock_primary
        mongo_client.secondary_client = None

        await mongo_client.disconnect()

        mock_primary.close.assert_called_once()

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

    async def test_write_telemetry_data_primary_fail_secondary_success(self, mongo_client):
        """Test telemetry write when primary fails but secondary succeeds."""
        # Mock both clients
        mock_primary_client = AsyncMock()
        mock_secondary_client = AsyncMock()
        mock_primary_collection = AsyncMock()
        mock_secondary_collection = AsyncMock()

        mock_secondary_result = MagicMock()
        mock_secondary_result.inserted_id = "secondary_id_456"

        # Setup primary client to fail
        mock_primary_client.__getitem__.return_value.__getitem__.return_value = (
            mock_primary_collection
        )
        mock_primary_collection.insert_one.side_effect = Exception("Primary write failed")

        # Setup secondary client to succeed
        mock_secondary_client.__getitem__.return_value.__getitem__.return_value = (
            mock_secondary_collection
        )
        mock_secondary_collection.insert_one.return_value = mock_secondary_result

        mongo_client.primary_client = mock_primary_client
        mongo_client.secondary_client = mock_secondary_client

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is True  # Overall success because secondary succeeded
        assert result["primary_success"] is False
        assert result["secondary_success"] is True
        assert result["document_id"] == "secondary_id_456"  # Uses secondary ID
        assert len(result["errors"]) == 1
        assert "Primary write failed" in result["errors"][0]

    async def test_write_telemetry_data_both_fail(self, mongo_client):
        """Test telemetry write when both databases fail."""
        # Mock both clients
        mock_primary_client = AsyncMock()
        mock_secondary_client = AsyncMock()
        mock_primary_collection = AsyncMock()
        mock_secondary_collection = AsyncMock()

        # Setup both clients to fail
        mock_primary_client.__getitem__.return_value.__getitem__.return_value = (
            mock_primary_collection
        )
        mock_primary_collection.insert_one.side_effect = Exception("Primary write failed")

        mock_secondary_client.__getitem__.return_value.__getitem__.return_value = (
            mock_secondary_collection
        )
        mock_secondary_collection.insert_one.side_effect = Exception("Secondary write failed")

        mongo_client.primary_client = mock_primary_client
        mongo_client.secondary_client = mock_secondary_client

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is False  # Overall failure
        assert result["primary_success"] is False
        assert result["secondary_success"] is False
        assert result["document_id"] is None
        assert len(result["errors"]) == 2

    async def test_write_telemetry_data_no_databases(self, mongo_client):
        """Test telemetry write when no databases are available."""
        mongo_client.primary_client = None
        mongo_client.secondary_client = None

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is False
        assert result["error"] == "No databases available"

    async def test_write_telemetry_data_without_request_id(self, mongo_client):
        """Test telemetry write without request_id parameter."""
        # Mock primary client
        mock_primary_client = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        mock_primary_client.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_collection.insert_one.return_value = mock_insert_result

        mongo_client.primary_client = mock_primary_client
        mongo_client.secondary_client = None

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"},
            data_type="traces",  # No request_id
        )

        assert result["success"] is True

        # Verify request_id is None in the document
        call_args = mock_collection.insert_one.call_args[0][0]
        assert call_args["request_id"] is None

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

    async def test_health_check_secondary_unhealthy(self, mongo_client):
        """Test health check with unhealthy secondary database."""
        from pymongo.errors import OperationFailure

        mock_secondary_client = AsyncMock()
        mock_secondary_client.admin.command = AsyncMock(
            side_effect=OperationFailure("Operation failed")
        )

        mongo_client.secondary_client = mock_secondary_client

        health = await mongo_client.health_check()

        assert health["secondary"]["connected"] is False
        assert "Operation failed" in health["secondary"]["error"]

    async def test_health_check_both_databases_configured(self, mongo_client):
        """Test health check with both databases configured."""
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://primary:27017",
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            mongo_client_both = MongoDBClient()

        mock_primary_client = AsyncMock()
        mock_secondary_client = AsyncMock()

        mock_primary_client.admin.command = AsyncMock(return_value={"ok": 1})
        mock_secondary_client.admin.command = AsyncMock(return_value={"ok": 1})

        mongo_client_both.primary_client = mock_primary_client
        mongo_client_both.secondary_client = mock_secondary_client

        health = await mongo_client_both.health_check()

        assert health["primary"]["connected"] is True
        assert health["primary"]["configured"] is True
        assert health["secondary"]["connected"] is True
        assert health["secondary"]["configured"] is True


@pytest.mark.unit
class TestGetMongoDBClient:
    """Test the singleton MongoDB client getter function."""

    def test_get_mongodb_client_singleton(self):
        """Test that get_mongodb_client returns the same instance."""
        # Clear any existing global instance
        import app.mongo_client

        app.mongo_client._mongodb_client = None

        client1 = get_mongodb_client()
        client2 = get_mongodb_client()

        assert client1 is client2  # Should be the same instance

    def test_get_mongodb_client_creates_instance(self):
        """Test that get_mongodb_client creates an instance when none exists."""
        # Clear any existing global instance
        import app.mongo_client

        app.mongo_client._mongodb_client = None

        client = get_mongodb_client()

        assert isinstance(client, MongoDBClient)
        assert app.mongo_client._mongodb_client is client
