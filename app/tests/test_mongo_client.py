"""Test MongoDB client functionality with time series collections."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pymongo.errors import ConnectionFailure, OperationFailure

from app.mongo_client import MongoDBClient, get_mongodb_client


@pytest.mark.unit
class TestMongoDBClient:
    """Test MongoDB client functionality."""

    @pytest.fixture
    def client(self):
        """Create MongoDB client with mocked environment."""
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://localhost:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            return MongoDBClient()

    @pytest.fixture
    def client_with_secondary(self):
        """Create MongoDB client with both primary and secondary."""
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://primary:27017",
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            return MongoDBClient()

    def test_default_database_name(self):
        """Test default database name is otel_db_ts."""
        with patch.dict(
            "os.environ", {"PRIMARY_MONGODB_URI": "mongodb://localhost:27017"}, clear=True
        ):
            client = MongoDBClient()
            assert client.db_name == "otel_db_ts"

    def test_custom_database_name(self, client):
        """Test custom database name from environment."""
        assert client.db_name == "test_db"

    def test_granularity_is_minutes(self, client):
        """Test granularity is hardcoded to minutes."""
        assert client.granularity == "minutes"

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_primary_success(self, mock_motor_client, client):
        """Test successful primary connection."""
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        mock_database = AsyncMock()
        mock_database.list_collection_names = AsyncMock(return_value=[])
        mock_database.create_collection = AsyncMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_motor_client.return_value = mock_client

        await client.connect()

        assert client.primary_client is not None
        assert client.primary_setup_complete is True

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_primary_failure(self, mock_motor_client, client_with_secondary):
        """Test fallback to secondary when primary fails."""
        mock_primary = AsyncMock()
        mock_primary.admin.command = AsyncMock(
            side_effect=ConnectionFailure("Primary connection failed")
        )

        mock_secondary = AsyncMock()
        mock_secondary.admin.command = AsyncMock(return_value={"ok": 1})
        mock_database = AsyncMock()
        mock_database.list_collection_names = AsyncMock(return_value=[])
        mock_database.create_collection = AsyncMock()
        mock_secondary.__getitem__ = MagicMock(return_value=mock_database)

        mock_motor_client.side_effect = [mock_primary, mock_secondary]

        await client_with_secondary.connect()

        assert client_with_secondary.primary_client is None
        assert client_with_secondary.secondary_client is not None

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_secondary_success(self, mock_motor_client):
        """Test secondary connection success when only secondary is configured."""
        with patch.dict(
            "os.environ",
            {
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
            clear=True,
        ):
            client = MongoDBClient()

        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        mock_database = AsyncMock()
        mock_database.list_collection_names = AsyncMock(return_value=[])
        mock_database.create_collection = AsyncMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_motor_client.return_value = mock_client

        await client.connect()

        assert client.secondary_client == mock_client
        assert client.primary_client is None

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_secondary_failure_primary_success(self, mock_motor_client):
        """Test secondary connection failure when primary succeeds."""
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://primary:27017",
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            client = MongoDBClient()

        mock_primary = AsyncMock()
        mock_secondary = AsyncMock()
        mock_primary.admin.command = AsyncMock(return_value={"ok": 1})
        mock_secondary.admin.command = AsyncMock(
            side_effect=OperationFailure("Secondary connection failed")
        )
        mock_database = AsyncMock()
        mock_database.list_collection_names = AsyncMock(return_value=[])
        mock_database.create_collection = AsyncMock()
        mock_primary.__getitem__ = MagicMock(return_value=mock_database)

        def side_effect(uri):
            if "primary" in uri:
                return mock_primary
            return mock_secondary

        mock_motor_client.side_effect = side_effect

        await client.connect()

        assert client.primary_client == mock_primary
        assert client.secondary_client is None

    async def test_connect_no_databases_available(self):
        """Test connection failure when no database URIs are configured."""
        with patch.dict("os.environ", {}, clear=True):
            client = MongoDBClient()

        with pytest.raises(ConnectionError, match="No MongoDB databases available"):
            await client.connect()

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_connect_both_fail_raises_error(self, mock_motor_client, client_with_secondary):
        """Test exception when both connections fail."""
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(side_effect=ConnectionFailure("Connection failed"))
        mock_motor_client.return_value = mock_client

        with pytest.raises(ConnectionError, match="No MongoDB databases available"):
            await client_with_secondary.connect()

    async def test_disconnect(self, client):
        """Test disconnect closes clients."""
        mock_primary = MagicMock()
        mock_secondary = MagicMock()
        client.primary_client = mock_primary
        client.secondary_client = mock_secondary

        await client.disconnect()

        mock_primary.close.assert_called_once()
        mock_secondary.close.assert_called_once()

    async def test_disconnect_partial_clients(self, client):
        """Test disconnect when only primary client is available."""
        mock_primary = MagicMock()
        client.primary_client = mock_primary
        client.secondary_client = None

        await client.disconnect()

        mock_primary.close.assert_called_once()

    async def test_write_telemetry_data_success(self, client):
        """Test successful write to database."""
        mock_client = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        mock_database = MagicMock()
        mock_database.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        client.primary_client = mock_client
        client.primary_setup_complete = True

        result = await client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is True
        assert result["primary_success"] is True
        assert result["document_id"] == "test_id_123"

    async def test_write_telemetry_data_document_has_datetime_created_at(self, client):
        """Test that written document has native datetime created_at for time series."""
        mock_client = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id"

        mock_database = MagicMock()
        mock_database.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        client.primary_client = mock_client
        client.primary_setup_complete = True

        await client.write_telemetry_data(
            data={"test": "data"}, data_type="metrics", request_id="test-123"
        )

        # Verify the document has created_at as native datetime (required for time series)
        call_args = mock_collection.insert_one.call_args[0][0]
        assert "created_at" in call_args
        assert isinstance(call_args["created_at"], datetime)

    async def test_write_telemetry_data_both_databases(self, client_with_secondary):
        """Test write to both primary and secondary databases."""
        mock_client = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        mock_database = MagicMock()
        mock_database.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        client_with_secondary.primary_client = mock_client
        client_with_secondary.secondary_client = mock_client
        client_with_secondary.primary_setup_complete = True
        client_with_secondary.secondary_setup_complete = True

        result = await client_with_secondary.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is True
        assert result["primary_success"] is True
        assert result["secondary_success"] is True

    async def test_write_telemetry_data_primary_fail_secondary_success(self, client_with_secondary):
        """Test fallback to secondary when primary write fails."""
        mock_primary = AsyncMock()
        mock_secondary = AsyncMock()
        mock_primary_collection = AsyncMock()
        mock_secondary_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "secondary_id_456"

        mock_primary_db = MagicMock()
        mock_primary_db.__getitem__ = MagicMock(return_value=mock_primary_collection)
        mock_primary.__getitem__ = MagicMock(return_value=mock_primary_db)
        mock_primary_collection.insert_one = AsyncMock(
            side_effect=Exception("Primary write failed")
        )

        mock_secondary_db = MagicMock()
        mock_secondary_db.__getitem__ = MagicMock(return_value=mock_secondary_collection)
        mock_secondary.__getitem__ = MagicMock(return_value=mock_secondary_db)
        mock_secondary_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        client_with_secondary.primary_client = mock_primary
        client_with_secondary.secondary_client = mock_secondary
        client_with_secondary.primary_setup_complete = True
        client_with_secondary.secondary_setup_complete = True

        result = await client_with_secondary.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is True
        assert result["primary_success"] is False
        assert result["secondary_success"] is True
        assert result["document_id"] == "secondary_id_456"

    async def test_write_telemetry_data_both_fail(self, client_with_secondary):
        """Test handling when both writes fail."""
        mock_client = AsyncMock()
        mock_collection = AsyncMock()

        mock_database = MagicMock()
        mock_database.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_collection.insert_one = AsyncMock(side_effect=Exception("Write failed"))

        client_with_secondary.primary_client = mock_client
        client_with_secondary.secondary_client = mock_client
        client_with_secondary.primary_setup_complete = True
        client_with_secondary.secondary_setup_complete = True

        result = await client_with_secondary.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is False
        assert result["primary_success"] is False
        assert result["secondary_success"] is False

    async def test_write_telemetry_data_no_databases(self, client):
        """Test handling when no databases are connected."""
        client.primary_client = None
        client.secondary_client = None

        result = await client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is False
        assert result["error"] == "No databases available"

    async def test_write_telemetry_data_without_request_id(self, client):
        """Test telemetry write without request_id parameter."""
        mock_client = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        mock_database = MagicMock()
        mock_database.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        client.primary_client = mock_client
        client.primary_setup_complete = True

        result = await client.write_telemetry_data(
            data={"test": "data"},
            data_type="traces",
            # No request_id
        )

        assert result["success"] is True

        # Verify request_id is None in the document
        call_args = mock_collection.insert_one.call_args[0][0]
        assert call_args["request_id"] is None

    async def test_write_telemetry_data_primary_connection_lost(self, client):
        """Test write skips primary when connection validation fails."""
        mock_client = AsyncMock()
        # Ping fails - connection lost
        mock_client.admin.command = AsyncMock(side_effect=Exception("Connection lost"))

        client.primary_client = mock_client
        client.primary_setup_complete = True

        result = await client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        # Should fail since primary is the only client and connection is lost
        assert result["success"] is False
        assert result["error"] == "No databases available"

    async def test_write_telemetry_data_secondary_connection_lost(self, client_with_secondary):
        """Test write skips secondary when connection validation fails."""
        mock_primary = AsyncMock()
        mock_secondary = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "primary_id_123"

        # Primary works
        mock_primary.admin.command = AsyncMock(return_value={"ok": 1})
        mock_primary_db = MagicMock()
        mock_primary_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_primary.__getitem__ = MagicMock(return_value=mock_primary_db)
        mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        # Secondary connection lost
        mock_secondary.admin.command = AsyncMock(side_effect=Exception("Connection lost"))

        client_with_secondary.primary_client = mock_primary
        client_with_secondary.secondary_client = mock_secondary
        client_with_secondary.primary_setup_complete = True
        client_with_secondary.secondary_setup_complete = True

        result = await client_with_secondary.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        # Should succeed via primary, secondary skipped
        assert result["success"] is True
        assert result["primary_success"] is True
        assert result["secondary_success"] is None  # Skipped, not attempted

    async def test_validate_connection_failure(self, client):
        """Test _validate_connection returns False when ping fails."""
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(side_effect=Exception("Connection refused"))

        result = await client._validate_connection(mock_client, "primary")

        assert result is False

    async def test_health_check_healthy(self, client):
        """Test health check when connected."""
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(return_value={"ok": 1})
        client.primary_client = mock_client

        health = await client.health_check()

        assert health["primary"]["connected"] is True
        assert health["primary"]["error"] is None
        assert health["database"] == "test_db"
        assert health["granularity"] == "minutes"

    async def test_health_check_unhealthy(self, client):
        """Test health check when connection fails."""
        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(side_effect=ConnectionFailure("Connection lost"))
        client.primary_client = mock_client

        health = await client.health_check()

        assert health["primary"]["connected"] is False
        assert "Connection lost" in health["primary"]["error"]

    async def test_health_check_secondary_unhealthy(self, client):
        """Test health check with unhealthy secondary database."""
        mock_secondary_client = AsyncMock()
        mock_secondary_client.admin.command = AsyncMock(
            side_effect=OperationFailure("Operation failed")
        )

        client.secondary_client = mock_secondary_client

        health = await client.health_check()

        assert health["secondary"]["connected"] is False
        assert "Operation failed" in health["secondary"]["error"]

    async def test_health_check_both_databases_configured(self):
        """Test health check with both databases configured and healthy."""
        with patch.dict(
            "os.environ",
            {
                "PRIMARY_MONGODB_URI": "mongodb://primary:27017",
                "SECONDARY_MONGODB_URI": "mongodb://secondary:27017",
                "MONGODB_DATABASE": "test_db",
            },
        ):
            client = MongoDBClient()

        mock_primary_client = AsyncMock()
        mock_secondary_client = AsyncMock()

        mock_primary_client.admin.command = AsyncMock(return_value={"ok": 1})
        mock_secondary_client.admin.command = AsyncMock(return_value={"ok": 1})

        client.primary_client = mock_primary_client
        client.secondary_client = mock_secondary_client

        health = await client.health_check()

        assert health["primary"]["connected"] is True
        assert health["primary"]["configured"] is True
        assert health["secondary"]["connected"] is True
        assert health["secondary"]["configured"] is True

    async def test_create_timeseries_collection(self, client):
        """Test time series collection creation."""
        mock_database = AsyncMock()
        mock_database.create_collection = AsyncMock()

        await client._create_timeseries_collection(mock_database, "metrics", "primary")

        mock_database.create_collection.assert_called_once_with(
            "metrics",
            timeseries={
                "timeField": "created_at",
                "granularity": "minutes",
            },
        )

    async def test_create_timeseries_collection_already_exists(self, client):
        """Test handling when collection already exists."""
        mock_database = AsyncMock()
        mock_database.create_collection = AsyncMock(
            side_effect=Exception("Collection already exists")
        )

        # Should not raise
        await client._create_timeseries_collection(mock_database, "metrics", "primary")

    async def test_create_timeseries_collection_other_error(self, client):
        """Test handling when collection creation fails with non-'already exists' error."""
        mock_database = AsyncMock()
        mock_database.create_collection = AsyncMock(side_effect=Exception("Permission denied"))

        # Should not raise, but should log warning
        await client._create_timeseries_collection(mock_database, "metrics", "primary")

    async def test_ensure_database_setup(self, client):
        """Test database setup creates time series collections."""
        mock_client = AsyncMock()
        mock_database = AsyncMock()
        mock_database.list_collection_names = AsyncMock(return_value=[])
        mock_database.create_collection = AsyncMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_database)

        await client._ensure_database_setup(mock_client, "primary")

        # Should create 3 collections: traces, metrics, logs
        assert mock_database.create_collection.call_count == 3

    async def test_ensure_database_setup_failure(self, client):
        """Test database setup failure is handled gracefully."""
        mock_client = AsyncMock()
        mock_client.__getitem__ = MagicMock(side_effect=Exception("Database access failed"))

        # Should not raise exception, just log warning
        await client._ensure_database_setup(mock_client, "primary")

        # Test passes if no exception is raised

    @patch("app.mongo_client.AsyncIOMotorClient")
    async def test_database_setup_integration_with_connect(self, mock_motor_client, client):
        """Test that database setup is called during connection."""
        with patch.object(client, "_ensure_database_setup") as mock_setup:
            mock_client = AsyncMock()
            mock_client.admin.command = AsyncMock(return_value={"ok": 1})
            mock_motor_client.return_value = mock_client

            await client.connect()

            # Verify setup was called for primary database
            mock_setup.assert_called_once_with(mock_client, "primary")

    async def test_database_setup_on_write_when_not_done_during_connect(self, client):
        """Test that database setup happens on first write if not done during connection."""
        mock_client = AsyncMock()
        mock_database = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        # Setup mock hierarchy
        mock_database.__getitem__ = MagicMock(return_value=mock_collection)
        mock_database.list_collection_names = AsyncMock(return_value=[])
        mock_database.create_collection = AsyncMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        # Set client but mark setup as incomplete
        client.secondary_client = mock_client
        client.secondary_setup_complete = False

        # Write data
        result = await client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        # Verify setup was called during write
        assert client.secondary_setup_complete is True
        # Verify write succeeded
        assert result["success"] is True
        assert result["secondary_success"] is True

    async def test_primary_database_setup_on_write_when_not_done_during_connect(self, client):
        """Test that primary database setup happens on first write if not done during connection."""
        mock_client = AsyncMock()
        mock_database = AsyncMock()
        mock_collection = AsyncMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_456"

        # Setup mock hierarchy
        mock_database.__getitem__ = MagicMock(return_value=mock_collection)
        mock_database.list_collection_names = AsyncMock(return_value=[])
        mock_database.create_collection = AsyncMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_database)
        mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        # Set primary client but mark setup as incomplete
        client.primary_client = mock_client
        client.primary_setup_complete = False

        # Write data
        result = await client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-456"
        )

        # Verify setup was called during write (covers line 128)
        assert client.primary_setup_complete is True
        # Verify write succeeded
        assert result["success"] is True
        assert result["primary_success"] is True


@pytest.mark.unit
class TestMaskUriPassword:
    """Test the URI password masking helper function."""

    def test_mask_uri_password_with_password(self):
        """Test password is masked in URI."""
        from app.mongo_client import _mask_uri_password

        uri = "mongodb://user:secretpassword@localhost:27017/db"
        masked = _mask_uri_password(uri)
        assert "secretpassword" not in masked
        assert "*****" in masked
        assert "user:" in masked

    def test_mask_uri_password_empty_uri(self):
        """Test empty URI returns early."""
        from app.mongo_client import _mask_uri_password

        assert _mask_uri_password("") == ""
        assert _mask_uri_password(None) is None


@pytest.mark.unit
class TestGetMongoDBClient:
    """Test the singleton MongoDB client getter function."""

    def test_get_mongodb_client_singleton(self):
        """Test that get_mongodb_client returns the same instance."""
        import app.mongo_client

        app.mongo_client._mongodb_client = None

        client1 = get_mongodb_client()
        client2 = get_mongodb_client()

        assert client1 is client2

    def test_get_mongodb_client_creates_instance(self):
        """Test that get_mongodb_client creates an instance when none exists."""
        import app.mongo_client

        app.mongo_client._mongodb_client = None

        client = get_mongodb_client()

        assert isinstance(client, MongoDBClient)
        assert app.mongo_client._mongodb_client is client
