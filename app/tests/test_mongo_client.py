"""Test MongoDB client functionality with comprehensive edge case coverage."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pymongo.errors import ConnectionFailure, OperationFailure

from app.mongo_client import MongoDBClient, get_mongodb_client


# Pytest fixtures for mock setup
@pytest.fixture
def mock_successful_client():
    """Create a mock client configured for successful database operations."""
    mock_client = AsyncMock()
    mock_database = MagicMock()
    mock_collection = MagicMock()

    mock_insert_result = MagicMock()
    mock_insert_result.inserted_id = "test_id_123"

    mock_database.__getitem__.return_value = mock_collection
    mock_client.get_database = MagicMock(return_value=mock_database)
    mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

    return mock_client, mock_collection


@pytest.fixture
def mock_failing_client():
    """Create a mock client configured to fail database operations."""
    mock_client = AsyncMock()
    mock_database = MagicMock()
    mock_collection = MagicMock()

    mock_database.__getitem__.return_value = mock_collection
    mock_client.get_database = MagicMock(return_value=mock_database)
    mock_collection.insert_one = AsyncMock(side_effect=Exception("Write failed"))

    return mock_client, mock_collection


@pytest.fixture
def mock_client_factory():
    """Factory fixture to create mock clients with custom configurations."""

    def _create_mock_client(insert_result_id="test_id_123", should_fail=False):
        mock_client = AsyncMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()

        mock_database.__getitem__.return_value = mock_collection
        mock_client.get_database = MagicMock(return_value=mock_database)

        if should_fail:
            mock_collection.insert_one = AsyncMock(side_effect=Exception("Write failed"))
        else:
            mock_insert_result = MagicMock()
            mock_insert_result.inserted_id = insert_result_id
            mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)

        return mock_client, mock_collection

    return _create_mock_client


@pytest.fixture
def mock_successful_write_result():
    """Standard mock result for successful database writes."""
    mock_result = MagicMock()
    mock_result.inserted_id = "test_id_123"
    return mock_result


@pytest.fixture
def setup_mongo_client_mocks():
    """Fixture to setup common mocks for MongoDBClient instance."""

    def _setup_mocks(mongo_client, primary_client=None, secondary_client=None):
        mongo_client._validate_connection = AsyncMock(return_value=True)
        mongo_client._ensure_database_setup_on_write = AsyncMock()

        mongo_client.primary_client = primary_client
        mongo_client.secondary_client = secondary_client

        return mongo_client

    return _setup_mocks


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

    async def test_write_telemetry_data_success(
        self, mongo_client, mock_successful_client, setup_mongo_client_mocks
    ):
        """Test successful telemetry data write to primary database."""
        mock_primary_client, mock_collection = mock_successful_client
        setup_mongo_client_mocks(mongo_client, primary_client=mock_primary_client)

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        # Verify results
        assert result["success"] is True
        assert result["primary_success"] is True
        assert result["secondary_success"] is None
        assert result["document_id"] == "test_id_123"

        # Verify insert was called with correct data
        mock_collection.insert_one.assert_called_once()
        call_args = mock_collection.insert_one.call_args[0][0]
        assert call_args["test"] == "data"
        assert call_args["data_type"] == "traces"
        assert call_args["request_id"] == "test-123"

    async def test_write_telemetry_data_both_databases(
        self, mongo_client, mock_client_factory, setup_mongo_client_mocks
    ):
        """Test telemetry data write to both primary and secondary databases."""
        mock_primary_client, mock_primary_collection = mock_client_factory("test_id_123")
        mock_secondary_client, mock_secondary_collection = mock_client_factory("test_id_123")
        setup_mongo_client_mocks(
            mongo_client, primary_client=mock_primary_client, secondary_client=mock_secondary_client
        )

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is True
        assert result["primary_success"] is True
        assert result["secondary_success"] is True

        # Verify both inserts were called
        mock_primary_collection.insert_one.assert_called_once()
        mock_secondary_collection.insert_one.assert_called_once()

    async def test_write_telemetry_data_primary_fail_secondary_success(
        self, mongo_client, mock_client_factory, setup_mongo_client_mocks
    ):
        """Test telemetry write when primary fails but secondary succeeds."""
        mock_primary_client, _ = mock_client_factory(should_fail=True)
        mock_secondary_client, _ = mock_client_factory("secondary_id_456")
        setup_mongo_client_mocks(
            mongo_client, primary_client=mock_primary_client, secondary_client=mock_secondary_client
        )

        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        assert result["success"] is True  # Overall success because secondary succeeded
        assert result["primary_success"] is False
        assert result["secondary_success"] is True
        assert result["document_id"] == "secondary_id_456"  # Uses secondary ID
        assert len(result["errors"]) == 1
        assert "Write failed" in result["errors"][0]

    async def test_write_telemetry_data_both_fail(
        self, mongo_client, mock_client_factory, setup_mongo_client_mocks
    ):
        """Test telemetry write when both databases fail."""
        mock_primary_client, _ = mock_client_factory(should_fail=True)
        mock_secondary_client, _ = mock_client_factory(should_fail=True)
        setup_mongo_client_mocks(
            mongo_client, primary_client=mock_primary_client, secondary_client=mock_secondary_client
        )

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

    async def test_write_telemetry_data_without_request_id(
        self, mongo_client, mock_successful_client, setup_mongo_client_mocks
    ):
        """Test telemetry write without request_id parameter."""
        mock_primary_client, mock_collection = mock_successful_client
        setup_mongo_client_mocks(mongo_client, primary_client=mock_primary_client)

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

    @pytest.mark.parametrize(
        "db_type,error_class,error_message",
        [
            ("primary", "ConnectionFailure", "Connection lost"),
            ("secondary", "OperationFailure", "Operation failed"),
        ],
    )
    async def test_health_check_database_unhealthy(
        self, mongo_client, db_type, error_class, error_message
    ):
        """Test health check with unhealthy database (parametrized for primary/secondary)."""
        from pymongo.errors import ConnectionFailure, OperationFailure

        error_classes = {
            "ConnectionFailure": ConnectionFailure,
            "OperationFailure": OperationFailure,
        }
        exception = error_classes[error_class](error_message)

        mock_client = AsyncMock()
        mock_client.admin.command = AsyncMock(side_effect=exception)
        setattr(mongo_client, f"{db_type}_client", mock_client)

        health = await mongo_client.health_check()

        assert health[db_type]["connected"] is False
        assert error_message in health[db_type]["error"]

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

    async def test_ensure_database_setup_success(self, mongo_client):
        """Test successful database setup with collections and indexes."""
        mock_client = AsyncMock()
        mock_database = AsyncMock()
        mock_collection = MagicMock()

        # Setup mock hierarchy: client[db_name][collection_name]
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.create_index = AsyncMock()

        await mongo_client._ensure_database_setup(mock_client, "primary")

        # Verify database access
        mock_client.__getitem__.assert_called_with("test_db")

        # Verify all OTEL collections were accessed
        expected_collections = ["traces", "metrics", "logs"]
        assert mock_database.__getitem__.call_count == len(expected_collections)

        # Verify create_index was called for each collection
        assert mock_collection.create_index.call_count == len(expected_collections)

    async def test_ensure_database_setup_failure(self, mongo_client):
        """Test database setup failure is handled gracefully."""
        mock_client = AsyncMock()
        mock_client.__getitem__.side_effect = Exception("Database access failed")

        # Should not raise exception, just log warning
        await mongo_client._ensure_database_setup(mock_client, "primary")

        # Test passes if no exception is raised

    @pytest.mark.parametrize(
        "collection_name,db_type,should_fail",
        [
            ("traces", "primary", False),
            ("metrics", "secondary", True),
        ],
    )
    async def test_ensure_indexes(self, mongo_client, collection_name, db_type, should_fail):
        """Test index creation (parametrized for success/failure scenarios)."""
        mock_collection = MagicMock()

        if should_fail:
            mock_collection.create_index = AsyncMock(side_effect=Exception("Index creation failed"))
            # Should not raise exception, just log warning
            await mongo_client._ensure_indexes(mock_collection, collection_name, db_type)
        else:
            mock_collection.create_index = AsyncMock()
            await mongo_client._ensure_indexes(mock_collection, collection_name, db_type)
            mock_collection.create_index.assert_called_once_with(
                "created_at", background=True, name=f"{collection_name}_created_at_idx"
            )

    async def test_database_setup_integration_with_connect(self, mongo_client):
        """Test that database setup is called during connection."""
        with patch.object(mongo_client, "_ensure_database_setup") as mock_setup:
            with patch("app.mongo_client.AsyncIOMotorClient") as mock_motor_client:
                mock_client = AsyncMock()
                mock_client.admin.command = AsyncMock(return_value={"ok": 1})
                mock_motor_client.return_value = mock_client

                await mongo_client.connect()

                # Verify setup was called for primary database
                mock_setup.assert_called_once_with(mock_client, "primary")

    async def test_database_setup_on_write_when_not_done_during_connect(self, mongo_client):
        """Test that database setup happens on first write if not done during connection."""
        # Simulate a client that connected but didn't complete setup
        mock_client = AsyncMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "test_id_123"

        # Setup mock hierarchy using get_database approach
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.create_index = AsyncMock()
        mock_collection.insert_one = AsyncMock(return_value=mock_insert_result)
        mock_client.get_database = MagicMock(return_value=mock_database)

        # Mock _validate_connection to return True
        mongo_client._validate_connection = AsyncMock(return_value=True)

        # Set client but mark setup as incomplete
        mongo_client.secondary_client = mock_client
        mongo_client.secondary_setup_complete = False

        # Write data
        result = await mongo_client.write_telemetry_data(
            data={"test": "data"}, data_type="traces", request_id="test-123"
        )

        # Verify setup was called during write
        assert mongo_client.secondary_setup_complete is True
        # Verify write succeeded
        assert result["success"] is True
        assert result["secondary_success"] is True


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
