"""Test configuration and unified fixtures.

This module provides unified fixtures that generate both JSON and protobuf
test data from single sources, eliminating duplication.

FIXTURE USAGE:
- Use unified_*_data fixtures for automatic JSON/protobuf testing
- Use json_*_data fixtures when you only need JSON format
- Use protobuf_*_data fixtures when you only need protobuf format
- Use mock_mongodb_client and test_app for consistent mocking
- Use otel_integration_context for container-based integration tests
"""

import asyncio
import os
import subprocess
import time
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from pymongo import MongoClient

from app.mongo_client import MongoDBClient, get_mongodb_client
from app.otel_service import OTELService

# Import unified fixtures (new approach - preferred for new tests)
from .unified_fixtures import *  # noqa: F403


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Shared Mock Fixtures
@pytest.fixture
def mock_mongodb_client():
    """Shared mock MongoDB client with consistent behavior across all tests."""
    client = MagicMock()

    # Mock the write_telemetry_data method
    client.write_telemetry_data = AsyncMock(
        return_value={
            "success": True,
            "primary_success": True,
            "secondary_success": False,
            "document_id": "test_id_123",
        }
    )

    # Mock the health_check method
    client.health_check = AsyncMock(
        return_value={
            "primary": {"connected": True, "error": None, "configured": True},
            "secondary": {"connected": False, "error": None, "configured": False},
        }
    )

    # Mock the actual database client properties that the code accesses
    client.local_client = MagicMock()
    client.local_db_name = "test_db"

    return client


@pytest.fixture
def test_app(mock_mongodb_client):
    """Shared FastAPI test app with consistent dependency overrides."""
    from app.main import create_app

    app = create_app()
    app.dependency_overrides[get_mongodb_client] = lambda: mock_mongodb_client
    return app


@pytest.fixture
def client(test_app):
    """FastAPI test client using the shared test app."""
    return TestClient(test_app)


# Integration Test Fixtures
def start_test_mongodb() -> tuple[str, str]:
    """
    Start a simple MongoDB container for testing.

    Returns:
        Tuple of (connection_uri, container_name)
    """
    container_name = f"otel-test-mongodb-{uuid.uuid4().hex[:8]}"
    test_port = 27020  # Use a different port to avoid conflicts

    # Clean up any existing containers on this port
    try:
        result = subprocess.run(  # noqa: S603
            ["docker", "ps", "-q", "--filter", f"publish={test_port}"],  # noqa: S607
            check=False,
            capture_output=True,
            text=True,
        )
        if result.stdout.strip():
            subprocess.run(  # noqa: S603
                ["docker", "stop", *result.stdout.strip().split()],  # noqa: S607
                check=False,
            )
    except Exception:
        pass

    # Start new container
    try:
        subprocess.run(  # noqa: S603
            [  # noqa: S607
                "docker",
                "run",
                "-d",
                "--name",
                container_name,
                "-p",
                f"{test_port}:27017",
                "--rm",  # Auto-remove when stopped
                "mongo:7.0",
                "--noauth",
            ],
            check=True,
            capture_output=True,
        )

        connection_uri = f"mongodb://localhost:{test_port}"

        # Wait for MongoDB to be ready
        for i in range(30):
            try:
                client = MongoClient(connection_uri, serverSelectionTimeoutMS=1000)
                client.admin.command("ping")
                client.close()
                print(f"✅ Test MongoDB ready: {connection_uri}")
                return connection_uri, container_name
            except Exception:
                if i == 29:
                    raise RuntimeError("MongoDB failed to start")
                time.sleep(1)

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to start MongoDB container: {e}")

    return connection_uri, container_name


def stop_test_mongodb(container_name: str):
    """Stop and remove the test MongoDB container."""
    try:
        subprocess.run(  # noqa: S603
            ["docker", "stop", container_name],  # noqa: S607
            check=False,
            capture_output=True,
        )
        print(f"🗑️  Stopped test MongoDB: {container_name}")
    except Exception as e:
        print(f"⚠️  Warning: Could not stop container {container_name}: {e}")


class OTELIntegrationTestContext:
    """Context manager for OTEL MongoDB integration test resources."""

    def __init__(
        self,
        client: MongoClient,
        db,
        mongo_client: MongoDBClient,
        otel_service: OTELService,
        uri: str,
    ):
        self.client = client
        self.db = db
        self.mongo_client = mongo_client
        self.otel_service = otel_service
        self.uri = uri
        self.collections_created = []

    def get_collection(self, name: str):
        """Get or create a collection and track it for cleanup."""
        collection = self.db[name]
        if name not in self.collections_created:
            self.collections_created.append(name)
        return collection

    async def verify_telemetry_data(
        self, data_type: str, expected_count: int = 1, request_id: str = None
    ) -> list:
        """Verify telemetry data was written to MongoDB."""
        collection = self.get_collection(data_type)

        # Build query - use request_id for specificity if provided
        query = {"data_type": data_type}
        if request_id:
            query["request_id"] = request_id

        documents = list(collection.find(query))
        assert len(documents) == expected_count, (
            f"Expected {expected_count} {data_type} documents with query {query}, found {len(documents)}"
        )
        return documents

    async def count_documents_by_service(self, data_type: str, service_name: str) -> int:
        """Count documents for a specific service name."""
        collection = self.get_collection(data_type)
        count = collection.count_documents(
            {
                "data_type": data_type,
                f"resource{data_type.title()[:-1]}s.resource.attributes": {
                    "$elemMatch": {"key": "service.name", "value.stringValue": service_name}
                },
            }
        )
        return count

    def cleanup_collections(self):
        """Drop all collections created during the test."""
        for collection_name in self.collections_created:
            self.db.drop_collection(collection_name)


@pytest.fixture(scope="session")
def mongodb_container():
    """Session-scoped MongoDB container for all integration tests."""
    connection_uri, container_name = start_test_mongodb()

    try:
        yield connection_uri
    finally:
        stop_test_mongodb(container_name)


@pytest.fixture
async def otel_integration_context(
    mongodb_container,
    request,
):
    """
    Function-scoped OTEL integration test context with unique database and configured services.

    Provides:
    - Isolated test database
    - Configured MongoDBClient pointing to test database
    - OTELService instance ready for testing
    - Automatic cleanup after test
    """
    connection_uri = mongodb_container

    # Create unique database name for test isolation
    test_id = str(uuid.uuid4()).replace("-", "")[:8]
    db_name = f"otel_test_{test_id}"

    # Connect to MongoDB
    client = MongoClient(connection_uri)
    db = client[db_name]

    # Configure environment for our MongoDB client
    original_env = {}
    test_env_vars = {
        "PRIMARY_MONGODB_URI": connection_uri,
        "MONGODB_DATABASE": db_name,
    }

    # Backup original env vars
    for key in test_env_vars:
        if key in os.environ:
            original_env[key] = os.environ[key]

    # Set test env vars
    for key, value in test_env_vars.items():
        os.environ[key] = value

    try:
        # Create MongoDB client and connect
        mongo_client = MongoDBClient()
        await mongo_client.connect()

        # Create OTEL service
        otel_service = OTELService(mongo_client)

        # Create test context
        context = OTELIntegrationTestContext(client, db, mongo_client, otel_service, connection_uri)

        print(f"🔧 OTEL Integration: Created database {db_name}")

        yield context

    finally:
        # Cleanup
        try:
            await mongo_client.disconnect()
        except Exception:
            pass

        # Check if --keep-db flag was provided
        keep_db = getattr(request.config, "getoption", lambda x, default=None: default)(
            "--keep-db", default=False
        )

        if keep_db:
            print(f"\n💾 Kept OTEL integration database: {db_name}")
            print(f"Connection URI: {connection_uri}/{db_name}")
        else:
            try:
                client.drop_database(db_name)
                print(f"🗑️  Cleaned up OTEL integration database: {db_name}")
            except Exception as e:
                print(f"⚠️  Warning: Could not drop OTEL integration database {db_name}: {e}")

        # Close client connection
        try:
            client.close()
        except Exception:
            pass

        # Restore original environment
        for key in test_env_vars:
            if key in original_env:
                os.environ[key] = original_env[key]
            else:
                os.environ.pop(key, None)


def pytest_addoption(parser):
    """Add command line options for integration tests."""
    parser.addoption(
        "--keep-db",
        action="store_true",
        default=False,
        help="Keep test databases after tests complete (for debugging)",
    )
    parser.addoption(
        "--integration-only",
        action="store_true",
        default=False,
        help="Run only integration tests",
    )
