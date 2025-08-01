"""
MongoDB integration fixtures for OTEL service testing.

Simple, standardized approach inspired by the reference implementation.
"""

import os
import subprocess
import time
import uuid
from collections.abc import Generator

import pytest
from pymongo import MongoClient

from app.mongo_client import MongoDBClient
from app.otel_service import OTELService


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
        result = subprocess.run(
            ["docker", "ps", "-q", "--filter", f"publish={test_port}"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.stdout.strip():
            subprocess.run(["docker", "stop"] + result.stdout.strip().split(), check=False)
    except Exception:
        pass

    # Start new container
    try:
        subprocess.run(
            [
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
        subprocess.run(["docker", "stop", container_name], check=False, capture_output=True)
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
) -> Generator[OTELIntegrationTestContext, None, None]:
    # TODO: evaluate if this is still needed ot can be simplified
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
