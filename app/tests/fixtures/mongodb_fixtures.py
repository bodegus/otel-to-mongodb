"""
MongoDB test fixtures for OTEL integration testing.

This module provides standardized MongoDB fixtures using containers
for consistent testing with time series collections.

Usage Examples:
    @pytest.mark.integration
    def test_with_context(otel_integration_context):
        # Use the context for OTEL service testing
        await context.otel_service.process_traces(traces_data, request_id="test-123")
        docs = await context.verify_telemetry_data("traces", expected_count=1, request_id="test-123")
"""

# ruff: noqa: S603, S607
# S603: subprocess call - these are controlled test fixtures, not production code
# S607: partial executable path - "docker" is intentionally used without full path

import os
import subprocess
import time
import uuid
from collections.abc import Generator
from typing import Any

import pytest
from pymongo import MongoClient
from pymongo.database import Database

from app.mongo_client import MongoDBClient
from app.otel_service import OTELService


def get_worker_id() -> str:
    """
    Get the pytest-xdist worker ID to enable parallel testing with unique database namespacing.

    Returns:
        Worker ID string (e.g., 'gw0', 'gw1', etc.) or 'master' for non-parallel runs
    """
    return os.environ.get("PYTEST_XDIST_WORKER", "master")


def start_test_mongodb_container() -> tuple[str, str]:
    """
    Start a fresh MongoDB test container on port 27020.

    This function stops any existing container first to ensure a clean state,
    then creates a new container for the test session.

    Returns:
        Tuple of (connection_uri, container_name)
    """
    container_name = "otel-test-mongodb"
    test_port = 27020
    mongo_image = "mongo:7.0"
    connection_uri = f"mongodb://localhost:{test_port}"

    # Always stop and remove any existing container first for clean state
    print("🧹 Cleaning up any existing test container...")
    subprocess.run(
        ["docker", "stop", container_name],
        capture_output=True,
        check=False,
    )
    subprocess.run(
        ["docker", "rm", container_name],
        capture_output=True,
        check=False,
    )

    # Clean up any other containers that might be using our port
    port_containers = subprocess.run(
        [
            "docker",
            "ps",
            "-a",
            "--format",
            "{{.Names}}",
            "--filter",
            f"publish={test_port}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    for old_container in port_containers.stdout.strip().split("\n"):
        if old_container:
            print(f"🧹 Cleaning up container {old_container} using port {test_port}")
            subprocess.run(["docker", "stop", old_container], capture_output=True, check=False)
            subprocess.run(["docker", "rm", old_container], capture_output=True, check=False)

    # Start fresh MongoDB container
    print(f"🚀 Starting fresh MongoDB test container ({mongo_image})...")

    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "-p",
            f"{test_port}:27017",
            mongo_image,
            "--noauth",
        ],
        check=True,
    )

    # Wait for MongoDB to be ready
    print("⏳ Waiting for MongoDB to be ready...")
    max_retries = 30
    for i in range(max_retries):
        try:
            client = MongoClient(connection_uri, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            client.close()
            print(f"✅ MongoDB test container ready on port {test_port}")
            return connection_uri, container_name
        except Exception as e:
            if i == max_retries - 1:
                # Show container logs before failing
                print("📋 MongoDB container logs:")
                try:
                    logs_result = subprocess.run(
                        ["docker", "logs", "--tail", "50", container_name],
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    print(logs_result.stdout)
                    print(logs_result.stderr)
                except Exception:
                    pass
                raise RuntimeError(f"MongoDB failed to start after {max_retries} retries: {e}")

            if i % 10 == 0 and i > 0:
                print(f"⏳ Still waiting for MongoDB... (attempt {i + 1}/{max_retries})")
            time.sleep(1)

    return connection_uri, container_name


def stop_test_mongodb_container(container_name: str) -> None:
    """
    Stop and remove the MongoDB test container.

    Args:
        container_name: Name of the container to stop
    """
    print(f"🗑️  Stopping MongoDB test container: {container_name}")
    try:
        subprocess.run(
            ["docker", "stop", container_name],
            capture_output=True,
            check=False,
        )
        subprocess.run(
            ["docker", "rm", container_name],
            capture_output=True,
            check=False,
        )
        print("✅ MongoDB test container stopped and removed")
    except Exception as e:
        print(f"⚠️  Warning: Could not stop container {container_name}: {e}")


class OTELIntegrationTestContext:
    """Context manager for OTEL MongoDB integration test resources."""

    def __init__(
        self,
        client: MongoClient,
        db: Database,
        mongo_client: MongoDBClient,
        otel_service: OTELService,
        uri: str,
    ):
        self.client = client
        self.db = db
        self.mongo_client = mongo_client
        self.otel_service = otel_service
        self.uri = uri
        self.collections_created: list[str] = []

    def get_collection(self, name: str):
        """Get or create a collection and track it for cleanup."""
        collection = self.db[name]
        if name not in self.collections_created:
            self.collections_created.append(name)
        return collection

    async def verify_telemetry_data(
        self, data_type: str, expected_count: int = 1, request_id: str | None = None
    ) -> list:
        """Verify telemetry data was written to MongoDB."""
        collection = self.get_collection(data_type)

        # Build query - use request_id for specificity if provided
        query: dict[str, Any] = {"data_type": data_type}
        if request_id:
            query["request_id"] = request_id

        documents = list(collection.find(query))
        assert len(documents) == expected_count, (
            f"Expected {expected_count} {data_type} documents with query {query}, "
            f"found {len(documents)}"
        )
        return documents

    async def count_documents_by_service(self, data_type: str, service_name: str) -> int:
        """Count documents for a specific service name."""
        collection = self.get_collection(data_type)

        # Build the resource key based on data type
        if data_type == "traces":
            resource_key = "resourceSpans"
        elif data_type == "metrics":
            resource_key = "resourceMetrics"
        else:
            resource_key = "resourceLogs"

        count = collection.count_documents(
            {
                "data_type": data_type,
                f"{resource_key}.resource.attributes": {
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
def mongodb_container() -> Generator[tuple[str, MongoClient], None, None]:
    """
    Session-scoped MongoDB container fixture.

    Creates a fresh MongoDB test container at the start of the test session
    and destroys it at the end. Each test gets a unique database for isolation.

    Yields:
        Tuple of (connection_uri, client)
    """
    worker_id = get_worker_id()
    print(f"🔧 MongoDB [{worker_id}]: Starting fresh test container")

    # Start fresh container (stops any existing one first)
    connection_uri, container_name = start_test_mongodb_container()

    # Create client connection
    client = MongoClient(connection_uri)

    # Verify connection
    try:
        client.admin.command("ping")
        print(f"✅ MongoDB [{worker_id}]: Connected to test container at {connection_uri}")
    except Exception as e:
        client.close()
        stop_test_mongodb_container(container_name)
        raise RuntimeError(f"Failed to connect to MongoDB test container: {e}")

    try:
        yield connection_uri, client
    finally:
        # Close client connection
        try:
            client.close()
        except Exception:
            pass
        # Stop and remove the container
        stop_test_mongodb_container(container_name)


@pytest.fixture
async def otel_integration_context(
    mongodb_container, request
) -> Generator[OTELIntegrationTestContext, None, None]:
    """
    Function-scoped OTEL integration test context with worker-aware database namespacing.

    Creates a unique test database for each test with worker ID namespacing to avoid
    collisions in parallel testing. Each database is automatically cleaned up after the test.

    Provides:
    - Isolated test database with time series collections
    - Configured MongoDBClient pointing to test database
    - OTELService instance ready for testing
    - Automatic cleanup after test
    """
    connection_uri, sync_client = mongodb_container
    worker_id = get_worker_id()

    # Create unique database name with worker namespace and test ID
    test_id = str(uuid.uuid4()).replace("-", "")[:8]
    db_name = f"otel_test_{worker_id}_{test_id}"

    # Get database reference
    db = sync_client[db_name]

    # Configure environment for our MongoDB client
    original_env: dict[str, str] = {}
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
        context = OTELIntegrationTestContext(
            sync_client, db, mongo_client, otel_service, connection_uri
        )

        print(f"🔧 OTEL Integration [{worker_id}]: Created database {db_name}")

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
                sync_client.drop_database(db_name)
                print(f"🗑️  Cleaned up OTEL integration database: {db_name}")
            except Exception as e:
                print(f"⚠️  Warning: Could not drop OTEL integration database {db_name}: {e}")

        # Restore original environment
        for key in test_env_vars:
            if key in original_env:
                os.environ[key] = original_env[key]
            else:
                os.environ.pop(key, None)


def pytest_addoption(parser):
    """Add command line options for integration tests."""
    try:
        parser.addoption(
            "--keep-db",
            action="store_true",
            default=False,
            help="Keep test databases after tests complete (for debugging)",
        )
    except ValueError:
        # Option already added
        pass
