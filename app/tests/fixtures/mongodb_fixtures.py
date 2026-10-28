"""
MongoDB test fixtures for OTEL integration testing.

These fixtures run against the embedded MongoDB engine (`pymongo-embedded`) rather than a
container. MongoDB's real engine runs inside the pytest process against a directory on disk,
so there is no image to pull, no port to bind, and nothing to tear down but a temporary
directory pytest already manages.

Two properties of the engine shape everything below:

- Only one engine may be open per process, synchronous or asynchronous. So the test's
  verification reads go through the same client the application opened, rather than the
  separate driver connection a container allowed.
- Opening is cheap and a directory is private to whoever holds it. So each test gets its own
  directory instead of sharing one server and namespacing by database.

Usage Examples:
    @pytest.mark.integration
    async def test_with_context(otel_integration_context):
        # Use the context for OTEL service testing
        await context.otel_service.process_traces(traces_data, request_id="test-123")
        docs = await context.verify_telemetry_data("traces", expected_count=1, request_id="test-123")
"""

import os
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from app.mongo_client import MongoDBClient
from app.otel_service import OTELService


# The scheme the embedded engine answers to. Its client hands any address it does not
# recognise to PyMongo untouched, which is what lets us substitute it for the client the
# application builds without the application knowing the difference.
EMBEDDED_URI_SCHEME = "mongodb_embedded://"

_BINDING_MISSING = (
    "pymongo-embedded is not installed, so there is no MongoDB engine to test against.\n"
    "It is a compiled extension and only a linux/arm64 wheel is vendored, so it cannot be "
    "installed on macOS.\n"
    "Run the integration tests in a linux/arm64 container instead:\n"
    "\n"
    "    ./scripts/embedded/run-tests.sh\n"
)


def get_worker_id() -> str:
    """
    Get the pytest-xdist worker ID to enable parallel testing with unique database namespacing.

    Each worker is its own process, which is also what makes parallel runs legal here: the
    one-engine-per-process rule is a per-worker rule.

    Returns:
        Worker ID string (e.g., 'gw0', 'gw1', etc.) or 'master' for non-parallel runs
    """
    return os.environ.get("PYTEST_XDIST_WORKER", "master")


def embedded_client_class() -> type:
    """
    The embedded engine's asynchronous client class.

    Skips the calling test when the binding is not installed, rather than failing it: there is
    no MongoDB to reach either way, and a skip says why.
    """
    try:
        from pymongo_embedded import AsyncMongoClient
    except ImportError:
        pytest.skip(_BINDING_MISSING)

    return AsyncMongoClient


class OTELIntegrationTestContext:
    """Context manager for OTEL MongoDB integration test resources."""

    def __init__(
        self,
        mongo_client: MongoDBClient,
        db_name: str,
        otel_service: OTELService,
        uri: str,
    ):
        self.mongo_client = mongo_client
        self.db_name = db_name
        self.otel_service = otel_service
        self.uri = uri

    @property
    def db(self):
        """
        The test database, resolved through the application's client on every access.

        Deliberately not cached: tests that exercise a reconnect replace the underlying
        client, and a handle captured once would go on pointing at the closed one.
        """
        client = self.mongo_client.primary_client
        if client is None:
            raise RuntimeError("MongoDB client is not connected")
        return client[self.db_name]

    def get_collection(self, name: str):
        """Get a collection in the test database."""
        return self.db[name]

    async def verify_telemetry_data(
        self, data_type: str, expected_count: int = 1, request_id: str | None = None
    ) -> list:
        """Verify telemetry data was written to MongoDB."""
        collection = self.get_collection(data_type)

        # Build query - use request_id for specificity if provided
        query: dict[str, Any] = {"data_type": data_type}
        if request_id:
            query["request_id"] = request_id

        documents = await collection.find(query).to_list()
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

        count = await collection.count_documents(
            {
                "data_type": data_type,
                f"{resource_key}.resource.attributes": {
                    "$elemMatch": {"key": "service.name", "value.stringValue": service_name}
                },
            }
        )
        return count


@pytest.fixture
async def otel_integration_context(
    tmp_path, monkeypatch, request
) -> AsyncGenerator[OTELIntegrationTestContext, None]:
    """
    Function-scoped OTEL integration test context backed by the embedded MongoDB engine.

    Each test opens its own engine over its own directory, so isolation needs no namespacing
    and no cleanup: the directory is discarded with pytest's tmp_path.

    Provides:
    - Isolated embedded MongoDB engine and database
    - Configured MongoDBClient pointing at that engine
    - OTELService instance ready for testing
    - Automatic cleanup after test
    """
    client_class = embedded_client_class()
    worker_id = get_worker_id()

    data_dir = tmp_path / "embedded-mongodb"
    data_dir.mkdir()
    uri = f"{EMBEDDED_URI_SCHEME}{data_dir}"

    # The database name no longer has to be unique -- the directory already is -- but it is
    # kept distinctive so anything that leaks into a log still says which test wrote it.
    test_id = str(uuid.uuid4()).replace("-", "")[:8]
    db_name = f"otel_test_{worker_id}_{test_id}"

    # Substituting the class the application instantiates is what keeps the embedded engine
    # out of application code: MongoDBClient still only builds a client from a URI, and this
    # client passes a non-embedded URI straight through to PyMongo.
    monkeypatch.setattr("app.mongo_client.AsyncMongoClient", client_class)
    monkeypatch.setenv("PRIMARY_MONGODB_URI", uri)
    monkeypatch.setenv("MONGODB_DATABASE", db_name)
    # One engine per process, so the secondary database has to stay unconfigured; the tests
    # that cover dual writes do it with mocks.
    monkeypatch.delenv("SECONDARY_MONGODB_URI", raising=False)

    mongo_client = MongoDBClient()
    await mongo_client.connect()

    otel_service = OTELService(mongo_client)
    context = OTELIntegrationTestContext(mongo_client, db_name, otel_service, uri)

    print(f"🔧 OTEL Integration [{worker_id}]: opened embedded engine at {data_dir}")

    try:
        yield context
    finally:
        # Releases the engine. Nothing else in this process may open one until it returns.
        try:
            await mongo_client.disconnect()
        except Exception as e:
            print(f"⚠️  Warning: Could not close embedded MongoDB engine: {e}")

        keep_db = request.config.getoption("--keep-db", default=False)
        if keep_db:
            print(f"\n💾 Kept embedded MongoDB directory: {data_dir}")
        else:
            print(f"🗑️  Discarding embedded MongoDB directory: {data_dir}")


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
