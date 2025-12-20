"""Test configuration and unified fixtures.

This module provides unified fixtures that generate both JSON and protobuf
test data from single sources, eliminating duplication.

FIXTURE USAGE:
- Use unified_*_data fixtures for automatic JSON/protobuf testing
- Use json_*_data fixtures when you only need JSON format
- Use protobuf_*_data fixtures when you only need protobuf format
- Use mock_mongodb_client and test_app for consistent mocking
- Use otel_integration_context for container-based integration tests (uses time series collections)
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

# Import MongoDB integration fixtures
from .fixtures.mongodb_fixtures import (
    OTELIntegrationTestContext,
    mongodb_container,
    otel_integration_context,
)

# Import unified fixtures (new approach - preferred for new tests)
from .unified_fixtures import *  # noqa: F403


# Re-export for pytest discovery
__all__ = [
    "OTELIntegrationTestContext",
    "mongodb_container",
    "otel_integration_context",
]


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
    app.state.mongodb_client = mock_mongodb_client
    return app


@pytest.fixture
def client(test_app):
    """FastAPI test client using the shared test app."""
    return TestClient(test_app)
