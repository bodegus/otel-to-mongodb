"""Configuration for OTEL integration tests."""

import pytest  # noqa: F401

# Import integration fixtures to make them available to pytest
from .fixtures import mongodb_container, otel_integration_context  # noqa: F401


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
