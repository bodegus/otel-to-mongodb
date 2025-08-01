"""Test configuration and unified fixtures.

This module provides unified fixtures that generate both JSON and protobuf
test data from single sources, eliminating duplication.

FIXTURE USAGE:
- Use unified_*_data fixtures for automatic JSON/protobuf testing
- Use json_*_data fixtures when you only need JSON format
- Use protobuf_*_data fixtures when you only need protobuf format
- Legacy fixtures are maintained for specific edge cases
"""

import asyncio

import pytest

# Import unified fixtures (new approach - preferred for new tests)
from .unified_fixtures import *  # noqa: F403


# All fixtures now provided by unified_fixtures.py
# Legacy fixture files kept for reference but no longer imported


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
