"""Simple test configuration and fixtures."""

import pytest
import asyncio
from typing import Dict, Any


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_trace_data() -> Dict[str, Any]:
    """Sample OpenTelemetry trace data."""
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": []},
                "scopeSpans": [
                    {
                        "scope": {"name": "test-scope"},
                        "spans": [
                            {
                                "traceId": "0123456789abcdef0123456789abcdef",
                                "spanId": "0123456789abcdef",
                                "name": "test-span",
                                "kind": 1,
                                "startTimeUnixNano": "1640995200000000000",
                                "endTimeUnixNano": "1640995201000000000",
                            }
                        ],
                    }
                ],
            }
        ]
    }