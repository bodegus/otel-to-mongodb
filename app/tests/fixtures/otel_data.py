"""Shared OTEL test data fixtures that can be used by both unit and integration tests."""

import pytest


@pytest.fixture
def sample_traces_data():
    """Comprehensive traces data for both unit and integration testing with multiple spans."""
    return {
        "data": {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "test-service"}},
                            {"key": "service.version", "value": {"stringValue": "1.0.0"}},
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "test-scope", "version": "1.0"},
                            "spans": [
                                {
                                    "traceId": "abcdef1234567890abcdef1234567890",
                                    "spanId": "1234567890abcdef",
                                    "name": "test-span",
                                    "kind": 1,
                                    "startTimeUnixNano": "1609459200000000000",
                                    "endTimeUnixNano": "1609459201000000000",
                                    "attributes": [
                                        {"key": "test.type", "value": {"stringValue": "traces"}},
                                        {"key": "test.comprehensive", "value": {"boolValue": True}},
                                    ],
                                },
                                {
                                    "traceId": "abcdef1234567890abcdef1234567890",
                                    "spanId": "abcdef1234567890",
                                    "name": "child-span",
                                    "kind": 2,
                                    "startTimeUnixNano": "1609459200500000000",
                                    "endTimeUnixNano": "1609459200800000000",
                                    "parentSpanId": "1234567890abcdef",
                                },
                            ],
                        }
                    ],
                }
            ]
        },
        "expected_count": 2,  # 2 spans: parent + child
    }


@pytest.fixture
def sample_metrics_data():
    """Comprehensive metrics data for both unit and integration testing with multiple metric types."""
    return {
        "data": {
            "resourceMetrics": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "test-service"}},
                            {"key": "deployment.environment", "value": {"stringValue": "test"}},
                        ]
                    },
                    "scopeMetrics": [
                        {
                            "scope": {"name": "test-metrics", "version": "1.0"},
                            "metrics": [
                                {
                                    "name": "request_count",
                                    "description": "Total number of requests",
                                    "unit": "1",
                                    "sum": {
                                        "dataPoints": [
                                            {
                                                "timeUnixNano": "1609459200000000000",
                                                "asInt": "100",
                                                "attributes": [
                                                    {
                                                        "key": "method",
                                                        "value": {"stringValue": "GET"},
                                                    },
                                                    {
                                                        "key": "status",
                                                        "value": {"stringValue": "200"},
                                                    },
                                                ],
                                            }
                                        ],
                                        "aggregationTemporality": 2,
                                        "isMonotonic": True,
                                    },
                                },
                                {
                                    "name": "response_time",
                                    "description": "Response time in milliseconds",
                                    "unit": "ms",
                                    "gauge": {
                                        "dataPoints": [
                                            {
                                                "timeUnixNano": "1609459200000000000",
                                                "asDouble": 45.7,
                                            }
                                        ]
                                    },
                                },
                            ],
                        }
                    ],
                }
            ]
        },
        "expected_count": 2,  # 2 metrics: request_count (sum) + response_time (gauge)
    }


@pytest.fixture
def sample_logs_data():
    """Comprehensive logs data for both unit and integration testing with multiple severity levels."""
    return {
        "data": {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "test-service"}},
                            {"key": "host.name", "value": {"stringValue": "test-host"}},
                        ]
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": "test-logs", "version": "1.0"},
                            "logRecords": [
                                {
                                    "timeUnixNano": "1609459200000000000",
                                    "severityNumber": 9,  # INFO
                                    "severityText": "INFO",
                                    "body": {"stringValue": "Test operation started successfully"},
                                    "attributes": [
                                        {"key": "test.phase", "value": {"stringValue": "start"}},
                                    ],
                                },
                                {
                                    "timeUnixNano": "1609459201000000000",
                                    "severityNumber": 13,  # ERROR
                                    "severityText": "ERROR",
                                    "body": {"stringValue": "Test error condition simulated"},
                                    "attributes": [
                                        {
                                            "key": "test.phase",
                                            "value": {"stringValue": "error_simulation"},
                                        },
                                    ],
                                },
                                {
                                    "timeUnixNano": "1609459202000000000",
                                    "severityNumber": 5,  # DEBUG
                                    "severityText": "DEBUG",
                                    "body": {"stringValue": "Debug information for test"},
                                    "attributes": [
                                        {"key": "test.phase", "value": {"stringValue": "debug"}},
                                    ],
                                },
                            ],
                        }
                    ],
                }
            ]
        },
        "expected_count": 3,  # 3 log records: INFO + ERROR + DEBUG
    }


# Legacy aliases for backward compatibility (can be removed later)
@pytest.fixture
def integration_traces_data(sample_traces_data):
    """Alias for sample_traces_data for backward compatibility."""
    return sample_traces_data


@pytest.fixture
def integration_metrics_data(sample_metrics_data):
    """Alias for sample_metrics_data for backward compatibility."""
    return sample_metrics_data


@pytest.fixture
def integration_logs_data(sample_logs_data):
    """Alias for sample_logs_data for backward compatibility."""
    return sample_logs_data


# Test data for counting functions (extracted from unit tests)
@pytest.fixture
def multi_span_traces_data():
    """Traces data with multiple spans for counting tests."""
    return {
        "data": {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "multi-span-service"}}
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "test-scope", "version": "1.0"},
                            "spans": [
                                {
                                    "traceId": "abcdef1234567890abcdef1234567890",
                                    "spanId": "1234567890abcdef",
                                    "name": "span1",
                                    "kind": 1,
                                    "startTimeUnixNano": "1609459200000000000",
                                    "endTimeUnixNano": "1609459201000000000",
                                },
                                {
                                    "traceId": "abcdef1234567890abcdef1234567890",
                                    "spanId": "abcdef1234567890",
                                    "name": "span2",
                                    "kind": 2,
                                    "startTimeUnixNano": "1609459200500000000",
                                    "endTimeUnixNano": "1609459200800000000",
                                },
                            ],
                        },
                        {
                            "scope": {"name": "test-scope-2", "version": "1.0"},
                            "spans": [
                                {
                                    "traceId": "abcdef1234567890abcdef1234567890",
                                    "spanId": "fedcba0987654321",
                                    "name": "span3",
                                    "kind": 1,
                                    "startTimeUnixNano": "1609459201000000000",
                                    "endTimeUnixNano": "1609459202000000000",
                                }
                            ],
                        },
                    ],
                }
            ]
        },
        "expected_count": 3,  # 3 spans total
    }


@pytest.fixture
def multi_metrics_data():
    """Metrics data with multiple metrics for counting tests."""
    return {
        "data": {
            "resourceMetrics": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {"stringValue": "multi-metrics-service"},
                            }
                        ]
                    },
                    "scopeMetrics": [
                        {
                            "scope": {"name": "test-metrics-scope", "version": "1.0"},
                            "metrics": [
                                {
                                    "name": "metric1",
                                    "description": "Test metric 1",
                                    "unit": "count",
                                    "sum": {
                                        "dataPoints": [
                                            {
                                                "startTimeUnixNano": "1609459200000000000",
                                                "timeUnixNano": "1609459201000000000",
                                                "asInt": "42",
                                            }
                                        ],
                                        "aggregationTemporality": 2,
                                        "isMonotonic": True,
                                    },
                                },
                                {
                                    "name": "metric2",
                                    "description": "Test metric 2",
                                    "unit": "ms",
                                    "gauge": {
                                        "dataPoints": [
                                            {
                                                "timeUnixNano": "1609459201000000000",
                                                "asDouble": 123.45,
                                            }
                                        ]
                                    },
                                },
                            ],
                        }
                    ],
                }
            ]
        },
        "expected_count": 2,  # 2 metrics total
    }


@pytest.fixture
def multi_logs_data():
    """Logs data with multiple log records for counting tests."""
    return {
        "data": {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "multi-logs-service"}}
                        ]
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": "test-logs-scope", "version": "1.0"},
                            "logRecords": [
                                {
                                    "timeUnixNano": "1609459200000000000",
                                    "severityNumber": 9,  # INFO
                                    "severityText": "INFO",
                                    "body": {"stringValue": "log1"},
                                },
                                {
                                    "timeUnixNano": "1609459201000000000",
                                    "severityNumber": 17,  # ERROR
                                    "severityText": "ERROR",
                                    "body": {"stringValue": "log2"},
                                },
                            ],
                        }
                    ],
                }
            ]
        },
        "expected_count": 2,  # 2 log records total
    }
