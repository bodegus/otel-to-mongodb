"""Unified test fixtures that generate both JSON and protobuf data from single sources.

This module consolidates test data generation to eliminate duplication between
JSON and protobuf fixtures. It uses parametrized fixtures to generate both
formats from a single data definition.
"""

import pytest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import KeyValue
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord, ResourceLogs, ScopeLogs
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    Gauge,
    Metric,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span


# Content type parameters for parametrized tests
CONTENT_TYPES = ["json", "protobuf"]


def create_key_value(
    key: str, string_value: str = None, bool_value: bool = None, int_value: int = None
) -> KeyValue:
    """Helper to create KeyValue protobuf objects."""
    kv = KeyValue()
    kv.key = key

    if string_value is not None:
        kv.value.string_value = string_value
    elif bool_value is not None:
        kv.value.bool_value = bool_value
    elif int_value is not None:
        kv.value.int_value = int_value

    return kv


def create_resource(attributes: list[tuple[str, str]]) -> Resource:
    """Helper to create Resource protobuf objects with string attributes."""
    resource = Resource()
    for key, value in attributes:
        resource.attributes.append(create_key_value(key, string_value=value))
    return resource


# Base test data definitions (format-agnostic)
SAMPLE_TRACES_DATA = {
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
}

SAMPLE_METRICS_DATA = {
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
                                            {"key": "method", "value": {"stringValue": "GET"}},
                                            {"key": "status", "value": {"stringValue": "200"}},
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
}

SAMPLE_LOGS_DATA = {
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
                                {"key": "test.phase", "value": {"stringValue": "error_simulation"}},
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
}


def json_to_protobuf_traces(json_data: dict) -> dict:
    """Convert JSON traces data to protobuf format."""
    request = ExportTraceServiceRequest()

    for resource_span_data in json_data["resourceSpans"]:
        resource_spans = ResourceSpans()

        # Convert resource
        if "attributes" in resource_span_data["resource"]:
            for attr in resource_span_data["resource"]["attributes"]:
                key = attr["key"]
                if "stringValue" in attr["value"]:
                    resource_spans.resource.attributes.append(
                        create_key_value(key, string_value=attr["value"]["stringValue"])
                    )

        # Convert scope spans
        for scope_span_data in resource_span_data["scopeSpans"]:
            scope_spans = ScopeSpans()
            scope_spans.scope.name = scope_span_data["scope"]["name"]
            if "version" in scope_span_data["scope"]:
                scope_spans.scope.version = scope_span_data["scope"]["version"]

            # Convert spans
            for span_data in scope_span_data["spans"]:
                span = Span()
                span.trace_id = bytes.fromhex(span_data["traceId"])
                span.span_id = bytes.fromhex(span_data["spanId"])
                span.name = span_data["name"]
                span.kind = span_data["kind"]
                span.start_time_unix_nano = int(span_data["startTimeUnixNano"])
                span.end_time_unix_nano = int(span_data["endTimeUnixNano"])

                if "parentSpanId" in span_data:
                    span.parent_span_id = bytes.fromhex(span_data["parentSpanId"])

                # Convert attributes
                if "attributes" in span_data:
                    for attr in span_data["attributes"]:
                        key = attr["key"]
                        if "stringValue" in attr["value"]:
                            span.attributes.append(
                                create_key_value(key, string_value=attr["value"]["stringValue"])
                            )
                        elif "boolValue" in attr["value"]:
                            span.attributes.append(
                                create_key_value(key, bool_value=attr["value"]["boolValue"])
                            )

                scope_spans.spans.append(span)

            resource_spans.scope_spans.append(scope_spans)

        request.resource_spans.append(resource_spans)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
    }


def json_to_protobuf_metrics(json_data: dict) -> dict:
    """Convert JSON metrics data to protobuf format."""
    request = ExportMetricsServiceRequest()

    for resource_metric_data in json_data["resourceMetrics"]:
        resource_metrics = ResourceMetrics()

        # Convert resource attributes
        if "attributes" in resource_metric_data["resource"]:
            for attr in resource_metric_data["resource"]["attributes"]:
                key = attr["key"]
                if "stringValue" in attr["value"]:
                    resource_metrics.resource.attributes.append(
                        create_key_value(key, string_value=attr["value"]["stringValue"])
                    )

        # Convert scope metrics
        for scope_metric_data in resource_metric_data["scopeMetrics"]:
            scope_metrics = ScopeMetrics()
            scope_metrics.scope.name = scope_metric_data["scope"]["name"]
            if "version" in scope_metric_data["scope"]:
                scope_metrics.scope.version = scope_metric_data["scope"]["version"]

            # Convert metrics
            for metric_data in scope_metric_data["metrics"]:
                metric = Metric()
                metric.name = metric_data["name"]
                if "description" in metric_data:
                    metric.description = metric_data["description"]
                if "unit" in metric_data:
                    metric.unit = metric_data["unit"]

                # Handle sum metrics
                if "sum" in metric_data:
                    sum_metric = Sum()
                    sum_data = metric_data["sum"]
                    sum_metric.aggregation_temporality = sum_data["aggregationTemporality"]
                    sum_metric.is_monotonic = sum_data["isMonotonic"]

                    for dp_data in sum_data["dataPoints"]:
                        data_point = NumberDataPoint()
                        data_point.time_unix_nano = int(dp_data["timeUnixNano"])
                        if "asInt" in dp_data:
                            data_point.as_int = int(dp_data["asInt"])
                        elif "asDouble" in dp_data:
                            data_point.as_double = float(dp_data["asDouble"])

                        # Convert attributes
                        if "attributes" in dp_data:
                            for attr in dp_data["attributes"]:
                                key = attr["key"]
                                if "stringValue" in attr["value"]:
                                    data_point.attributes.append(
                                        create_key_value(
                                            key, string_value=attr["value"]["stringValue"]
                                        )
                                    )

                        sum_metric.data_points.append(data_point)

                    metric.sum.CopyFrom(sum_metric)

                # Handle gauge metrics
                elif "gauge" in metric_data:
                    gauge_metric = Gauge()
                    gauge_data = metric_data["gauge"]

                    for dp_data in gauge_data["dataPoints"]:
                        data_point = NumberDataPoint()
                        data_point.time_unix_nano = int(dp_data["timeUnixNano"])
                        if "asInt" in dp_data:
                            data_point.as_int = int(dp_data["asInt"])
                        elif "asDouble" in dp_data:
                            data_point.as_double = float(dp_data["asDouble"])

                        gauge_metric.data_points.append(data_point)

                    metric.gauge.CopyFrom(gauge_metric)

                scope_metrics.metrics.append(metric)

            resource_metrics.scope_metrics.append(scope_metrics)

        request.resource_metrics.append(resource_metrics)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
    }


def json_to_protobuf_logs(json_data: dict) -> dict:
    """Convert JSON logs data to protobuf format."""
    request = ExportLogsServiceRequest()

    for resource_log_data in json_data["resourceLogs"]:
        resource_logs = ResourceLogs()

        # Convert resource attributes
        if "attributes" in resource_log_data["resource"]:
            for attr in resource_log_data["resource"]["attributes"]:
                key = attr["key"]
                if "stringValue" in attr["value"]:
                    resource_logs.resource.attributes.append(
                        create_key_value(key, string_value=attr["value"]["stringValue"])
                    )

        # Convert scope logs
        for scope_log_data in resource_log_data["scopeLogs"]:
            scope_logs = ScopeLogs()
            scope_logs.scope.name = scope_log_data["scope"]["name"]
            if "version" in scope_log_data["scope"]:
                scope_logs.scope.version = scope_log_data["scope"]["version"]

            # Convert log records
            for log_data in scope_log_data["logRecords"]:
                log_record = LogRecord()
                log_record.time_unix_nano = int(log_data["timeUnixNano"])
                log_record.severity_number = log_data["severityNumber"]
                if "severityText" in log_data:
                    log_record.severity_text = log_data["severityText"]
                if "body" in log_data and "stringValue" in log_data["body"]:
                    log_record.body.string_value = log_data["body"]["stringValue"]

                # Convert attributes
                if "attributes" in log_data:
                    for attr in log_data["attributes"]:
                        key = attr["key"]
                        if "stringValue" in attr["value"]:
                            log_record.attributes.append(
                                create_key_value(key, string_value=attr["value"]["stringValue"])
                            )

                scope_logs.log_records.append(log_record)

            resource_logs.scope_logs.append(scope_logs)

        request.resource_logs.append(resource_logs)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
    }


@pytest.fixture(params=CONTENT_TYPES)
def unified_traces_data(request):
    """Unified traces data fixture that provides both JSON and protobuf formats."""
    content_type = request.param

    if content_type == "json":
        return {
            "content_type": "application/json",
            "data": SAMPLE_TRACES_DATA,
            "expected_count": 2,  # 2 spans
        }
    else:  # protobuf
        protobuf_data = json_to_protobuf_traces(SAMPLE_TRACES_DATA)
        return {
            "content_type": "application/x-protobuf",
            "binary_data": protobuf_data["binary_data"],
            "data": protobuf_data["binary_data"],  # For compatibility
            "expected_count": 2,  # 2 spans
        }


@pytest.fixture(params=CONTENT_TYPES)
def unified_metrics_data(request):
    """Unified metrics data fixture that provides both JSON and protobuf formats."""
    content_type = request.param

    if content_type == "json":
        return {
            "content_type": "application/json",
            "data": SAMPLE_METRICS_DATA,
            "expected_count": 2,  # 2 metrics
        }
    else:  # protobuf
        protobuf_data = json_to_protobuf_metrics(SAMPLE_METRICS_DATA)
        return {
            "content_type": "application/x-protobuf",
            "binary_data": protobuf_data["binary_data"],
            "data": protobuf_data["binary_data"],  # For compatibility
            "expected_count": 2,  # 2 metrics
        }


@pytest.fixture(params=CONTENT_TYPES)
def unified_logs_data(request):
    """Unified logs data fixture that provides both JSON and protobuf formats."""
    content_type = request.param

    if content_type == "json":
        return {
            "content_type": "application/json",
            "data": SAMPLE_LOGS_DATA,
            "expected_count": 3,  # 3 log records
        }
    else:  # protobuf
        protobuf_data = json_to_protobuf_logs(SAMPLE_LOGS_DATA)
        return {
            "content_type": "application/x-protobuf",
            "binary_data": protobuf_data["binary_data"],
            "data": protobuf_data["binary_data"],  # For compatibility
            "expected_count": 3,  # 3 log records
        }


# Individual content-type fixtures for tests that need specific formats
@pytest.fixture
def json_traces_data():
    """JSON-only traces data."""
    return {
        "content_type": "application/json",
        "data": SAMPLE_TRACES_DATA,
        "expected_count": 2,
    }


@pytest.fixture
def protobuf_traces_data():
    """Protobuf-only traces data."""
    protobuf_data = json_to_protobuf_traces(SAMPLE_TRACES_DATA)
    return {
        "content_type": "application/x-protobuf",
        "binary_data": protobuf_data["binary_data"],
        "expected_count": 2,
    }


@pytest.fixture
def json_metrics_data():
    """JSON-only metrics data."""
    return {
        "content_type": "application/json",
        "data": SAMPLE_METRICS_DATA,
        "expected_count": 2,
    }


@pytest.fixture
def protobuf_metrics_data():
    """Protobuf-only metrics data."""
    protobuf_data = json_to_protobuf_metrics(SAMPLE_METRICS_DATA)
    return {
        "content_type": "application/x-protobuf",
        "binary_data": protobuf_data["binary_data"],
        "expected_count": 2,
    }


@pytest.fixture
def json_logs_data():
    """JSON-only logs data."""
    return {
        "content_type": "application/json",
        "data": SAMPLE_LOGS_DATA,
        "expected_count": 3,
    }


@pytest.fixture
def protobuf_logs_data():
    """Protobuf-only logs data."""
    protobuf_data = json_to_protobuf_logs(SAMPLE_LOGS_DATA)
    return {
        "content_type": "application/x-protobuf",
        "binary_data": protobuf_data["binary_data"],
        "expected_count": 3,
    }


# Edge case fixtures
@pytest.fixture
def empty_protobuf_traces_data():
    """Empty protobuf traces data for testing validation."""
    request = ExportTraceServiceRequest()
    return {
        "request": request,
        "binary_data": request.SerializeToString(),
        "expected_count": 0,
    }


@pytest.fixture
def malformed_protobuf_data():
    """Malformed protobuf data for error testing."""
    return {
        "binary_data": b"not_valid_protobuf_data",
        "expected_error": "Invalid protobuf data",
    }


# Large payload fixture
@pytest.fixture
def large_protobuf_traces_data():
    """Large protobuf traces data for performance testing."""
    request = ExportTraceServiceRequest()
    resource_spans = ResourceSpans()
    resource_spans.resource.CopyFrom(
        create_resource([("service.name", "test-service"), ("test.size", "large")])
    )

    scope_spans = ScopeSpans()
    scope_spans.scope.name = "large-test-scope"

    # Create 100 spans for performance testing
    for i in range(100):
        span = Span()
        span.trace_id = bytes.fromhex("abcdef1234567890abcdef1234567890")
        span.span_id = bytes.fromhex(f"{i:016x}")
        span.name = f"large-span-{i}"
        span.kind = 1
        span.start_time_unix_nano = 1609459200000000000 + i * 1000000
        span.end_time_unix_nano = 1609459200000000000 + (i + 1) * 1000000
        scope_spans.spans.append(span)

    resource_spans.scope_spans.append(scope_spans)
    request.resource_spans.append(resource_spans)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
        "expected_count": 100,
    }


# Legacy wrapper fixtures for backward compatibility
# These delegate to the main unified fixtures above


@pytest.fixture
def sample_protobuf_traces_data(protobuf_traces_data):
    """Legacy wrapper - use protobuf_traces_data directly."""
    return protobuf_traces_data


@pytest.fixture
def sample_protobuf_metrics_data(protobuf_metrics_data):
    """Legacy wrapper - use protobuf_metrics_data directly."""
    return protobuf_metrics_data


@pytest.fixture
def sample_protobuf_logs_data(protobuf_logs_data):
    """Legacy wrapper - use protobuf_logs_data directly."""
    return protobuf_logs_data
