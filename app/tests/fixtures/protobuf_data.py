"""Protobuf OTEL test data fixtures that mirror the JSON test data."""

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


@pytest.fixture
def sample_protobuf_traces_data():
    """Protobuf equivalent of sample_traces_data fixture."""
    request = ExportTraceServiceRequest()

    # Create resource spans
    resource_spans = ResourceSpans()
    resource_spans.resource.CopyFrom(
        create_resource([("service.name", "test-service"), ("service.version", "1.0.0")])
    )

    # Create scope spans
    scope_spans = ScopeSpans()
    scope_spans.scope.name = "test-scope"
    scope_spans.scope.version = "1.0"

    # Create first span
    span1 = Span()
    span1.trace_id = bytes.fromhex("abcdef1234567890abcdef1234567890")
    span1.span_id = bytes.fromhex("1234567890abcdef")
    span1.name = "test-span"
    span1.kind = 1
    span1.start_time_unix_nano = 1609459200000000000
    span1.end_time_unix_nano = 1609459201000000000
    span1.attributes.append(create_key_value("test.type", string_value="traces"))
    span1.attributes.append(create_key_value("test.comprehensive", bool_value=True))

    # Create second span (child)
    span2 = Span()
    span2.trace_id = bytes.fromhex("abcdef1234567890abcdef1234567890")
    span2.span_id = bytes.fromhex("abcdef1234567890")
    span2.name = "child-span"
    span2.kind = 2
    span2.start_time_unix_nano = 1609459200500000000
    span2.end_time_unix_nano = 1609459200800000000
    span2.parent_span_id = bytes.fromhex("1234567890abcdef")

    scope_spans.spans.extend([span1, span2])
    resource_spans.scope_spans.append(scope_spans)
    request.resource_spans.append(resource_spans)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
        "expected_count": 2,
    }


@pytest.fixture
def sample_protobuf_metrics_data():
    """Protobuf equivalent of sample_metrics_data fixture."""
    request = ExportMetricsServiceRequest()

    # Create resource metrics
    resource_metrics = ResourceMetrics()
    resource_metrics.resource.CopyFrom(
        create_resource([("service.name", "test-service"), ("deployment.environment", "test")])
    )

    # Create scope metrics
    scope_metrics = ScopeMetrics()
    scope_metrics.scope.name = "test-metrics"
    scope_metrics.scope.version = "1.0"

    # Create first metric (sum)
    metric1 = Metric()
    metric1.name = "request_count"
    metric1.description = "Total number of requests"
    metric1.unit = "1"

    # Create sum metric data
    sum_metric = Sum()
    sum_metric.aggregation_temporality = 2
    sum_metric.is_monotonic = True

    data_point1 = NumberDataPoint()
    data_point1.time_unix_nano = 1609459200000000000
    data_point1.as_int = 100
    data_point1.attributes.append(create_key_value("method", string_value="GET"))
    data_point1.attributes.append(create_key_value("status", string_value="200"))

    sum_metric.data_points.append(data_point1)
    metric1.sum.CopyFrom(sum_metric)

    # Create second metric (gauge)
    metric2 = Metric()
    metric2.name = "response_time"
    metric2.description = "Response time in milliseconds"
    metric2.unit = "ms"

    # Create gauge metric data
    gauge_metric = Gauge()
    data_point2 = NumberDataPoint()
    data_point2.time_unix_nano = 1609459200000000000
    data_point2.as_double = 45.7

    gauge_metric.data_points.append(data_point2)
    metric2.gauge.CopyFrom(gauge_metric)

    scope_metrics.metrics.extend([metric1, metric2])
    resource_metrics.scope_metrics.append(scope_metrics)
    request.resource_metrics.append(resource_metrics)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
        "expected_count": 2,
    }


@pytest.fixture
def sample_protobuf_logs_data():
    """Protobuf equivalent of sample_logs_data fixture."""
    request = ExportLogsServiceRequest()

    # Create resource logs
    resource_logs = ResourceLogs()
    resource_logs.resource.CopyFrom(
        create_resource([("service.name", "test-service"), ("host.name", "test-host")])
    )

    # Create scope logs
    scope_logs = ScopeLogs()
    scope_logs.scope.name = "test-logs"
    scope_logs.scope.version = "1.0"

    # Create first log record (INFO)
    log1 = LogRecord()
    log1.time_unix_nano = 1609459200000000000
    log1.severity_number = 9  # INFO
    log1.severity_text = "INFO"
    log1.body.string_value = "Test operation started successfully"
    log1.attributes.append(create_key_value("test.phase", string_value="start"))

    # Create second log record (ERROR)
    log2 = LogRecord()
    log2.time_unix_nano = 1609459201000000000
    log2.severity_number = 13  # ERROR
    log2.severity_text = "ERROR"
    log2.body.string_value = "Test error condition simulated"
    log2.attributes.append(create_key_value("test.phase", string_value="error_simulation"))

    # Create third log record (DEBUG)
    log3 = LogRecord()
    log3.time_unix_nano = 1609459202000000000
    log3.severity_number = 5  # DEBUG
    log3.severity_text = "DEBUG"
    log3.body.string_value = "Debug information for test"
    log3.attributes.append(create_key_value("test.phase", string_value="debug"))

    scope_logs.log_records.extend([log1, log2, log3])
    resource_logs.scope_logs.append(scope_logs)
    request.resource_logs.append(resource_logs)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
        "expected_count": 3,
    }


@pytest.fixture
def multi_span_protobuf_traces_data():
    """Protobuf equivalent of multi_span_traces_data fixture."""
    request = ExportTraceServiceRequest()

    # Create resource spans
    resource_spans = ResourceSpans()
    resource_spans.resource.CopyFrom(create_resource([("service.name", "multi-span-service")]))

    # Create first scope spans
    scope_spans1 = ScopeSpans()
    scope_spans1.scope.name = "test-scope"
    scope_spans1.scope.version = "1.0"

    # Create spans for first scope
    span1 = Span()
    span1.trace_id = bytes.fromhex("abcdef1234567890abcdef1234567890")
    span1.span_id = bytes.fromhex("1234567890abcdef")
    span1.name = "span1"
    span1.kind = 1
    span1.start_time_unix_nano = 1609459200000000000
    span1.end_time_unix_nano = 1609459201000000000

    span2 = Span()
    span2.trace_id = bytes.fromhex("abcdef1234567890abcdef1234567890")
    span2.span_id = bytes.fromhex("abcdef1234567890")
    span2.name = "span2"
    span2.kind = 2
    span2.start_time_unix_nano = 1609459200500000000
    span2.end_time_unix_nano = 1609459200800000000

    scope_spans1.spans.extend([span1, span2])

    # Create second scope spans
    scope_spans2 = ScopeSpans()
    scope_spans2.scope.name = "test-scope-2"
    scope_spans2.scope.version = "1.0"

    span3 = Span()
    span3.trace_id = bytes.fromhex("abcdef1234567890abcdef1234567890")
    span3.span_id = bytes.fromhex("fedcba0987654321")
    span3.name = "span3"
    span3.kind = 1
    span3.start_time_unix_nano = 1609459201000000000
    span3.end_time_unix_nano = 1609459202000000000

    scope_spans2.spans.append(span3)

    resource_spans.scope_spans.extend([scope_spans1, scope_spans2])
    request.resource_spans.append(resource_spans)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
        "expected_count": 3,
    }


# Edge case fixtures
@pytest.fixture
def empty_protobuf_traces_data():
    """Empty protobuf traces data for testing validation."""
    request = ExportTraceServiceRequest()
    # Create empty resource spans array - should trigger validation error
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


@pytest.fixture
def large_protobuf_traces_data():
    """Large protobuf traces data for performance testing."""
    request = ExportTraceServiceRequest()

    # Create resource spans with many spans
    resource_spans = ResourceSpans()
    resource_spans.resource.CopyFrom(
        create_resource([("service.name", "large-test-service"), ("test.size", "large")])
    )

    scope_spans = ScopeSpans()
    scope_spans.scope.name = "large-test-scope"
    scope_spans.scope.version = "1.0"

    # Create 100 spans for performance testing
    for i in range(100):
        span = Span()
        span.trace_id = bytes.fromhex(f"{i:032x}")  # Different trace ID for each span
        span.span_id = bytes.fromhex(f"{i:016x}")
        span.name = f"large-test-span-{i}"
        span.kind = 1
        span.start_time_unix_nano = 1609459200000000000 + (i * 1000000)
        span.end_time_unix_nano = 1609459200000000000 + (i * 1000000) + 1000000
        span.attributes.append(create_key_value("span.index", int_value=i))
        span.attributes.append(create_key_value("test.type", string_value="large"))

        scope_spans.spans.append(span)

    resource_spans.scope_spans.append(scope_spans)
    request.resource_spans.append(resource_spans)

    return {
        "request": request,
        "binary_data": request.SerializeToString(),
        "expected_count": 100,
    }


# Utility functions for test data creation
def create_protobuf_traces_request(
    service_name: str = "test-service",
    span_count: int = 1,
    trace_id: str = "abcdef1234567890abcdef1234567890",
) -> ExportTraceServiceRequest:
    """Utility to create custom protobuf traces requests for testing."""
    request = ExportTraceServiceRequest()

    resource_spans = ResourceSpans()
    resource_spans.resource.CopyFrom(create_resource([("service.name", service_name)]))

    scope_spans = ScopeSpans()
    scope_spans.scope.name = "test-scope"
    scope_spans.scope.version = "1.0"

    for i in range(span_count):
        span = Span()
        span.trace_id = bytes.fromhex(trace_id)
        span.span_id = bytes.fromhex(f"{i:016x}")
        span.name = f"test-span-{i}"
        span.kind = 1
        span.start_time_unix_nano = 1609459200000000000 + (i * 1000000)
        span.end_time_unix_nano = 1609459200000000000 + (i * 1000000) + 1000000

        scope_spans.spans.append(span)

    resource_spans.scope_spans.append(scope_spans)
    request.resource_spans.append(resource_spans)

    return request


def create_protobuf_metrics_request(
    service_name: str = "test-service", metric_count: int = 1
) -> ExportMetricsServiceRequest:
    """Utility to create custom protobuf metrics requests for testing."""
    request = ExportMetricsServiceRequest()

    resource_metrics = ResourceMetrics()
    resource_metrics.resource.CopyFrom(create_resource([("service.name", service_name)]))

    scope_metrics = ScopeMetrics()
    scope_metrics.scope.name = "test-metrics"
    scope_metrics.scope.version = "1.0"

    for i in range(metric_count):
        metric = Metric()
        metric.name = f"test_metric_{i}"
        metric.description = f"Test metric {i}"
        metric.unit = "count"

        # Create sum metric
        sum_metric = Sum()
        sum_metric.aggregation_temporality = 2
        sum_metric.is_monotonic = True

        data_point = NumberDataPoint()
        data_point.time_unix_nano = 1609459200000000000
        data_point.as_int = i + 1

        sum_metric.data_points.append(data_point)
        metric.sum.CopyFrom(sum_metric)

        scope_metrics.metrics.append(metric)

    resource_metrics.scope_metrics.append(scope_metrics)
    request.resource_metrics.append(resource_metrics)

    return request


def create_protobuf_logs_request(
    service_name: str = "test-service", log_count: int = 1
) -> ExportLogsServiceRequest:
    """Utility to create custom protobuf logs requests for testing."""
    request = ExportLogsServiceRequest()

    resource_logs = ResourceLogs()
    resource_logs.resource.CopyFrom(create_resource([("service.name", service_name)]))

    scope_logs = ScopeLogs()
    scope_logs.scope.name = "test-logs"
    scope_logs.scope.version = "1.0"

    for i in range(log_count):
        log_record = LogRecord()
        log_record.time_unix_nano = 1609459200000000000 + (i * 1000000000)
        log_record.severity_number = 9  # INFO
        log_record.severity_text = "INFO"
        log_record.body.string_value = f"Test log message {i}"
        log_record.attributes.append(create_key_value("log.index", int_value=i))

        scope_logs.log_records.append(log_record)

    resource_logs.scope_logs.append(scope_logs)
    request.resource_logs.append(resource_logs)

    return request
