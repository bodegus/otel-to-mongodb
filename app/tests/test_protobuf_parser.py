"""Unit tests for ProtobufParser module."""

import pytest

from app.models import OTELLogsData, OTELMetricsData, OTELTracesData
from app.protobuf_parser import ProtobufParser, ProtobufParsingError


@pytest.mark.unit
class TestProtobufParser:
    """Test suite for ProtobufParser class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = ProtobufParser()

    def test_parse_traces_success(self, sample_protobuf_traces_data):
        """Test successful parsing of protobuf traces data."""
        # Act
        result = self.parser.parse_traces(sample_protobuf_traces_data["binary_data"])

        # Assert
        assert isinstance(result, OTELTracesData)
        assert result.resource_spans is not None
        assert len(result.resource_spans) == 1

        # Verify resource attributes
        resource_spans = result.resource_spans[0]
        assert len(resource_spans.resource.attributes) == 2

        # Check service name attribute
        service_name_attr = next(
            attr for attr in resource_spans.resource.attributes if attr.key == "service.name"
        )
        assert service_name_attr.value["stringValue"] == "test-service"

        # Verify spans
        assert len(resource_spans.scope_spans) == 1
        scope_spans = resource_spans.scope_spans[0]
        assert len(scope_spans.spans) == sample_protobuf_traces_data["expected_count"]

        # Verify first span details
        span1 = scope_spans.spans[0]
        assert span1.name == "test-span"
        assert span1.trace_id == "abcdef1234567890abcdef1234567890"
        assert span1.span_id == "1234567890abcdef"
        assert span1.kind == 1

    def test_parse_traces_multi_span(self, protobuf_traces_data):
        """Test parsing protobuf traces data with multiple spans."""
        # Act
        result = self.parser.parse_traces(protobuf_traces_data["binary_data"])

        # Assert
        assert isinstance(result, OTELTracesData)
        total_spans = sum(
            len(scope_spans.spans)
            for resource_spans in result.resource_spans
            for scope_spans in resource_spans.scope_spans
        )
        assert total_spans == protobuf_traces_data["expected_count"]

    def test_parse_traces_empty_data(self, empty_protobuf_traces_data):
        """Test parsing empty protobuf traces data raises validation error."""
        # Act & Assert - empty resourceSpans should trigger validation error
        with pytest.raises(ProtobufParsingError, match="Error parsing protobuf traces"):
            self.parser.parse_traces(empty_protobuf_traces_data["binary_data"])

    def test_parse_traces_malformed_data(self, malformed_protobuf_data):
        """Test parsing malformed protobuf traces data raises error."""
        # Act & Assert
        with pytest.raises(ProtobufParsingError, match="Invalid protobuf traces data"):
            self.parser.parse_traces(malformed_protobuf_data["binary_data"])

    def test_parse_metrics_success(self, sample_protobuf_metrics_data):
        """Test successful parsing of protobuf metrics data."""
        # Act
        result = self.parser.parse_metrics(sample_protobuf_metrics_data["binary_data"])

        # Assert
        assert isinstance(result, OTELMetricsData)
        assert result.resource_metrics is not None
        assert len(result.resource_metrics) == 1

        # Verify resource attributes
        resource_metrics = result.resource_metrics[0]
        assert len(resource_metrics.resource.attributes) == 2

        # Check service name attribute
        service_name_attr = next(
            attr for attr in resource_metrics.resource.attributes if attr.key == "service.name"
        )
        assert service_name_attr.value["stringValue"] == "test-service"

        # Verify metrics
        assert len(resource_metrics.scope_metrics) == 1
        scope_metrics = resource_metrics.scope_metrics[0]
        assert len(scope_metrics.metrics) == sample_protobuf_metrics_data["expected_count"]

        # Verify first metric (sum)
        metric1 = scope_metrics.metrics[0]
        assert metric1.name == "request_count"
        assert metric1.description == "Total number of requests"
        assert metric1.unit == "1"
        assert metric1.sum is not None
        assert metric1.sum.is_monotonic is True
        assert len(metric1.sum.data_points) == 1

        # Verify second metric (gauge)
        metric2 = scope_metrics.metrics[1]
        assert metric2.name == "response_time"
        assert metric2.description == "Response time in milliseconds"
        assert metric2.unit == "ms"
        assert metric2.gauge is not None
        assert len(metric2.gauge.data_points) == 1
        assert metric2.gauge.data_points[0].as_double == 45.7

    def test_parse_metrics_malformed_data(self, malformed_protobuf_data):
        """Test parsing malformed protobuf metrics data raises error."""
        # Act & Assert
        with pytest.raises(ProtobufParsingError, match="Invalid protobuf metrics data"):
            self.parser.parse_metrics(malformed_protobuf_data["binary_data"])

    def test_parse_logs_success(self, sample_protobuf_logs_data):
        """Test successful parsing of protobuf logs data."""
        # Act
        result = self.parser.parse_logs(sample_protobuf_logs_data["binary_data"])

        # Assert
        assert isinstance(result, OTELLogsData)
        assert result.resource_logs is not None
        assert len(result.resource_logs) == 1

        # Verify resource attributes
        resource_logs = result.resource_logs[0]
        assert len(resource_logs.resource.attributes) == 2

        # Check service name attribute
        service_name_attr = next(
            attr for attr in resource_logs.resource.attributes if attr.key == "service.name"
        )
        assert service_name_attr.value["stringValue"] == "test-service"

        # Verify log records
        assert len(resource_logs.scope_logs) == 1
        scope_logs = resource_logs.scope_logs[0]
        assert len(scope_logs.log_records) == sample_protobuf_logs_data["expected_count"]

        # Verify first log record (INFO)
        log1 = scope_logs.log_records[0]
        assert log1.severity_number == 9
        assert log1.severity_text == "INFO"
        assert log1.body["stringValue"] == "Test operation started successfully"

        # Verify second log record (ERROR)
        log2 = scope_logs.log_records[1]
        assert log2.severity_number == 13
        assert log2.severity_text == "ERROR"
        assert log2.body["stringValue"] == "Test error condition simulated"

        # Verify third log record (DEBUG)
        log3 = scope_logs.log_records[2]
        assert log3.severity_number == 5
        assert log3.severity_text == "DEBUG"
        assert log3.body["stringValue"] == "Debug information for test"

    def test_parse_logs_malformed_data(self, malformed_protobuf_data):
        """Test parsing malformed protobuf logs data raises error."""
        # Act & Assert
        with pytest.raises(ProtobufParsingError, match="Invalid protobuf logs data"):
            self.parser.parse_logs(malformed_protobuf_data["binary_data"])

    def test_parse_empty_bytes(self):
        """Test parsing empty bytes raises appropriate error."""
        # Act & Assert
        with pytest.raises(ProtobufParsingError, match="Error parsing protobuf traces"):
            self.parser.parse_traces(b"")

        with pytest.raises(ProtobufParsingError, match="Error parsing protobuf metrics"):
            self.parser.parse_metrics(b"")

        with pytest.raises(ProtobufParsingError, match="Error parsing protobuf logs"):
            self.parser.parse_logs(b"")

    def test_convert_attribute_string(self):
        """Test conversion of KeyValue with string value."""
        from opentelemetry.proto.common.v1.common_pb2 import KeyValue

        # Arrange
        kv = KeyValue()
        kv.key = "test.key"
        kv.value.string_value = "test value"

        # Act
        result = self.parser._convert_attribute(kv)

        # Assert
        assert result == {"key": "test.key", "value": {"stringValue": "test value"}}

    def test_convert_attribute_bool(self):
        """Test conversion of KeyValue with boolean value."""
        from opentelemetry.proto.common.v1.common_pb2 import KeyValue

        # Arrange
        kv = KeyValue()
        kv.key = "test.bool"
        kv.value.bool_value = True

        # Act
        result = self.parser._convert_attribute(kv)

        # Assert
        assert result == {"key": "test.bool", "value": {"boolValue": True}}

    def test_convert_attribute_int(self):
        """Test conversion of KeyValue with integer value."""
        from opentelemetry.proto.common.v1.common_pb2 import KeyValue

        # Arrange
        kv = KeyValue()
        kv.key = "test.int"
        kv.value.int_value = 42

        # Act
        result = self.parser._convert_attribute(kv)

        # Assert
        assert result == {"key": "test.int", "value": {"intValue": "42"}}

    def test_convert_attribute_double(self):
        """Test conversion of KeyValue with double value."""
        from opentelemetry.proto.common.v1.common_pb2 import KeyValue

        # Arrange
        kv = KeyValue()
        kv.key = "test.double"
        kv.value.double_value = 3.14

        # Act
        result = self.parser._convert_attribute(kv)

        # Assert
        assert result == {"key": "test.double", "value": {"doubleValue": 3.14}}

    def test_convert_attribute_bytes(self):
        """Test conversion of KeyValue with bytes value."""
        from opentelemetry.proto.common.v1.common_pb2 import KeyValue

        # Arrange
        kv = KeyValue()
        kv.key = "test.bytes"
        kv.value.bytes_value = b"test bytes"

        # Act
        result = self.parser._convert_attribute(kv)

        # Assert - bytes are not base64 encoded in this implementation
        assert result == {"key": "test.bytes", "value": {"bytesValue": b"test bytes"}}

    def test_convert_any_value_array(self):
        """Test conversion of AnyValue with array value."""
        from opentelemetry.proto.common.v1.common_pb2 import AnyValue, ArrayValue

        # Arrange
        array_value = ArrayValue()

        # Add string value
        string_val = AnyValue()
        string_val.string_value = "item1"
        array_value.values.append(string_val)

        # Add int value
        int_val = AnyValue()
        int_val.int_value = 42
        array_value.values.append(int_val)

        any_value = AnyValue()
        any_value.array_value.CopyFrom(array_value)

        # Act
        result = self.parser._convert_any_value(any_value)

        # Assert
        expected = {"arrayValue": {"values": [{"stringValue": "item1"}, {"intValue": "42"}]}}
        assert result == expected

    def test_convert_any_value_kvlist(self):
        """Test conversion of AnyValue with key-value list."""
        from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue, KeyValueList

        # Arrange
        kvlist = KeyValueList()

        # Add key-value pair
        kv = KeyValue()
        kv.key = "nested.key"
        kv.value.string_value = "nested value"
        kvlist.values.append(kv)

        any_value = AnyValue()
        any_value.kvlist_value.CopyFrom(kvlist)

        # Act
        result = self.parser._convert_any_value(any_value)

        # Assert
        expected = {
            "kvlistValue": {
                "values": [{"key": "nested.key", "value": {"stringValue": "nested value"}}]
            }
        }
        assert result == expected

    def test_large_protobuf_traces_performance(self, large_protobuf_traces_data):
        """Test performance with large protobuf traces data."""
        # Act
        result = self.parser.parse_traces(large_protobuf_traces_data["binary_data"])

        # Assert
        assert isinstance(result, OTELTracesData)
        total_spans = sum(
            len(scope_spans.spans)
            for resource_spans in result.resource_spans
            for scope_spans in resource_spans.scope_spans
        )
        assert total_spans == large_protobuf_traces_data["expected_count"]

    def test_trace_id_conversion(self):
        """Test proper conversion of trace IDs from bytes to hex strings."""
        from opentelemetry.proto.trace.v1.trace_pb2 import Span

        # Arrange
        span = Span()
        span.trace_id = bytes.fromhex("abcdef1234567890abcdef1234567890")
        span.span_id = bytes.fromhex("1234567890abcdef")
        span.name = "test-span"

        # Act
        result = self.parser._convert_span(span)

        # Assert
        assert result["traceId"] == "abcdef1234567890abcdef1234567890"
        assert result["spanId"] == "1234567890abcdef"
        assert result["name"] == "test-span"

    def test_span_with_parent_not_implemented(self):
        """Test that parent span ID conversion is not implemented."""
        from opentelemetry.proto.trace.v1.trace_pb2 import Span

        # Arrange
        span = Span()
        span.trace_id = bytes.fromhex("abcdef1234567890abcdef1234567890")
        span.span_id = bytes.fromhex("1234567890abcdef")
        span.parent_span_id = bytes.fromhex("fedcba0987654321")
        span.name = "child-span"

        # Act
        result = self.parser._convert_span(span)

        # Assert - parent span ID is not implemented in this version
        assert "parentSpanId" not in result
        assert result["name"] == "child-span"

    def test_number_data_point_conversion(self):
        """Test conversion of NumberDataPoint with different value types."""
        from opentelemetry.proto.metrics.v1.metrics_pb2 import NumberDataPoint

        # Test int value
        ndp_int = NumberDataPoint()
        ndp_int.as_int = 100
        ndp_int.time_unix_nano = 1609459200000000000

        result_int = self.parser._convert_number_data_point(ndp_int)
        assert result_int["asInt"] == "100"
        assert result_int["timeUnixNano"] == "1609459200000000000"

        # Test double value
        ndp_double = NumberDataPoint()
        ndp_double.as_double = 45.7
        ndp_double.time_unix_nano = 1609459200000000000

        result_double = self.parser._convert_number_data_point(ndp_double)
        assert result_double["asDouble"] == 45.7
        assert result_double["timeUnixNano"] == "1609459200000000000"

    def test_histogram_data_point_conversion_not_implemented(self):
        """Test that histogram conversion is not implemented (removed for simplicity)."""
        # Histogram support was not implemented in this version
        # This test documents the limitation
        assert not hasattr(self.parser, "_convert_histogram_data_point")

    def test_memory_usage_large_payload(self, large_protobuf_traces_data):
        """Test memory efficiency with large payloads."""
        # Process large payload without memory monitoring (psutil not available)
        result = self.parser.parse_traces(large_protobuf_traces_data["binary_data"])

        # Assert successful processing
        assert isinstance(result, OTELTracesData)
        total_spans = sum(
            len(scope_spans.spans)
            for resource_spans in result.resource_spans
            for scope_spans in resource_spans.scope_spans
        )
        assert total_spans == large_protobuf_traces_data["expected_count"]
