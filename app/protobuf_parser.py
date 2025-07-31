"""Protocol Buffer parser for OpenTelemetry data."""

from typing import Any

import structlog
from google.protobuf.message import DecodeError
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from .models import OTELLogsData, OTELMetricsData, OTELTracesData


logger = structlog.get_logger()


class ProtobufParsingError(Exception):
    """Exception raised when protobuf parsing fails."""

    pass


class ProtobufParser:
    """Parser for OTLP protobuf messages to Pydantic models."""

    def parse_traces(self, data: bytes) -> OTELTracesData:
        """Parse protobuf traces to OTELTracesData model."""
        try:
            pb_request = ExportTraceServiceRequest()
            pb_request.ParseFromString(data)

            traces_dict = self._convert_traces_to_dict(pb_request)
            logger.info(
                "Successfully parsed protobuf traces",
                resource_spans_count=len(traces_dict["resourceSpans"]),
            )

            return OTELTracesData(**traces_dict)

        except DecodeError as e:
            logger.error("Failed to parse protobuf traces", error=str(e))
            raise ProtobufParsingError(f"Invalid protobuf traces data: {str(e)}") from e
        except Exception as e:
            logger.error("Unexpected error parsing protobuf traces", error=str(e), exc_info=True)
            raise ProtobufParsingError(f"Error parsing protobuf traces: {str(e)}") from e

    def parse_metrics(self, data: bytes) -> OTELMetricsData:
        """Parse protobuf metrics to OTELMetricsData model."""
        try:
            pb_request = ExportMetricsServiceRequest()
            pb_request.ParseFromString(data)

            metrics_dict = self._convert_metrics_to_dict(pb_request)
            logger.info(
                "Successfully parsed protobuf metrics",
                resource_metrics_count=len(metrics_dict["resourceMetrics"]),
            )

            return OTELMetricsData(**metrics_dict)

        except DecodeError as e:
            logger.error("Failed to parse protobuf metrics", error=str(e))
            raise ProtobufParsingError(f"Invalid protobuf metrics data: {str(e)}") from e
        except Exception as e:
            logger.error("Unexpected error parsing protobuf metrics", error=str(e), exc_info=True)
            raise ProtobufParsingError(f"Error parsing protobuf metrics: {str(e)}") from e

    def parse_logs(self, data: bytes) -> OTELLogsData:
        """Parse protobuf logs to OTELLogsData model."""
        try:
            pb_request = ExportLogsServiceRequest()
            pb_request.ParseFromString(data)

            logs_dict = self._convert_logs_to_dict(pb_request)
            logger.info(
                "Successfully parsed protobuf logs",
                resource_logs_count=len(logs_dict["resourceLogs"]),
            )

            return OTELLogsData(**logs_dict)

        except DecodeError as e:
            logger.error("Failed to parse protobuf logs", error=str(e))
            raise ProtobufParsingError(f"Invalid protobuf logs data: {str(e)}") from e
        except Exception as e:
            logger.error("Unexpected error parsing protobuf logs", error=str(e), exc_info=True)
            raise ProtobufParsingError(f"Error parsing protobuf logs: {str(e)}") from e

    def _convert_traces_to_dict(self, pb_request: ExportTraceServiceRequest) -> dict[str, Any]:
        """Convert protobuf traces to dict suitable for Pydantic."""
        return {
            "resourceSpans": [
                {
                    "resource": self._convert_resource(rs.resource),
                    "scopeSpans": [
                        {
                            "scope": self._convert_scope(ss.scope),
                            "spans": [self._convert_span(span) for span in ss.spans],
                        }
                        for ss in rs.scope_spans
                    ],
                }
                for rs in pb_request.resource_spans
            ]
        }

    def _convert_metrics_to_dict(self, pb_request: ExportMetricsServiceRequest) -> dict[str, Any]:
        """Convert protobuf metrics to dict suitable for Pydantic."""
        return {
            "resourceMetrics": [
                {
                    "resource": self._convert_resource(rm.resource),
                    "scopeMetrics": [
                        {
                            "scope": self._convert_scope(sm.scope),
                            "metrics": [self._convert_metric(metric) for metric in sm.metrics],
                        }
                        for sm in rm.scope_metrics
                    ],
                }
                for rm in pb_request.resource_metrics
            ]
        }

    def _convert_logs_to_dict(self, pb_request: ExportLogsServiceRequest) -> dict[str, Any]:
        """Convert protobuf logs to dict suitable for Pydantic."""
        return {
            "resourceLogs": [
                {
                    "resource": self._convert_resource(rl.resource),
                    "scopeLogs": [
                        {
                            "scope": self._convert_scope(sl.scope),
                            "logRecords": [self._convert_log_record(log) for log in sl.log_records],
                        }
                        for sl in rl.scope_logs
                    ],
                }
                for rl in pb_request.resource_logs
            ]
        }

    def _convert_resource(self, pb_resource: Any) -> dict[str, Any]:
        """Convert protobuf Resource to dict."""
        return {"attributes": [self._convert_attribute(attr) for attr in pb_resource.attributes]}

    def _convert_scope(self, pb_scope: Any) -> dict[str, Any]:
        """Convert protobuf InstrumentationScope to dict."""
        return {"name": pb_scope.name, "version": pb_scope.version if pb_scope.version else None}

    def _convert_attribute(self, pb_attribute: Any) -> dict[str, Any]:
        """Convert protobuf KeyValue to dict."""
        return {"key": pb_attribute.key, "value": self._convert_any_value(pb_attribute.value)}

    def _convert_any_value(self, pb_any_value: Any) -> dict[str, Any]:  # noqa: PLR0911
        """Convert protobuf AnyValue to dict."""
        # Handle different value types based on the oneof field
        if pb_any_value.HasField("string_value"):
            return {"stringValue": pb_any_value.string_value}
        elif pb_any_value.HasField("bool_value"):
            return {"boolValue": pb_any_value.bool_value}
        elif pb_any_value.HasField("int_value"):
            return {"intValue": str(pb_any_value.int_value)}
        elif pb_any_value.HasField("double_value"):
            return {"doubleValue": pb_any_value.double_value}
        elif pb_any_value.HasField("array_value"):
            return {
                "arrayValue": {
                    "values": [
                        self._convert_any_value(val) for val in pb_any_value.array_value.values
                    ]
                }
            }
        elif pb_any_value.HasField("kvlist_value"):
            return {
                "kvlistValue": {
                    "values": [
                        self._convert_attribute(kv) for kv in pb_any_value.kvlist_value.values
                    ]
                }
            }
        elif pb_any_value.HasField("bytes_value"):
            return {"bytesValue": pb_any_value.bytes_value}
        else:
            # Default to empty value
            return {}

    def _convert_span(self, pb_span: Any) -> dict[str, Any]:
        """Convert protobuf Span to dict."""
        return {
            "traceId": pb_span.trace_id.hex(),
            "spanId": pb_span.span_id.hex(),
            "name": pb_span.name,
            "kind": pb_span.kind,
            "startTimeUnixNano": str(pb_span.start_time_unix_nano),
            "endTimeUnixNano": str(pb_span.end_time_unix_nano),
            "attributes": [self._convert_attribute(attr) for attr in pb_span.attributes],
        }

    def _convert_metric(self, pb_metric: Any) -> dict[str, Any]:
        """Convert protobuf Metric to dict."""
        metric_dict = {
            "name": pb_metric.name,
            "description": pb_metric.description if pb_metric.description else None,
            "unit": pb_metric.unit if pb_metric.unit else None,
        }

        # Handle different metric types
        if pb_metric.HasField("gauge"):
            metric_dict["gauge"] = {
                "dataPoints": [
                    self._convert_number_data_point(dp) for dp in pb_metric.gauge.data_points
                ]
            }
        elif pb_metric.HasField("sum"):
            metric_dict["sum"] = {
                "dataPoints": [
                    self._convert_number_data_point(dp) for dp in pb_metric.sum.data_points
                ],
                "aggregationTemporality": pb_metric.sum.aggregation_temporality,
                "isMonotonic": pb_metric.sum.is_monotonic,
            }

        return metric_dict

    def _convert_number_data_point(self, pb_data_point: Any) -> dict[str, Any]:
        """Convert protobuf NumberDataPoint to dict."""
        data_point = {
            "timeUnixNano": str(pb_data_point.time_unix_nano),
            "attributes": [self._convert_attribute(attr) for attr in pb_data_point.attributes],
        }

        # Handle value types
        if pb_data_point.HasField("as_double"):
            data_point["asDouble"] = pb_data_point.as_double
        elif pb_data_point.HasField("as_int"):
            data_point["asInt"] = str(pb_data_point.as_int)

        return data_point

    def _convert_log_record(self, pb_log_record: Any) -> dict[str, Any]:
        """Convert protobuf LogRecord to dict."""
        log_dict = {
            "attributes": [self._convert_attribute(attr) for attr in pb_log_record.attributes]
        }

        # Handle optional fields
        if pb_log_record.time_unix_nano:
            log_dict["timeUnixNano"] = str(pb_log_record.time_unix_nano)
        if pb_log_record.severity_number:
            log_dict["severityNumber"] = pb_log_record.severity_number
        if pb_log_record.severity_text:
            log_dict["severityText"] = pb_log_record.severity_text
        if pb_log_record.HasField("body"):
            log_dict["body"] = self._convert_any_value(pb_log_record.body)
        if pb_log_record.trace_id:
            log_dict["traceId"] = pb_log_record.trace_id.hex()
        if pb_log_record.span_id:
            log_dict["spanId"] = pb_log_record.span_id.hex()

        return log_dict
