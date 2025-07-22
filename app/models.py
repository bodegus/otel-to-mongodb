"""Pydantic models for OpenTelemetry data and API responses."""

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


# OpenTelemetry Base Models
class OTELAttribute(BaseModel):
    """OpenTelemetry attribute."""

    key: str
    value: dict[str, Any]


class OTELResource(BaseModel):
    """OpenTelemetry resource."""

    attributes: list[OTELAttribute] = Field(default_factory=list)


class OTELScope(BaseModel):
    """OpenTelemetry instrumentation scope."""

    name: str
    version: Optional[str] = None


class OTELSpan(BaseModel):
    """OpenTelemetry span."""

    trace_id: str = Field(alias="traceId")
    span_id: str = Field(alias="spanId")
    name: str
    kind: int
    start_time_unix_nano: str = Field(alias="startTimeUnixNano")
    end_time_unix_nano: str = Field(alias="endTimeUnixNano")
    attributes: list[OTELAttribute] = Field(default_factory=list)

    @field_validator("trace_id", "span_id")
    @classmethod
    def validate_hex_strings(cls, v):
        """Validate hex strings."""
        if v:
            try:
                int(v, 16)
            except ValueError:
                raise ValueError(f"Invalid hex string: {v}")
        return v


class OTELScopeSpans(BaseModel):
    """OpenTelemetry scope spans."""

    scope: OTELScope
    spans: list[OTELSpan] = Field(default_factory=list)


class OTELResourceSpans(BaseModel):
    """OpenTelemetry resource spans."""

    resource: OTELResource
    scope_spans: list[OTELScopeSpans] = Field(default_factory=list, alias="scopeSpans")


class OTELTracesData(BaseModel):
    """OpenTelemetry traces data."""

    resource_spans: list[OTELResourceSpans] = Field(alias="resourceSpans")

    @field_validator("resource_spans")
    @classmethod
    def validate_non_empty(cls, v):
        """Ensure resource spans is not empty."""
        if not v:
            raise ValueError("resourceSpans cannot be empty")
        return v


# Metrics Models
class OTELNumberDataPoint(BaseModel):
    """OpenTelemetry number data point."""

    time_unix_nano: str = Field(alias="timeUnixNano")
    as_double: Optional[float] = Field(default=None, alias="asDouble")
    as_int: Optional[str] = Field(default=None, alias="asInt")
    attributes: list[OTELAttribute] = Field(default_factory=list)


class OTELSum(BaseModel):
    """OpenTelemetry sum metric."""

    data_points: list[OTELNumberDataPoint] = Field(alias="dataPoints")
    aggregation_temporality: int = Field(alias="aggregationTemporality")
    is_monotonic: bool = Field(alias="isMonotonic")


class OTELGauge(BaseModel):
    """OpenTelemetry gauge metric."""

    data_points: list[OTELNumberDataPoint] = Field(alias="dataPoints")


class OTELMetric(BaseModel):
    """OpenTelemetry metric."""

    name: str
    description: Optional[str] = None
    unit: Optional[str] = None
    gauge: Optional[OTELGauge] = None
    sum: Optional[OTELSum] = None

    @field_validator("name")
    @classmethod
    def validate_metric_name(cls, v):
        """Validate metric name."""
        if not v or not v.strip():
            raise ValueError("Metric name cannot be empty")
        return v.strip()


class OTELScopeMetrics(BaseModel):
    """OpenTelemetry scope metrics."""

    scope: OTELScope
    metrics: list[OTELMetric] = Field(default_factory=list)


class OTELResourceMetrics(BaseModel):
    """OpenTelemetry resource metrics."""

    resource: OTELResource
    scope_metrics: list[OTELScopeMetrics] = Field(default_factory=list, alias="scopeMetrics")


class OTELMetricsData(BaseModel):
    """OpenTelemetry metrics data."""

    resource_metrics: list[OTELResourceMetrics] = Field(alias="resourceMetrics")

    @field_validator("resource_metrics")
    @classmethod
    def validate_non_empty(cls, v):
        """Ensure resource metrics is not empty."""
        if not v:
            raise ValueError("resourceMetrics cannot be empty")
        return v


# Logs Models
class OTELLogRecord(BaseModel):
    """OpenTelemetry log record."""

    time_unix_nano: Optional[str] = Field(default=None, alias="timeUnixNano")
    severity_number: Optional[int] = Field(default=None, alias="severityNumber")
    severity_text: Optional[str] = Field(default=None, alias="severityText")
    body: Optional[dict[str, Any]] = None
    attributes: list[OTELAttribute] = Field(default_factory=list)
    trace_id: Optional[str] = Field(default=None, alias="traceId")
    span_id: Optional[str] = Field(default=None, alias="spanId")

    @field_validator("trace_id", "span_id")
    @classmethod
    def validate_hex_strings(cls, v):
        """Validate hex strings."""
        if v:
            try:
                int(v, 16)
            except ValueError:
                raise ValueError(f"Invalid hex string: {v}")
        return v


class OTELScopeLogs(BaseModel):
    """OpenTelemetry scope logs."""

    scope: OTELScope
    log_records: list[OTELLogRecord] = Field(default_factory=list, alias="logRecords")


class OTELResourceLogs(BaseModel):
    """OpenTelemetry resource logs."""

    resource: OTELResource
    scope_logs: list[OTELScopeLogs] = Field(default_factory=list, alias="scopeLogs")


class OTELLogsData(BaseModel):
    """OpenTelemetry logs data."""

    resource_logs: list[OTELResourceLogs] = Field(alias="resourceLogs")

    @field_validator("resource_logs")
    @classmethod
    def validate_non_empty(cls, v):
        """Ensure resource logs is not empty."""
        if not v:
            raise ValueError("resourceLogs cannot be empty")
        return v


# Response Models
class TelemetryResponse(BaseModel):
    """Response for telemetry data submission."""

    success: bool = True
    message: str
    data_type: str
    records_processed: int
    local_storage: bool
    cloud_storage: bool
    processing_time_ms: float
    document_id: Optional[str] = None


# OTLP-Compliant Response Models (as per OTLP specification)
class ExportTracePartialSuccess(BaseModel):
    """OTLP Export trace partial success."""

    rejected_spans: int = Field(default=0, alias="rejectedSpans")
    error_message: str = Field(default="", alias="errorMessage")


class ExportMetricsPartialSuccess(BaseModel):
    """OTLP Export metrics partial success."""

    rejected_data_points: int = Field(default=0, alias="rejectedDataPoints")
    error_message: str = Field(default="", alias="errorMessage")


class ExportLogsPartialSuccess(BaseModel):
    """OTLP Export logs partial success."""

    rejected_log_records: int = Field(default=0, alias="rejectedLogRecords")
    error_message: str = Field(default="", alias="errorMessage")


class ExportTraceServiceResponse(BaseModel):
    """OTLP-compliant trace export response."""

    partial_success: Optional[ExportTracePartialSuccess] = Field(
        default=None, alias="partialSuccess"
    )


class ExportMetricsServiceResponse(BaseModel):
    """OTLP-compliant metrics export response."""

    partial_success: Optional[ExportMetricsPartialSuccess] = Field(
        default=None, alias="partialSuccess"
    )


class ExportLogsServiceResponse(BaseModel):
    """OTLP-compliant logs export response."""

    partial_success: Optional[ExportLogsPartialSuccess] = Field(
        default=None, alias="partialSuccess"
    )


class Status(BaseModel):
    """OTLP Status message for errors."""

    code: Optional[int] = None
    message: str
    details: Optional[list[dict[str, Any]]] = None


class ErrorResponse(BaseModel):
    """Error response."""

    success: bool = False
    message: str
    error_code: str
