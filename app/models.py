"""Pydantic models for OpenTelemetry data and API responses."""

from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, field_validator


# OpenTelemetry Base Models
class OTELAttribute(BaseModel):
    """OpenTelemetry attribute."""
    key: str
    value: Dict[str, Any]


class OTELResource(BaseModel):
    """OpenTelemetry resource."""
    attributes: List[OTELAttribute] = Field(default_factory=list)


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
    attributes: List[OTELAttribute] = Field(default_factory=list)

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
    spans: List[OTELSpan] = Field(default_factory=list)


class OTELResourceSpans(BaseModel):
    """OpenTelemetry resource spans."""
    resource: OTELResource
    scope_spans: List[OTELScopeSpans] = Field(
        default_factory=list, alias="scopeSpans")


class OTELTracesData(BaseModel):
    """OpenTelemetry traces data."""
    resource_spans: List[OTELResourceSpans] = Field(alias="resourceSpans")

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
    attributes: List[OTELAttribute] = Field(default_factory=list)


class OTELSum(BaseModel):
    """OpenTelemetry sum metric."""
    data_points: List[OTELNumberDataPoint] = Field(alias="dataPoints")
    aggregation_temporality: int = Field(alias="aggregationTemporality")
    is_monotonic: bool = Field(alias="isMonotonic")


class OTELGauge(BaseModel):
    """OpenTelemetry gauge metric."""
    data_points: List[OTELNumberDataPoint] = Field(alias="dataPoints")


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
    metrics: List[OTELMetric] = Field(default_factory=list)


class OTELResourceMetrics(BaseModel):
    """OpenTelemetry resource metrics."""
    resource: OTELResource
    scope_metrics: List[OTELScopeMetrics] = Field(
        default_factory=list, alias="scopeMetrics")


class OTELMetricsData(BaseModel):
    """OpenTelemetry metrics data."""
    resource_metrics: List[OTELResourceMetrics] = Field(
        alias="resourceMetrics")

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
    severity_number: Optional[int] = Field(
        default=None, alias="severityNumber")
    severity_text: Optional[str] = Field(default=None, alias="severityText")
    body: Optional[Dict[str, Any]] = None
    attributes: List[OTELAttribute] = Field(default_factory=list)
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
    log_records: List[OTELLogRecord] = Field(
        default_factory=list, alias="logRecords")


class OTELResourceLogs(BaseModel):
    """OpenTelemetry resource logs."""
    resource: OTELResource
    scope_logs: List[OTELScopeLogs] = Field(
        default_factory=list, alias="scopeLogs")


class OTELLogsData(BaseModel):
    """OpenTelemetry logs data."""
    resource_logs: List[OTELResourceLogs] = Field(alias="resourceLogs")

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


class ErrorResponse(BaseModel):
    """Error response."""
    success: bool = False
    message: str
    error_code: str
