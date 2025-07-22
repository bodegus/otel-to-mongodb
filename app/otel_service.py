"""OpenTelemetry data processing service."""

import time
from typing import Any

import structlog

from .models import OTELLogsData, OTELMetricsData, OTELTracesData, TelemetryResponse
from .mongo_client import MongoDBClient

logger = structlog.get_logger()


class OTELService:
    """Service for processing OpenTelemetry data."""

    def __init__(self, mongodb_client: MongoDBClient):
        self.mongodb_client = mongodb_client

    async def process_traces(
        self, traces_data: OTELTracesData, request_id: str = None
    ) -> TelemetryResponse:
        """Process OpenTelemetry traces data."""
        start_time = time.time()

        # Convert to dict and count records
        data_dict = traces_data.model_dump(by_alias=True)
        record_count = self._count_spans(data_dict)

        logger.info("Processing traces", request_id=request_id, record_count=record_count)

        # Store in database
        result = await self.mongodb_client.write_telemetry_data(
            data=data_dict, data_type="traces", request_id=request_id
        )

        processing_time_ms = (time.time() - start_time) * 1000

        return TelemetryResponse(
            success=True,
            message=f"Successfully processed {record_count} traces",
            data_type="traces",
            records_processed=record_count,
            local_storage=result["local_success"],
            cloud_storage=result["cloud_success"],
            processing_time_ms=processing_time_ms,
            document_id=result["document_id"],
        )

    async def process_metrics(
        self, metrics_data: OTELMetricsData, request_id: str = None
    ) -> TelemetryResponse:
        """Process OpenTelemetry metrics data."""
        start_time = time.time()
        # Convert to dict and count records
        data_dict = metrics_data.model_dump(by_alias=True)
        record_count = self._count_metrics(data_dict)

        logger.info("Processing metrics", request_id=request_id, record_count=record_count)

        # Store in database
        result = await self.mongodb_client.write_telemetry_data(
            data=data_dict, data_type="metrics", request_id=request_id
        )

        processing_time_ms = (time.time() - start_time) * 1000

        return TelemetryResponse(
            success=True,
            message=f"Successfully processed {record_count} metrics",
            data_type="metrics",
            records_processed=record_count,
            local_storage=result["local_success"],
            cloud_storage=result["cloud_success"],
            processing_time_ms=processing_time_ms,
            document_id=result["document_id"],
        )

    async def process_logs(
        self, logs_data: OTELLogsData, request_id: str = None
    ) -> TelemetryResponse:
        """Process OpenTelemetry logs data."""
        start_time = time.time()

        # Convert to dict and count records
        data_dict = logs_data.model_dump(by_alias=True)
        record_count = self._count_log_records(data_dict)

        logger.info("Processing logs", request_id=request_id, record_count=record_count)

        # Store in database
        result = await self.mongodb_client.write_telemetry_data(
            data=data_dict, data_type="logs", request_id=request_id
        )

        processing_time_ms = (time.time() - start_time) * 1000

        return TelemetryResponse(
            success=True,
            message=f"Successfully processed {record_count} logs",
            data_type="logs",
            records_processed=record_count,
            local_storage=result["local_success"],
            cloud_storage=result["cloud_success"],
            processing_time_ms=processing_time_ms,
            document_id=result["document_id"],
        )

    def _count_spans(self, traces_data: dict[str, Any]) -> int:
        """Count spans in traces data."""
        count = 0
        for resource_span in traces_data.get("resourceSpans", []):
            for scope_span in resource_span.get("scopeSpans", []):
                count += len(scope_span.get("spans", []))
        return count

    def _count_metrics(self, metrics_data: dict[str, Any]) -> int:
        """Count metrics in metrics data."""
        count = 0
        for resource_metric in metrics_data.get("resourceMetrics", []):
            for scope_metric in resource_metric.get("scopeMetrics", []):
                count += len(scope_metric.get("metrics", []))
        return count

    def _count_log_records(self, logs_data: dict[str, Any]) -> int:
        """Count log records in logs data."""
        count = 0
        for resource_log in logs_data.get("resourceLogs", []):
            for scope_log in resource_log.get("scopeLogs", []):
                count += len(scope_log.get("logRecords", []))
        return count
