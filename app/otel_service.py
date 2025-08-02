"""OpenTelemetry data processing service."""

from typing import Any

import structlog

from .models import OTELLogsData, OTELMetricsData, OTELTracesData
from .mongo_client import MongoDBClient


logger = structlog.get_logger()


class OTELService:
    """Service for processing OpenTelemetry data."""

    def __init__(self, mongodb_client: MongoDBClient):
        self.mongodb_client = mongodb_client

    async def process_traces(self, traces_data: OTELTracesData, request_id: str = None) -> None:
        """Process OpenTelemetry traces data."""
        # Convert to dict and count records
        data_dict = traces_data.model_dump(by_alias=True)
        count_keys = ("resourceSpans", "scopeSpans", "spans")
        record_count = self._count_records(data_dict, count_keys)

        logger.info("Processing traces", request_id=request_id, record_count=record_count)

        # Store in database
        result = await self.mongodb_client.write_telemetry_data(
            data=data_dict, data_type="traces", request_id=request_id
        )

        # Check write result and raise exception on failure
        if not result.get("success", False):
            errors = result.get("errors", [])
            if not errors:
                errors = [result.get("error", "Unknown error")]
            error_msg = f"Failed to write traces data: {errors}"
            logger.error("Database write failed", request_id=request_id, result=result)
            raise RuntimeError(error_msg)

        logger.info(
            "Successfully processed traces", request_id=request_id, record_count=record_count
        )

    async def process_metrics(self, metrics_data: OTELMetricsData, request_id: str = None) -> None:
        """Process OpenTelemetry metrics data."""
        # Convert to dict and count records
        data_dict = metrics_data.model_dump(by_alias=True)
        count_keys = ("resourceMetrics", "scopeMetrics", "metrics")
        record_count = self._count_records(data_dict, count_keys)

        logger.info("Processing metrics", request_id=request_id, record_count=record_count)

        # Store in database
        result = await self.mongodb_client.write_telemetry_data(
            data=data_dict, data_type="metrics", request_id=request_id
        )

        # Check write result and raise exception on failure
        if not result.get("success", False):
            errors = result.get("errors", [])
            if not errors:
                errors = [result.get("error", "Unknown error")]
            error_msg = f"Failed to write metrics data: {errors}"
            logger.error("Database write failed", request_id=request_id, result=result)
            raise RuntimeError(error_msg)

        logger.info(
            "Successfully processed metrics", request_id=request_id, record_count=record_count
        )

    async def process_logs(self, logs_data: OTELLogsData, request_id: str = None) -> None:
        """Process OpenTelemetry logs data."""
        # Convert to dict and count records
        data_dict = logs_data.model_dump(by_alias=True)
        count_keys = ("resourceLogs", "scopeLogs", "logRecords")
        record_count = self._count_records(data_dict, count_keys)

        logger.info("Processing logs", request_id=request_id, record_count=record_count)

        # Store in database
        result = await self.mongodb_client.write_telemetry_data(
            data=data_dict, data_type="logs", request_id=request_id
        )

        # Check write result and raise exception on failure
        if not result.get("success", False):
            errors = result.get("errors", [])
            if not errors:
                errors = [result.get("error", "Unknown error")]
            error_msg = f"Failed to write logs data: {errors}"
            logger.error("Database write failed", request_id=request_id, result=result)
            raise RuntimeError(error_msg)

        logger.info("Successfully processed logs", request_id=request_id, record_count=record_count)

    def _count_records(self, data: dict[str, Any], count_keys: tuple[str, str, str]) -> int:
        """Count records in telemetry data using the provided key hierarchy."""
        resource_key, scope_key, record_key = count_keys
        count = 0
        for resource_item in data.get(resource_key, []):
            for scope_item in resource_item.get(scope_key, []):
                count += len(scope_item.get(record_key, []))
        return count
