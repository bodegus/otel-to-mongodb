"""Tests for OTEL service."""

from unittest.mock import AsyncMock

import pytest

from app.models import OTELLogsData, OTELMetricsData, OTELTracesData
from app.otel_service import OTELService


class TestOTELService:
    """Test OTEL service functionality."""

    @pytest.fixture
    def otel_service(self, mock_mongodb_client):
        """Create OTEL service instance with mocked MongoDB client."""
        return OTELService(mock_mongodb_client)

    # sample_traces_data fixture now imported from shared fixtures

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_traces_success(
        self, otel_service, mock_mongodb_client, json_traces_data
    ):
        """Test successful traces processing."""
        # Convert dict to Pydantic model - use data from fixture
        traces_data = OTELTracesData(**json_traces_data["data"])

        # Should not raise any exceptions (success case)
        await otel_service.process_traces(traces_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()

        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "traces"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_metrics_success(
        self, otel_service, mock_mongodb_client, json_metrics_data
    ):
        """Test successful metrics processing using shared test data."""
        # Convert dict to Pydantic model - use data from fixture
        metrics_data = OTELMetricsData(**json_metrics_data["data"])

        # Should not raise any exceptions (success case)
        await otel_service.process_metrics(metrics_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()

        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "metrics"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_logs_success(self, otel_service, mock_mongodb_client, json_logs_data):
        """Test successful logs processing using shared test data."""
        # Convert dict to Pydantic model - use data from fixture
        logs_data = OTELLogsData(**json_logs_data["data"])

        # Should not raise any exceptions (success case)
        await otel_service.process_logs(logs_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()

        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "logs"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

    @pytest.mark.unit
    def test_count_spans(self, otel_service, json_traces_data):
        """Test span counting using unified traces fixture."""
        count_keys = ("resourceSpans", "scopeSpans", "spans")
        count = otel_service._count_records(json_traces_data["data"], count_keys)
        assert count == json_traces_data["expected_count"]

    @pytest.mark.unit
    def test_count_metrics(self, otel_service, json_metrics_data):
        """Test metrics counting using unified metrics fixture."""
        count_keys = ("resourceMetrics", "scopeMetrics", "metrics")
        count = otel_service._count_records(json_metrics_data["data"], count_keys)
        assert count == json_metrics_data["expected_count"]

    @pytest.mark.unit
    def test_count_log_records(self, otel_service, json_logs_data):
        """Test log records counting using unified logs fixture."""
        count_keys = ("resourceLogs", "scopeLogs", "logRecords")
        count = otel_service._count_records(json_logs_data["data"], count_keys)
        assert count == json_logs_data["expected_count"]

    @pytest.mark.unit
    async def test_process_traces_write_failure(self, otel_service, json_traces_data):
        """Test traces processing handles write failures properly."""
        # Mock MongoDB client to return failure
        otel_service.mongodb_client.write_telemetry_data = AsyncMock(
            return_value={"success": False, "errors": ["Database connection failed"]}
        )

        traces_data = OTELTracesData(**json_traces_data["data"])

        with pytest.raises(RuntimeError, match="Failed to write traces data"):
            await otel_service.process_traces(traces_data)

    @pytest.mark.unit
    async def test_process_metrics_write_failure(self, otel_service, json_metrics_data):
        """Test metrics processing handles write failures properly."""
        # Mock MongoDB client to return failure
        otel_service.mongodb_client.write_telemetry_data = AsyncMock(
            return_value={"success": False, "errors": ["Database connection failed"]}
        )

        metrics_data = OTELMetricsData(**json_metrics_data["data"])

        with pytest.raises(RuntimeError, match="Failed to write metrics data"):
            await otel_service.process_metrics(metrics_data)

    @pytest.mark.unit
    async def test_process_logs_write_failure(self, otel_service, json_logs_data):
        """Test logs processing handles write failures properly."""
        # Mock MongoDB client to return failure
        otel_service.mongodb_client.write_telemetry_data = AsyncMock(
            return_value={"success": False, "errors": ["Database connection failed"]}
        )

        logs_data = OTELLogsData(**json_logs_data["data"])

        with pytest.raises(RuntimeError, match="Failed to write logs data"):
            await otel_service.process_logs(logs_data)
