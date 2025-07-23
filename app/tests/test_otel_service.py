"""Tests for OTEL service."""

# ruff: noqa: F811  # Ignore fixture redefinition warnings (imports vs function parameters)

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import OTELLogsData, OTELMetricsData, OTELTracesData
from app.otel_service import OTELService

# Import shared fixtures (used as pytest fixtures in function parameters)
from .fixtures.otel_data import (  # noqa: F401
    multi_logs_data,
    multi_metrics_data,
    multi_span_traces_data,
    sample_logs_data,
    sample_metrics_data,
    sample_traces_data,
)


class TestOTELService:
    """Test OTEL service functionality."""

    @pytest.fixture
    def mock_mongodb_client(self):
        """Mock MongoDB client."""
        client = MagicMock()
        client.write_telemetry_data = AsyncMock(
            return_value={
                "success": True,
                "primary_success": True,
                "secondary_success": True,
                "document_id": "test_id_123",
            }
        )
        return client

    @pytest.fixture
    def otel_service(self, mock_mongodb_client):
        """Create OTEL service instance with mocked MongoDB client."""
        return OTELService(mock_mongodb_client)

    # sample_traces_data fixture now imported from shared fixtures

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_traces_success(
        self, otel_service, mock_mongodb_client, sample_traces_data
    ):
        """Test successful traces processing."""
        # Convert dict to Pydantic model - use data from fixture
        traces_data = OTELTracesData(**sample_traces_data["data"])

        result = await otel_service.process_traces(traces_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()

        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "traces"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

        # Verify return values using expected count from fixture
        assert result.success is True
        assert result.data_type == "traces"
        assert result.records_processed == sample_traces_data["expected_count"]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_metrics_success(
        self, otel_service, mock_mongodb_client, sample_metrics_data
    ):
        """Test successful metrics processing using shared test data."""
        # Convert dict to Pydantic model - use data from fixture
        metrics_data = OTELMetricsData(**sample_metrics_data["data"])

        result = await otel_service.process_metrics(metrics_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()

        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "metrics"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

        # Verify return values using expected count from fixture
        assert result.success is True
        assert result.data_type == "metrics"
        assert result.records_processed == sample_metrics_data["expected_count"]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_logs_success(self, otel_service, mock_mongodb_client, sample_logs_data):
        """Test successful logs processing using shared test data."""
        # Convert dict to Pydantic model - use data from fixture
        logs_data = OTELLogsData(**sample_logs_data["data"])

        result = await otel_service.process_logs(logs_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()

        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "logs"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

        # Verify return values using expected count from fixture
        assert result.success is True
        assert result.data_type == "logs"
        assert result.records_processed == sample_logs_data["expected_count"]

    @pytest.mark.unit
    def test_count_spans(self, otel_service, multi_span_traces_data):
        """Test span counting using multi_span fixture."""
        count = otel_service._count_spans(multi_span_traces_data["data"])
        assert count == multi_span_traces_data["expected_count"]

    @pytest.mark.unit
    def test_count_metrics(self, otel_service, multi_metrics_data):
        """Test metrics counting using multi_metrics fixture."""
        count = otel_service._count_metrics(multi_metrics_data["data"])
        assert count == multi_metrics_data["expected_count"]

    @pytest.mark.unit
    def test_count_log_records(self, otel_service, multi_logs_data):
        """Test log records counting using multi_logs fixture."""
        count = otel_service._count_log_records(multi_logs_data["data"])
        assert count == multi_logs_data["expected_count"]
