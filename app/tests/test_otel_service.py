"""Tests for OTEL service."""

# Ignore fixture redefinition warnings (imports vs function parameters)

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models import OTELLogsData, OTELMetricsData, OTELTracesData
from app.otel_service import OTELService


# Use unified fixtures from conftest.py - no explicit imports needed
# Fixtures: json_traces_data, json_metrics_data, json_logs_data are automatically available


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
        self, otel_service, mock_mongodb_client, json_traces_data
    ):
        """Test successful traces processing."""
        # Convert dict to Pydantic model - use data from fixture
        traces_data = OTELTracesData(**json_traces_data["data"])

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
        assert result.records_processed == json_traces_data["expected_count"]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_metrics_success(
        self, otel_service, mock_mongodb_client, json_metrics_data
    ):
        """Test successful metrics processing using shared test data."""
        # Convert dict to Pydantic model - use data from fixture
        metrics_data = OTELMetricsData(**json_metrics_data["data"])

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
        assert result.records_processed == json_metrics_data["expected_count"]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_process_logs_success(self, otel_service, mock_mongodb_client, json_logs_data):
        """Test successful logs processing using shared test data."""
        # Convert dict to Pydantic model - use data from fixture
        logs_data = OTELLogsData(**json_logs_data["data"])

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
        assert result.records_processed == json_logs_data["expected_count"]

    @pytest.mark.unit
    def test_count_spans(self, otel_service, json_traces_data):
        """Test span counting using unified traces fixture."""
        count = otel_service._count_spans(json_traces_data["data"])
        assert count == json_traces_data["expected_count"]

    @pytest.mark.unit
    def test_count_metrics(self, otel_service, json_metrics_data):
        """Test metrics counting using unified metrics fixture."""
        count = otel_service._count_metrics(json_metrics_data["data"])
        assert count == json_metrics_data["expected_count"]

    @pytest.mark.unit
    def test_count_log_records(self, otel_service, json_logs_data):
        """Test log records counting using unified logs fixture."""
        count = otel_service._count_log_records(json_logs_data["data"])
        assert count == json_logs_data["expected_count"]
