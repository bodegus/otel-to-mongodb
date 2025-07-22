"""Tests for OTEL service."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from app.otel_service import OTELService
from app.models import OTELTracesData, OTELMetricsData, OTELLogsData


class TestOTELService:
    """Test OTEL service functionality."""

    @pytest.fixture
    def mock_mongodb_client(self):
        """Mock MongoDB client."""
        client = MagicMock()
        client.write_telemetry_data = AsyncMock(return_value={
            "local_success": True,
            "cloud_success": True,
            "document_id": "test_id_123"
        })
        return client

    @pytest.fixture
    def otel_service(self, mock_mongodb_client):
        """Create OTEL service instance with mocked MongoDB client."""
        return OTELService(mock_mongodb_client)

    @pytest.fixture
    def sample_traces_data(self):
        """Sample traces data for testing."""
        return {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name", 
                                "value": {"stringValue": "test-service"}
                            }
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "test"},
                            "spans": [
                                {
                                    "traceId": 
                                        "12345678901234567890123456789012",
                                    "spanId": "1234567890123456",
                                    "name": "test-span",
                                    "kind": 1,
                                    "startTimeUnixNano": 
                                        "1609459200000000000",
                                    "endTimeUnixNano": 
                                        "1609459201000000000",
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    @pytest.mark.asyncio
    async def test_process_traces_success(
        self, otel_service, mock_mongodb_client, sample_traces_data
    ):
        """Test successful traces processing."""
        # Convert dict to Pydantic model
        traces_data = OTELTracesData(**sample_traces_data)
        
        result = await otel_service.process_traces(traces_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()
        
        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "traces"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

        # Verify return values
        assert result.success is True
        assert result.data_type == "traces"
        assert result.records_processed == 1

    @pytest.mark.asyncio
    async def test_process_metrics_success(
        self, otel_service, mock_mongodb_client
    ):
        """Test successful metrics processing."""
        sample_metrics_data = {
            "resourceMetrics": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name", 
                                "value": {"stringValue": "test-service"}
                            }
                        ]
                    },
                    "scopeMetrics": [
                        {
                            "scope": {"name": "test"},
                            "metrics": [
                                {
                                    "name": "test_counter",
                                    "description": "A test counter",
                                    "unit": "1"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        
        # Convert dict to Pydantic model
        metrics_data = OTELMetricsData(**sample_metrics_data)
        
        result = await otel_service.process_metrics(metrics_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()
        
        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "metrics"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

        # Verify return values
        assert result.success is True
        assert result.data_type == "metrics"
        assert result.records_processed == 1

    @pytest.mark.asyncio
    async def test_process_logs_success(
        self, otel_service, mock_mongodb_client
    ):
        """Test successful logs processing."""
        sample_logs_data = {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name", 
                                "value": {"stringValue": "test-service"}
                            }
                        ]
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": "test"},
                            "logRecords": [
                                {
                                    "timeUnixNano": "1609459200000000000",
                                    "body": {
                                        "stringValue": "Test log message"
                                    },
                                    "severityText": "INFO"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        
        # Convert dict to Pydantic model
        logs_data = OTELLogsData(**sample_logs_data)
        
        result = await otel_service.process_logs(logs_data)

        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()
        
        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "logs"
        assert "data" in call_args[1]
        assert call_args[1]["request_id"] is None

        # Verify return values
        assert result.success is True
        assert result.data_type == "logs"
        assert result.records_processed == 1

    def test_count_spans(self, otel_service):
        """Test span counting."""
        data = {
            "resourceSpans": [
                {
                    "scopeSpans": [
                        {"spans": [{"name": "span1"}, {"name": "span2"}]},
                        {"spans": [{"name": "span3"}]}
                    ]
                }
            ]
        }

        count = otel_service._count_spans(data)
        assert count == 3

    def test_count_metrics(self, otel_service):
        """Test metrics counting."""
        data = {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {"metrics": [{"name": "metric1"}, {"name": "metric2"}]}
                    ]
                }
            ]
        }

        count = otel_service._count_metrics(data)
        assert count == 2

    def test_count_log_records(self, otel_service):
        """Test log records counting."""
        data = {
            "resourceLogs": [
                {
                    "scopeLogs": [
                        {"logRecords": [{"body": "log1"}, {"body": "log2"}]}
                    ]
                }
            ]
        }

        count = otel_service._count_log_records(data)
        assert count == 2
