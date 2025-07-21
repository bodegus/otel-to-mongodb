"""Tests for OTEL service."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from app.otel_service import OTELService
from app.models import OTELTracesData, OTELMetricsData, OTELLogsData


@pytest.mark.asyncio
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
        """OTEL service instance."""
        return OTELService(mock_mongodb_client)
    
    @pytest.fixture
    def sample_traces_data(self):
        """Sample traces data."""
        return OTELTracesData(
            resourceSpans=[
                {
                    "resource": {"attributes": []},
                    "scopeSpans": [
                        {
                            "scope": {"name": "test-scope"},
                            "spans": [
                                {
                                    "traceId": "0123456789abcdef0123456789abcdef",
                                    "spanId": "0123456789abcdef",
                                    "name": "test-span",
                                    "kind": 1,
                                    "startTimeUnixNano": "1640995200000000000",
                                    "endTimeUnixNano": "1640995201000000000"
                                }
                            ]
                        }
                    ]
                }
            ]
        )
    
    async def test_process_traces_success(self, otel_service, sample_traces_data, mock_mongodb_client):
        """Test successful traces processing."""
        result = await otel_service.process_traces(sample_traces_data, "test-request-123")
        
        assert result.success is True
        assert result.data_type == "traces"
        assert result.records_processed == 1
        assert result.local_storage is True
        assert result.cloud_storage is True
        assert result.document_id == "test_id_123"
        
        # Verify database call
        mock_mongodb_client.write_telemetry_data.assert_called_once()
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "traces"
        assert call_args[1]["request_id"] == "test-request-123"
    
    async def test_process_metrics_success(self, otel_service, mock_mongodb_client):
        """Test successful metrics processing."""
        metrics_data = OTELMetricsData(
            resourceMetrics=[
                {
                    "resource": {"attributes": []},
                    "scopeMetrics": [
                        {
                            "scope": {"name": "test-scope"},
                            "metrics": [
                                {
                                    "name": "test_counter",
                                    "sum": {
                                        "dataPoints": [{"timeUnixNano": "1640995201000000000", "asInt": "42"}],
                                        "aggregationTemporality": 2,
                                        "isMonotonic": True
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        )
        
        result = await otel_service.process_metrics(metrics_data, "test-request-456")
        
        assert result.success is True
        assert result.data_type == "metrics"
        assert result.records_processed == 1
    
    async def test_process_logs_success(self, otel_service, mock_mongodb_client):
        """Test successful logs processing."""
        logs_data = OTELLogsData(
            resourceLogs=[
                {
                    "resource": {"attributes": []},
                    "scopeLogs": [
                        {
                            "scope": {"name": "test-scope"},
                            "logRecords": [
                                {
                                    "timeUnixNano": "1640995200000000000",
                                    "severityNumber": 9,
                                    "body": {"stringValue": "Test log"}
                                }
                            ]
                        }
                    ]
                }
            ]
        )
        
        result = await otel_service.process_logs(logs_data, "test-request-789")
        
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
                        {"logRecords": [{"body": "log1"}]}
                    ]
                }
            ]
        }
        
        count = otel_service._count_log_records(data)
        assert count == 1