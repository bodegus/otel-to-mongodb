"""Tests for main FastAPI application."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch

from app.main import create_app


@pytest.fixture
def mock_mongodb_client():
    """Mock MongoDB client."""
    client = MagicMock()
    client.write_telemetry_data = AsyncMock(return_value={
        "local_success": True,
        "cloud_success": True,
        "document_id": "test_id_123"
    })
    client.health_check = AsyncMock(return_value={
        "local": {"connected": True, "error": None},
        "cloud": {"connected": False, "error": None, "enabled": False}
    })
    return client


@pytest.fixture
def test_app(mock_mongodb_client):
    """Test FastAPI app."""
    app = create_app()
    app.state.mongodb_client = mock_mongodb_client
    return app


@pytest.fixture
def client(test_app):
    """Test client."""
    return TestClient(test_app)


class TestHealthEndpoints:
    """Test health check endpoints."""
    
    def test_basic_health_check(self, client):
        """Test basic health endpoint."""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "otel-to-mongodb-api"
    
    def test_detailed_health_check(self, client, mock_mongodb_client):
        """Test detailed health endpoint."""
        with patch('app.main.get_mongodb_client', return_value=mock_mongodb_client):
            response = client.get("/health/detailed")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert data["local_database"]["connected"] is True
            assert data["cloud_database"]["connected"] is False


class TestTelemetryEndpoints:
    """Test telemetry data endpoints."""
    
    @pytest.fixture
    def sample_traces_data(self):
        """Sample traces data."""
        return {
            "resourceSpans": [
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
        }
    
    def test_submit_traces_success(self, client, sample_traces_data, mock_mongodb_client):
        """Test successful traces submission."""
        with patch('app.main.get_mongodb_client', return_value=mock_mongodb_client):
            response = client.post("/v1/traces", json=sample_traces_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert data["data_type"] == "traces"
            assert data["records_processed"] == 1
            assert data["local_storage"] is True
            assert data["cloud_storage"] is True
    
    def test_submit_traces_validation_error(self, client):
        """Test traces submission with validation error."""
        invalid_data = {"resourceSpans": []}  # Empty array should fail
        
        response = client.post("/v1/traces", json=invalid_data)
        
        assert response.status_code == 422
        assert "validation error" in response.json()["detail"][0]["msg"].lower()
    
    def test_submit_traces_invalid_hex_id(self, client, sample_traces_data):
        """Test traces with invalid hex ID."""
        # Make trace ID invalid
        sample_traces_data["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"] = "invalid_hex"
        
        response = client.post("/v1/traces", json=sample_traces_data)
        
        assert response.status_code == 422
    
    def test_submit_metrics_success(self, client, mock_mongodb_client):
        """Test successful metrics submission."""
        metrics_data = {
            "resourceMetrics": [
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
        }
        
        with patch('app.main.get_mongodb_client', return_value=mock_mongodb_client):
            response = client.post("/v1/metrics", json=metrics_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert data["data_type"] == "metrics"
    
    def test_submit_logs_success(self, client, mock_mongodb_client):
        """Test successful logs submission."""
        logs_data = {
            "resourceLogs": [
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
        }
        
        with patch('app.main.get_mongodb_client', return_value=mock_mongodb_client):
            response = client.post("/v1/logs", json=logs_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert data["data_type"] == "logs"
    
    def test_request_id_header(self, client, sample_traces_data, mock_mongodb_client):
        """Test that request ID is added to headers."""
        with patch('app.main.get_mongodb_client', return_value=mock_mongodb_client):
            response = client.post("/v1/traces", json=sample_traces_data)
            
            assert response.status_code == 200
            assert "X-Request-ID" in response.headers
    
    def test_cors_headers(self, client):
        """Test CORS headers are present."""
        response = client.options("/v1/traces")
        
        assert response.status_code == 200
        assert "access-control-allow-origin" in response.headers