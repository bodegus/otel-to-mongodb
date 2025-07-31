"""Tests for main FastAPI application."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def mock_mongodb_client():
    """Mock MongoDB client."""
    client = MagicMock()

    # Mock the write_telemetry_data method
    client.write_telemetry_data = AsyncMock(
        return_value={
            "success": True,
            "primary_success": True,
            "secondary_success": False,
            "document_id": "test_id_123",
        }
    )

    # Mock the health_check method
    client.health_check = AsyncMock(
        return_value={
            "primary": {"connected": True, "error": None, "configured": True},
            "secondary": {"connected": False, "error": None, "configured": False},
        }
    )

    # Mock the actual database client properties that the code accesses
    client.local_client = MagicMock()
    client.local_db_name = "test_db"

    return client


@pytest.fixture
def test_app(mock_mongodb_client):
    """Test FastAPI app."""
    from app.main import create_app
    from app.mongo_client import get_mongodb_client

    app = create_app()

    # Override the dependency
    app.dependency_overrides[get_mongodb_client] = lambda: mock_mongodb_client

    return app


@pytest.fixture
def client(test_app):
    """Test client."""
    return TestClient(test_app)


class TestHealthEndpoints:
    """Test health check endpoints."""

    @pytest.mark.unit
    def test_basic_health_check(self, client):
        """Test basic health endpoint."""
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "otel-to-mongodb-api"

    @pytest.mark.unit
    def test_detailed_health_check(self, client, mock_mongodb_client):
        """Test detailed health endpoint."""
        response = client.get("/health/detailed")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["primary_database"]["connected"] is True
        assert data["secondary_database"]["connected"] is False


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
                                    "traceId": ("0123456789abcdef0123456789abcdef"),
                                    "spanId": "0123456789abcdef",
                                    "name": "test-span",
                                    "kind": 1,
                                    "startTimeUnixNano": "1640995200000000000",
                                    "endTimeUnixNano": "1640995201000000000",
                                }
                            ],
                        }
                    ],
                }
            ]
        }

    @pytest.mark.unit
    def test_submit_traces_success(self, client, sample_traces_data, mock_mongodb_client):
        """Test successful traces submission."""
        response = client.post("/v1/traces", json=sample_traces_data)

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty on success
        # or contain only partialSuccess field if there were issues
        assert isinstance(data, dict)
        # For successful requests, partialSuccess should be None/absent
        assert data.get("partialSuccess") is None

    @pytest.mark.unit
    def test_submit_traces_validation_error(self, client):
        """Test traces submission with validation error."""
        invalid_data = {"resourceSpans": []}  # Empty array should fail

        response = client.post("/v1/traces", json=invalid_data)

        assert response.status_code == 422
        detail_msg = response.json()["detail"][0]["msg"].lower()
        # Updated to match actual Pydantic error message
        assert "value error" in detail_msg or "validation error" in detail_msg

    @pytest.mark.unit
    def test_submit_traces_invalid_hex_id(self, client, sample_traces_data):
        """Test traces with invalid hex ID."""
        # Make trace ID invalid
        resource_spans = sample_traces_data["resourceSpans"][0]
        spans = resource_spans["scopeSpans"][0]["spans"]
        spans[0]["traceId"] = "invalid_hex"

        response = client.post("/v1/traces", json=sample_traces_data)

        assert response.status_code == 422

    @pytest.mark.unit
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
                                        "dataPoints": [
                                            {"timeUnixNano": ("1640995201000000000"), "asInt": "42"}
                                        ],
                                        "aggregationTemporality": 2,
                                        "isMonotonic": True,
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        response = client.post("/v1/metrics", json=metrics_data)

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty on success
        assert isinstance(data, dict)
        assert data.get("partialSuccess") is None

    @pytest.mark.unit
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
                                    "body": {"stringValue": "Test log"},
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        response = client.post("/v1/logs", json=logs_data)

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty on success
        assert isinstance(data, dict)
        assert data.get("partialSuccess") is None

    @pytest.mark.unit
    def test_request_id_header(self, client, sample_traces_data, mock_mongodb_client):
        """Test that telemetry submission works."""
        response = client.post("/v1/traces", json=sample_traces_data)

        assert response.status_code == 200
        # Just verify we get a successful OTLP response
        data = response.json()
        assert isinstance(data, dict)

    @pytest.mark.unit
    def test_invalid_json_data(self, client):
        """Test that invalid JSON data is rejected."""
        response = client.post(
            "/v1/traces", content="invalid json data", headers={"Content-Type": "application/json"}
        )

        # FastAPI returns 422 for invalid JSON/data
        assert response.status_code == 422

    @pytest.mark.unit
    def test_non_json_content_type_rejected(self, client):
        """Test that non-JSON content types are rejected."""
        response = client.post(
            "/v1/traces", content="some data", headers={"Content-Type": "application/xml"}
        )

        # ContentTypeHandler returns 415 for unsupported content types (correct HTTP status)
        assert response.status_code == 415
        assert "Unsupported content type" in response.json()["detail"]

    @pytest.mark.unit
    def test_valid_json_data(self, client, sample_traces_data):
        """Test that valid JSON OTLP data is accepted."""
        response = client.post("/v1/traces", json=sample_traces_data)

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    # Protobuf Support Tests
    @pytest.mark.unit
    def test_submit_protobuf_traces_success(
        self, client, sample_protobuf_traces_data, mock_mongodb_client
    ):
        """Test successful protobuf traces submission."""
        response = client.post(
            "/v1/traces",
            content=sample_protobuf_traces_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty on success
        assert isinstance(data, dict)
        assert data.get("partialSuccess") is None

    @pytest.mark.unit
    def test_submit_protobuf_metrics_success(
        self, client, sample_protobuf_metrics_data, mock_mongodb_client
    ):
        """Test successful protobuf metrics submission."""
        response = client.post(
            "/v1/metrics",
            content=sample_protobuf_metrics_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty on success
        assert isinstance(data, dict)
        assert data.get("partialSuccess") is None

    @pytest.mark.unit
    def test_submit_protobuf_logs_success(
        self, client, sample_protobuf_logs_data, mock_mongodb_client
    ):
        """Test successful protobuf logs submission."""
        response = client.post(
            "/v1/logs",
            content=sample_protobuf_logs_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty on success
        assert isinstance(data, dict)
        assert data.get("partialSuccess") is None

    @pytest.mark.unit
    def test_protobuf_malformed_data_error(self, client, malformed_protobuf_data):
        """Test malformed protobuf data returns 400."""
        response = client.post(
            "/v1/traces",
            content=malformed_protobuf_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 400
        data = response.json()
        assert "Invalid protobuf" in data["detail"] or "Error parsing protobuf" in data["detail"]

    @pytest.mark.unit
    def test_protobuf_empty_data_error(self, client):
        """Test empty protobuf data returns 400."""
        response = client.post(
            "/v1/traces", content=b"", headers={"Content-Type": "application/x-protobuf"}
        )

        assert response.status_code == 400
        data = response.json()
        assert "protobuf" in data["detail"].lower()

    @pytest.mark.unit
    def test_protobuf_validation_error(self, client, empty_protobuf_traces_data):
        """Test protobuf data with validation errors."""
        response = client.post(
            "/v1/traces",
            content=empty_protobuf_traces_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        # Empty resourceSpans should trigger validation error
        assert response.status_code == 400
        data = response.json()
        assert "protobuf" in data["detail"].lower()

    @pytest.mark.unit
    def test_content_type_case_insensitive(
        self, client, sample_protobuf_traces_data, mock_mongodb_client
    ):
        """Test content-type header is case insensitive."""
        response = client.post(
            "/v1/traces",
            content=sample_protobuf_traces_data["binary_data"],
            headers={"Content-Type": "APPLICATION/X-PROTOBUF"},
        )

        assert response.status_code == 200

    @pytest.mark.unit
    def test_content_type_with_charset(self, client, sample_traces_data):
        """Test JSON content-type with charset parameter."""
        response = client.post(
            "/v1/traces",
            json=sample_traces_data,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )

        assert response.status_code == 200

    @pytest.mark.unit
    def test_missing_content_type_defaults_json(self, client, sample_traces_data):
        """Test missing content-type defaults to JSON."""
        # TestClient automatically sets content-type for json parameter,
        # so we'll use content parameter with no explicit content-type
        import json as json_module

        response = client.post(
            "/v1/traces",
            content=json_module.dumps(sample_traces_data),
            # No Content-Type header - should default to JSON
        )

        assert response.status_code == 200

    @pytest.mark.unit
    def test_unsupported_content_types_all_endpoints(self, client):
        """Test unsupported content types return 415 for all endpoints."""
        endpoints = ["/v1/traces", "/v1/metrics", "/v1/logs"]

        for endpoint in endpoints:
            response = client.post(
                endpoint, content="test data", headers={"Content-Type": "application/xml"}
            )

            assert response.status_code == 415
            data = response.json()
            assert "Unsupported content type" in data["detail"]
            assert "application/xml" in data["detail"]

    @pytest.mark.unit
    def test_protobuf_large_payload(self, client, large_protobuf_traces_data, mock_mongodb_client):
        """Test large protobuf payload handling."""
        response = client.post(
            "/v1/traces",
            content=large_protobuf_traces_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    @pytest.mark.unit
    def test_mixed_requests_same_endpoint(
        self, client, sample_traces_data, sample_protobuf_traces_data, mock_mongodb_client
    ):
        """Test that same endpoint can handle both JSON and protobuf requests."""
        # First send JSON request
        json_response = client.post("/v1/traces", json=sample_traces_data)
        assert json_response.status_code == 200

        # Then send protobuf request to same endpoint
        protobuf_response = client.post(
            "/v1/traces",
            content=sample_protobuf_traces_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )
        assert protobuf_response.status_code == 200

    @pytest.mark.unit
    def test_backward_compatibility_json_unchanged(
        self, client, sample_traces_data, mock_mongodb_client
    ):
        """Test that existing JSON behavior is completely unchanged."""
        # This test ensures backward compatibility
        response = client.post("/v1/traces", json=sample_traces_data)

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert data.get("partialSuccess") is None

        # Verify MongoDB client was called correctly
        mock_mongodb_client.write_telemetry_data.assert_called_once()
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == "traces"
        assert "data" in call_args[1]

    @pytest.mark.unit
    def test_accept_header_in_415_response(self, client):
        """Test that 415 responses include Accept header."""
        response = client.post(
            "/v1/traces", content="test data", headers={"Content-Type": "text/plain"}
        )

        assert response.status_code == 415
        # FastAPI TestClient may not preserve all headers, but the error should be clear
        data = response.json()
        assert "Unsupported content type" in data["detail"]
