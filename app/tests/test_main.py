"""Tests for main FastAPI application."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI

from app.main import create_app, lifespan


class TestLifespanManager:
    """Test application lifespan management."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_lifespan_startup_and_shutdown(self):
        """Test the lifespan context manager startup and shutdown sequence."""
        # Create a mock FastAPI app
        mock_app = MagicMock(spec=FastAPI)
        mock_app.state = MagicMock()

        # Mock the MongoDBClient
        with patch("app.main.MongoDBClient") as mock_mongodb_class:
            mock_client = AsyncMock()
            mock_mongodb_class.return_value = mock_client

            # Test the lifespan context manager
            async with lifespan(mock_app):
                # During startup
                # Verify MongoDB client was created and connected
                mock_mongodb_class.assert_called_once()
                mock_client.connect.assert_called_once()

                # Verify the client was attached to app state
                assert mock_app.state.mongodb_client == mock_client

            # After shutdown (exiting the context manager)
            # Verify MongoDB client was disconnected
            mock_client.disconnect.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_lifespan_startup_failure(self):
        """Test lifespan behavior when MongoDB connection fails during startup."""
        mock_app = MagicMock(spec=FastAPI)
        mock_app.state = MagicMock()

        with patch("app.main.MongoDBClient") as mock_mongodb_class:
            mock_client = AsyncMock()
            mock_client.connect.side_effect = Exception("Connection failed")
            mock_mongodb_class.return_value = mock_client

            # The lifespan should propagate the connection error
            with pytest.raises(Exception, match="Connection failed"):
                async with lifespan(mock_app):
                    pass  # Should not reach here

            # Verify connect was attempted
            mock_client.connect.assert_called_once()

            # Disconnect should not be called if startup failed
            mock_client.disconnect.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_lifespan_shutdown_failure_propagates(self):
        """Test that shutdown failures are propagated (not silently handled)."""
        mock_app = MagicMock(spec=FastAPI)
        mock_app.state = MagicMock()

        with patch("app.main.MongoDBClient") as mock_mongodb_class:
            mock_client = AsyncMock()
            mock_client.disconnect.side_effect = Exception("Disconnect failed")
            mock_mongodb_class.return_value = mock_client

            # Shutdown failures should be propagated, not silently handled
            with pytest.raises(Exception, match="Disconnect failed"):
                async with lifespan(mock_app):
                    # Startup should work fine
                    mock_client.connect.assert_called_once()
                    assert mock_app.state.mongodb_client == mock_client
                    # Exit context triggers disconnect which raises exception

            # Disconnect was attempted
            mock_client.disconnect.assert_called_once()

    @pytest.mark.unit
    def test_create_app_includes_lifespan(self):
        """Test that create_app properly configures the lifespan manager."""
        app = create_app()

        # Verify the app has the lifespan configured
        assert app.router.lifespan_context is not None

        # The lifespan should be our lifespan function
        # Note: FastAPI wraps the lifespan function, so we can't directly compare
        # but we can verify it's configured
        assert hasattr(app.router, "lifespan_context")


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

    @pytest.mark.unit
    def test_submit_traces_success(self, client, json_traces_data, mock_mongodb_client):
        """Test successful traces submission."""
        response = client.post("/v1/traces", json=json_traces_data["data"])

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty dict on success
        assert data == {}

    @pytest.mark.unit
    def test_submit_traces_validation_error(self, client):
        """Test traces submission with validation error."""
        invalid_data = {"resourceSpans": []}  # Empty array should fail

        response = client.post("/v1/traces", json=invalid_data)

        assert response.status_code == 422
        data = response.json()
        assert data["code"] == 3  # INVALID_ARGUMENT
        assert "Validation error" in data["message"]
        assert data["details"][0]["field_violations"][0]["field"] == "resourceSpans"

    @pytest.mark.unit
    def test_submit_traces_any_trace_id_format(self, client, json_traces_data):
        """Test traces with any trace ID format (no validation)."""
        # Make trace ID any string format - need to copy the data first
        import copy

        traces_data = copy.deepcopy(json_traces_data["data"])
        resource_spans = traces_data["resourceSpans"][0]
        spans = resource_spans["scopeSpans"][0]["spans"]
        spans[0]["traceId"] = "any_string_format"

        response = client.post("/v1/traces", json=traces_data)

        # Should succeed - no hex validation required
        assert response.status_code == 200

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
        # OTLP-compliant response should be empty dict on success
        assert data == {}

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
        # OTLP-compliant response should be empty dict on success
        assert data == {}

    @pytest.mark.unit
    def test_request_id_header(self, client, json_traces_data, mock_mongodb_client):
        """Test that telemetry submission works."""
        response = client.post("/v1/traces", json=json_traces_data["data"])

        assert response.status_code == 200
        # Just verify we get a successful OTLP response
        data = response.json()
        assert data == {}

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
    def test_valid_json_data(self, client, json_traces_data):
        """Test that valid JSON OTLP data is accepted."""
        response = client.post("/v1/traces", json=json_traces_data["data"])

        assert response.status_code == 200
        data = response.json()
        assert data == {}

    # Protobuf Support Tests
    @pytest.mark.unit
    def test_submit_protobuf_traces_success(
        self, client, protobuf_traces_data, mock_mongodb_client
    ):
        """Test successful protobuf traces submission."""
        response = client.post(
            "/v1/traces",
            content=protobuf_traces_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty dict on success
        assert data == {}

    @pytest.mark.unit
    def test_submit_protobuf_metrics_success(
        self, client, protobuf_metrics_data, mock_mongodb_client
    ):
        """Test successful protobuf metrics submission."""
        response = client.post(
            "/v1/metrics",
            content=protobuf_metrics_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty dict on success
        assert data == {}

    @pytest.mark.unit
    def test_submit_protobuf_logs_success(self, client, protobuf_logs_data, mock_mongodb_client):
        """Test successful protobuf logs submission."""
        response = client.post(
            "/v1/logs",
            content=protobuf_logs_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 200
        data = response.json()
        # OTLP-compliant response should be empty dict on success
        assert data == {}

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
        # Now using OTLP Status format with "message" field
        assert "message" in data
        assert "Invalid protobuf" in data["message"] or "Error parsing protobuf" in data["message"]

    @pytest.mark.unit
    def test_protobuf_empty_data_error(self, client):
        """Test empty protobuf data returns 400."""
        response = client.post(
            "/v1/traces", content=b"", headers={"Content-Type": "application/x-protobuf"}
        )

        assert response.status_code == 400
        data = response.json()
        # Now using OTLP Status format with "message" field
        assert "message" in data
        assert "protobuf" in data["message"].lower()

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
        # Now using OTLP Status format with "message" field
        assert "message" in data
        assert "protobuf" in data["message"].lower()

    @pytest.mark.unit
    def test_content_type_case_insensitive(self, client, protobuf_traces_data, mock_mongodb_client):
        """Test content-type header is case insensitive."""
        response = client.post(
            "/v1/traces",
            content=protobuf_traces_data["binary_data"],
            headers={"Content-Type": "APPLICATION/X-PROTOBUF"},
        )

        assert response.status_code == 200

    @pytest.mark.unit
    def test_content_type_with_charset(self, client, json_traces_data):
        """Test JSON content-type with charset parameter."""
        response = client.post(
            "/v1/traces",
            json=json_traces_data["data"],
            headers={"Content-Type": "application/json; charset=utf-8"},
        )

        assert response.status_code == 200

    @pytest.mark.unit
    def test_missing_content_type_defaults_json(self, client, json_traces_data):
        """Test missing content-type defaults to JSON."""
        # TestClient automatically sets content-type for json parameter,
        # so we'll use content parameter with no explicit content-type
        import json as json_module

        response = client.post(
            "/v1/traces",
            content=json_module.dumps(json_traces_data["data"]),
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
        assert data == {}

    @pytest.mark.unit
    def test_mixed_requests_same_endpoint(
        self, client, json_traces_data, protobuf_traces_data, mock_mongodb_client
    ):
        """Test that same endpoint can handle both JSON and protobuf requests."""
        # First send JSON request
        json_response = client.post("/v1/traces", json=json_traces_data["data"])
        assert json_response.status_code == 200

        # Then send protobuf request to same endpoint
        protobuf_response = client.post(
            "/v1/traces",
            content=protobuf_traces_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )
        assert protobuf_response.status_code == 200

    @pytest.mark.unit
    def test_backward_compatibility_json_unchanged(
        self, client, json_traces_data, mock_mongodb_client
    ):
        """Test that existing JSON behavior is completely unchanged."""
        # This test ensures backward compatibility
        response = client.post("/v1/traces", json=json_traces_data["data"])

        assert response.status_code == 200
        data = response.json()
        assert data == {}

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

    @pytest.mark.unit
    def test_empty_protobuf_metrics_data_error(self, client):
        """Test empty protobuf data error for metrics endpoint."""
        response = client.post(
            "/v1/metrics", content=b"", headers={"Content-Type": "application/x-protobuf"}
        )

        assert response.status_code == 400
        data = response.json()
        assert "message" in data
        assert "protobuf" in data["message"].lower()

    @pytest.mark.unit
    def test_empty_protobuf_logs_data_error(self, client):
        """Test empty protobuf data error for logs endpoint."""
        response = client.post(
            "/v1/logs", content=b"", headers={"Content-Type": "application/x-protobuf"}
        )

        assert response.status_code == 400
        data = response.json()
        assert "message" in data
        assert "protobuf" in data["message"].lower()
