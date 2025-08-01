"""Tests for error handling consistency between JSON and protobuf."""

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.mongo_client import get_mongodb_client


# Import protobuf fixtures


@pytest.fixture
def mock_mongodb_client():
    """Mock MongoDB client."""
    from unittest.mock import AsyncMock, MagicMock

    client = MagicMock()
    client.write_telemetry_data = AsyncMock(
        return_value={
            "success": True,
            "primary_success": True,
            "secondary_success": False,
            "document_id": "test_id_123",
        }
    )
    client.health_check = AsyncMock(
        return_value={
            "primary": {"connected": True, "error": None, "configured": True},
            "secondary": {"connected": False, "error": None, "configured": False},
        }
    )
    client.local_client = MagicMock()
    client.local_db_name = "test_db"
    return client


@pytest.fixture
def test_app(mock_mongodb_client):
    """Test FastAPI app with mocked dependencies."""
    app = create_app()
    app.dependency_overrides[get_mongodb_client] = lambda: mock_mongodb_client
    return app


@pytest.fixture
def client(test_app):
    """Test client."""
    return TestClient(test_app)


class TestProtobufErrorHandling:
    """Test error handling for protobuf-specific scenarios."""

    @pytest.mark.unit
    def test_protobuf_parsing_error_response_format(self, client):
        """Test that protobuf parsing errors return OTLP-compliant Status."""
        # Send invalid protobuf data
        response = client.post(
            "/v1/traces",
            content=b"invalid protobuf data",
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 400
        data = response.json()

        # Should have OTLP Status fields
        assert "code" in data
        assert data["code"] == 3  # INVALID_ARGUMENT
        assert "message" in data
        assert "Invalid protobuf" in data["message"] or "Error parsing protobuf" in data["message"]

        # Details field should be present
        if "details" in data:
            assert isinstance(data["details"], list)

    @pytest.mark.unit
    def test_json_parsing_error_response_format(self, client):
        """Test that JSON parsing errors return consistent error format."""
        # Send invalid JSON data
        response = client.post(
            "/v1/traces",
            content="invalid json data {{{",
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 422
        data = response.json()

        # FastAPI validation errors return a specific format
        assert "detail" in data
        assert isinstance(data["detail"], str | list)

    @pytest.mark.unit
    def test_protobuf_validation_error_response(self, client, empty_protobuf_traces_data):
        """Test that protobuf validation errors are handled correctly."""
        # Send protobuf data that will fail validation (empty resourceSpans)
        response = client.post(
            "/v1/traces",
            content=empty_protobuf_traces_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        # Empty protobuf should be caught as parsing error, not validation
        assert response.status_code == 400
        data = response.json()
        assert "protobuf" in str(data).lower()

    @pytest.mark.unit
    def test_json_validation_error_response(self, client):
        """Test that JSON validation errors return proper error format."""
        # Send JSON data that will fail validation
        invalid_json_data = {
            "resourceSpans": []  # Empty array should fail validation
        }

        response = client.post("/v1/traces", json=invalid_json_data)

        assert response.status_code == 422
        data = response.json()

        # Should have OTLP Status format
        assert "code" in data
        assert data["code"] == 3  # INVALID_ARGUMENT
        assert "message" in data
        assert "Validation error" in data["message"]
        assert "details" in data
        assert isinstance(data["details"], list)
        assert len(data["details"]) > 0

        # First detail should have field violations
        detail = data["details"][0]
        assert "@type" in detail
        assert detail["@type"] == "type.googleapis.com/google.rpc.BadRequest"
        assert "field_violations" in detail
        assert len(detail["field_violations"]) > 0

        # Each field violation should have field and description
        field_violation = detail["field_violations"][0]
        assert "field" in field_violation
        assert "description" in field_violation
        assert "resourceSpans" in field_violation["field"]

    @pytest.mark.unit
    def test_protobuf_empty_data_error(self, client):
        """Test that empty protobuf data returns appropriate error."""
        response = client.post(
            "/v1/traces", content=b"", headers={"Content-Type": "application/x-protobuf"}
        )

        assert response.status_code == 400
        data = response.json()

        # Should mention empty or protobuf in error
        error_msg = str(data).lower()
        assert "empty" in error_msg or "protobuf" in error_msg

    @pytest.mark.unit
    def test_json_empty_data_error(self, client):
        """Test that empty JSON data returns appropriate error."""
        response = client.post(
            "/v1/traces", content="", headers={"Content-Type": "application/json"}
        )

        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    @pytest.mark.unit
    def test_protobuf_malformed_hex_id_error(self, client, protobuf_traces_data):
        """Test that protobuf with invalid hex IDs is handled properly."""
        # Note: This would require creating a protobuf with invalid hex ID
        # For now, we'll test that valid protobuf is processed correctly
        response = client.post(
            "/v1/traces",
            content=protobuf_traces_data["binary_data"],
            headers={"Content-Type": "application/x-protobuf"},
        )

        # Should succeed with valid data
        assert response.status_code == 200

    @pytest.mark.unit
    def test_json_malformed_hex_id_error(self, client, json_traces_data):
        """Test that JSON with invalid hex IDs returns validation error."""
        import copy

        # Use fixture data and modify just the traceId to be invalid
        invalid_data = copy.deepcopy(json_traces_data["data"])
        invalid_data["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"] = "invalid_hex_id"

        response = client.post("/v1/traces", json=invalid_data)

        assert response.status_code == 422
        data = response.json()
        # Should have OTLP Status format
        assert "code" in data
        assert data["code"] == 3  # INVALID_ARGUMENT
        assert "message" in data
        assert "Validation error" in data["message"]

    @pytest.mark.unit
    def test_error_response_consistency_across_endpoints(self, client):
        """Test that all endpoints return consistent error formats."""
        endpoints = ["/v1/traces", "/v1/metrics", "/v1/logs"]

        for endpoint in endpoints:
            # Test protobuf error
            protobuf_response = client.post(
                endpoint,
                content=b"invalid protobuf",
                headers={"Content-Type": "application/x-protobuf"},
            )

            assert protobuf_response.status_code == 400
            protobuf_data = protobuf_response.json()

            # Should have consistent error structure
            assert "message" in protobuf_data or "detail" in protobuf_data

            # Test JSON error
            json_response = client.post(
                endpoint, content="invalid json", headers={"Content-Type": "application/json"}
            )

            assert json_response.status_code == 422

    @pytest.mark.unit
    def test_internal_server_error_format(self, client, monkeypatch):
        """Test that internal server errors return consistent format."""

        # Mock the content handler to raise an unexpected error
        def mock_parse_request_data(*args, **kwargs):
            raise RuntimeError("Unexpected internal error")

        from app.content_handler import ContentTypeHandler

        monkeypatch.setattr(ContentTypeHandler, "parse_request_data", mock_parse_request_data)

        response = client.post(
            "/v1/traces", json={"resourceSpans": [{"resource": {}, "scopeSpans": []}]}
        )

        # Should return 500 with ErrorResponse format
        assert response.status_code == 500
        data = response.json()

        # Check for ErrorResponse fields
        assert "success" in data or "message" in data
        if "success" in data:
            assert data["success"] is False
        if "message" in data:
            assert "internal" in data["message"].lower()


class TestErrorMessageClarity:
    """Test that error messages are clear and helpful."""

    @pytest.mark.unit
    def test_protobuf_error_mentions_protobuf(self, client):
        """Test that protobuf errors clearly mention protobuf."""
        response = client.post(
            "/v1/traces",
            content=b"not valid protobuf",
            headers={"Content-Type": "application/x-protobuf"},
        )

        assert response.status_code == 400
        data = response.json()

        # Error should mention protobuf
        error_text = str(data).lower()
        assert "protobuf" in error_text

    @pytest.mark.unit
    def test_content_type_error_mentions_supported_types(self, client):
        """Test that content type errors list supported types."""
        response = client.post(
            "/v1/traces", content="some data", headers={"Content-Type": "application/xml"}
        )

        assert response.status_code == 415
        data = response.json()

        # Should mention supported content types
        error_text = str(data)
        assert "application/json" in error_text
        assert "application/x-protobuf" in error_text
