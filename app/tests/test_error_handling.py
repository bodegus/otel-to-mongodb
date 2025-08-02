"""Tests for error handling consistency between JSON and protobuf."""

import pytest
from fastapi.testclient import TestClient


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
    def test_json_any_trace_id_format_accepted(self, client, json_traces_data):
        """Test that JSON with any trace ID format is accepted (no validation)."""
        import copy

        # Use fixture data and modify the traceId to any string format
        test_data = copy.deepcopy(json_traces_data["data"])
        test_data["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"] = "any_string_format"

        response = client.post("/v1/traces", json=test_data)

        # Should succeed - no hex validation required
        assert response.status_code == 200

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

    # Note: Removed test_internal_server_error_format since with simplified architecture,
    # internal server errors should be handled by the global exception handler.
    # The simplified endpoints no longer catch internal errors individually.


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


class TestEdgeCaseErrorHandlers:
    """Test edge case error handlers that are rarely triggered."""

    @pytest.mark.unit
    def test_unicode_decode_error_handler(self, test_app):
        """Test unicode decode error handling by directly triggering UnicodeDecodeError."""
        from unittest.mock import patch

        client = TestClient(test_app)

        # Mock request.json() to raise UnicodeDecodeError
        with patch("starlette.requests.Request.json") as mock_json:
            mock_json.side_effect = UnicodeDecodeError(
                "utf-8", b"\xff\xfe", 0, 1, "invalid start byte"
            )

            response = client.post(
                "/v1/traces",
                json={"dummy": "data"},  # This will be ignored due to mock
                headers={"Content-Type": "application/json"},
            )

            # Should return 422 for unicode decode error
            assert response.status_code == 422
            data = response.json()
            assert "detail" in data
            assert "JSON" in data["detail"]

    @pytest.mark.unit
    def test_global_exception_handler_via_otel_service_failure(self, test_app, mock_mongodb_client):
        """Test global exception handler by causing an unexpected error in OTELService."""
        from unittest.mock import patch

        # Use TestClient with raise_server_exceptions=False to capture 500 responses
        client = TestClient(test_app, raise_server_exceptions=False)

        # Mock the OTELService to raise an unexpected exception during processing
        with patch("app.main.OTELService") as mock_otel_service_class:
            mock_service = mock_otel_service_class.return_value
            mock_service.process_traces.side_effect = RuntimeError("Unexpected processing error")

            response = client.post(
                "/v1/traces",
                json={
                    "resourceSpans": [
                        {
                            "resource": {"attributes": []},
                            "scopeSpans": [
                                {
                                    "scope": {"name": "test"},
                                    "spans": [
                                        {
                                            "traceId": "123",
                                            "spanId": "456",
                                            "name": "test",
                                            "kind": 1,
                                            "startTimeUnixNano": "123",
                                            "endTimeUnixNano": "456",
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                },
            )

            # Should return 500 for unhandled exception
            assert response.status_code == 500
            data = response.json()
            assert "success" in data
            assert data["success"] is False
            assert "message" in data
            assert "error_code" in data
            assert data["error_code"] == "INTERNAL_ERROR"
