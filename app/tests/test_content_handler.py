"""Unit tests for ContentTypeHandler module."""

import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException, Request

from app.content_handler import ContentTypeHandler
from app.models import OTELLogsData, OTELMetricsData, OTELTracesData


class TestContentTypeHandler:
    """Test suite for ContentTypeHandler class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = ContentTypeHandler()

    async def test_parse_json_traces_success(self, sample_traces_data):
        """Test successful JSON traces parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=sample_traces_data["data"])

        # Act
        result = await self.handler.parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        assert len(result.resource_spans) == 1
        assert result.resource_spans[0].resource.attributes[0].key == "service.name"

    async def test_parse_json_metrics_success(self, sample_metrics_data):
        """Test successful JSON metrics parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=sample_metrics_data["data"])

        # Act
        result = await self.handler.parse_request_data(request, "metrics")

        # Assert
        assert isinstance(result, OTELMetricsData)
        assert len(result.resource_metrics) == 1

    async def test_parse_json_logs_success(self, sample_logs_data):
        """Test successful JSON logs parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=sample_logs_data["data"])

        # Act
        result = await self.handler.parse_request_data(request, "logs")

        # Assert
        assert isinstance(result, OTELLogsData)
        assert len(result.resource_logs) == 1

    async def test_parse_protobuf_traces_success(self, sample_protobuf_traces_data):
        """Test successful protobuf traces parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_traces_data["binary_data"])

        # Act
        result = await self.handler.parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        assert len(result.resource_spans) == 1

    async def test_parse_protobuf_metrics_success(self, sample_protobuf_metrics_data):
        """Test successful protobuf metrics parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_metrics_data["binary_data"])

        # Act
        result = await self.handler.parse_request_data(request, "metrics")

        # Assert
        assert isinstance(result, OTELMetricsData)
        assert len(result.resource_metrics) == 1

    async def test_parse_protobuf_logs_success(self, sample_protobuf_logs_data):
        """Test successful protobuf logs parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_logs_data["binary_data"])

        # Act
        result = await self.handler.parse_request_data(request, "logs")

        # Assert
        assert isinstance(result, OTELLogsData)
        assert len(result.resource_logs) == 1

    async def test_unsupported_content_type_error(self):
        """Test HTTP 415 for unsupported content types."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/xml"}

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 415
        assert "Unsupported content type" in exc_info.value.detail
        assert "application/xml" in exc_info.value.detail
        assert exc_info.value.headers == {"Accept": "application/json, application/x-protobuf"}

    async def test_missing_content_type_defaults_to_json(self, sample_traces_data):
        """Test missing content-type defaults to JSON."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {}  # No content-type header
        request.json = AsyncMock(return_value=sample_traces_data["data"])

        # Act
        result = await self.handler.parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        request.json.assert_called_once()

    async def test_empty_content_type_defaults_to_json(self, sample_traces_data):
        """Test empty content-type defaults to JSON."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": ""}
        request.json = AsyncMock(return_value=sample_traces_data["data"])

        # Act
        result = await self.handler.parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        request.json.assert_called_once()

    async def test_content_type_with_charset_normalized(self, sample_traces_data):
        """Test content-type with charset is normalized to base type."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json; charset=utf-8"}
        request.json = AsyncMock(return_value=sample_traces_data["data"])

        # Act
        result = await self.handler.parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        request.json.assert_called_once()

    async def test_content_type_case_insensitive(self, sample_traces_data):
        """Test content-type header is case insensitive."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "APPLICATION/JSON"}
        request.json = AsyncMock(return_value=sample_traces_data["data"])

        # Act
        result = await self.handler.parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        request.json.assert_called_once()

    async def test_json_validation_error_returns_422(self):
        """Test JSON validation error returns HTTP 422."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value={"resourceSpans": []})  # Invalid - empty

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 422
        assert isinstance(exc_info.value.detail, list)  # Pydantic validation errors

    async def test_json_decode_error_returns_422(self):
        """Test JSON decode error returns HTTP 422."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(side_effect=ValueError("Invalid JSON"))

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 422
        assert "Invalid JSON" in exc_info.value.detail

    async def test_json_unicode_decode_error_returns_422(self):
        """Test JSON unicode decode error returns HTTP 422."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(side_effect=UnicodeDecodeError("utf-8", b"", 0, 1, "invalid"))

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 422
        assert "Invalid JSON" in exc_info.value.detail

    async def test_json_unexpected_error_returns_400(self):
        """Test unexpected JSON parsing error returns HTTP 400."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(side_effect=RuntimeError("Unexpected error"))

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 400
        assert "Invalid JSON data" in exc_info.value.detail

    async def test_protobuf_parsing_error_returns_400(self):
        """Test protobuf parsing error returns HTTP 400."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=b"invalid_protobuf_data")

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 400
        assert (
            "Invalid protobuf" in exc_info.value.detail
            or "Error parsing protobuf" in exc_info.value.detail
        )

    async def test_protobuf_empty_data_error(self):
        """Test protobuf empty data returns HTTP 400."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=b"")

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 400

    async def test_protobuf_unexpected_error_returns_400(self):
        """Test unexpected protobuf parsing error returns HTTP 400."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(side_effect=RuntimeError("Unexpected error"))

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 400
        assert "Invalid protobuf data" in exc_info.value.detail

    async def test_invalid_data_type_raises_value_error(self, sample_traces_data):
        """Test invalid data type raises ValueError and returns 422."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=sample_traces_data["data"])

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "invalid_type")

        # ValueError is caught as JSON parsing error and returns 422
        assert exc_info.value.status_code == 422
        assert "Unknown data type" in exc_info.value.detail

    def test_get_content_type_with_header(self):
        """Test _get_content_type with explicit header."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}

        # Act
        result = self.handler._get_content_type(request)

        # Assert
        assert result == "application/json"

    def test_get_content_type_without_header(self):
        """Test _get_content_type defaults to JSON when no header."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {}

        # Act
        result = self.handler._get_content_type(request)

        # Assert
        assert result == "application/json"

    def test_get_content_type_with_parameters(self):
        """Test _get_content_type strips parameters."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json; charset=utf-8; boundary=something"}

        # Act
        result = self.handler._get_content_type(request)

        # Assert
        assert result == "application/json"

    def test_get_content_type_case_normalization(self):
        """Test _get_content_type normalizes case."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "APPLICATION/X-PROTOBUF"}

        # Act
        result = self.handler._get_content_type(request)

        # Assert
        assert result == "application/x-protobuf"

    def test_get_content_type_with_whitespace(self):
        """Test _get_content_type handles whitespace."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "  application/json  "}

        # Act
        result = self.handler._get_content_type(request)

        # Assert
        assert result == "application/json"

    def test_create_unsupported_media_type_response(self):
        """Test create_unsupported_media_type_response method."""
        # Act
        response = self.handler.create_unsupported_media_type_response("application/xml")

        # Assert
        assert response.status_code == 415
        assert "Unsupported media type" in json.loads(response.body.decode())["message"]
        assert response.headers["Accept"] == "application/json, application/x-protobuf"

    async def test_validation_error_serialization_with_context(self):
        """Test validation error with context objects are properly serialized."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}

        # Create invalid traces data that will trigger validation with context
        invalid_data = {
            "resourceSpans": [
                {
                    "resource": {"attributes": []},
                    "scopeSpans": [
                        {
                            "scope": {"name": "test"},
                            "spans": [
                                {
                                    "traceId": "invalid_hex",  # This will trigger hex validation
                                    "spanId": "1234567890abcdef",
                                    "name": "test",
                                    "kind": 1,
                                    "startTimeUnixNano": "1234567890",
                                    "endTimeUnixNano": "1234567891",
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        request.json = AsyncMock(return_value=invalid_data)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await self.handler.parse_request_data(request, "traces")

        # Should be 422 for validation error
        assert exc_info.value.status_code == 422
        # Detail should be serializable (no context objects that can't be JSON serialized)
        assert isinstance(exc_info.value.detail, list)

        # Check that all error details are JSON serializable
        json.dumps(exc_info.value.detail)  # Should not raise exception

    async def test_content_type_detection_logging(self, sample_traces_data):
        """Test that content type detection includes proper logging."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=sample_traces_data["data"])

        # Act
        with patch("app.content_handler.logger") as mock_logger:
            await self.handler.parse_request_data(request, "traces")

        # Assert
        mock_logger.info.assert_called_with(
            "Processing request with content type",
            content_type="application/json",
            data_type="traces",
        )

    async def test_unsupported_content_type_logging(self):
        """Test that unsupported content types are logged as warnings."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/xml"}

        # Act & Assert
        with patch("app.content_handler.logger") as mock_logger:
            with pytest.raises(HTTPException):
                await self.handler.parse_request_data(request, "traces")

        mock_logger.warning.assert_called_with(
            "Unsupported content type", content_type="application/xml", data_type="traces"
        )
