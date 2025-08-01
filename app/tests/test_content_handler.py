"""Unit tests for request data parsing."""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException, Request
from pydantic import ValidationError

from app.content_handler import parse_request_data
from app.models import OTELLogsData, OTELMetricsData, OTELTracesData
from app.protobuf_parser import ProtobufParsingError


class TestRequestDataParsing:
    """Test suite for request data parsing functions."""

    async def test_parse_json_traces_success(self, json_traces_data):
        """Test successful JSON traces parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        assert len(result.resource_spans) == 1
        assert result.resource_spans[0].resource.attributes[0].key == "service.name"

    async def test_parse_json_metrics_success(self, json_metrics_data):
        """Test successful JSON metrics parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=json_metrics_data["data"])

        # Act
        result = await parse_request_data(request, "metrics")

        # Assert
        assert isinstance(result, OTELMetricsData)
        assert len(result.resource_metrics) == 1

    async def test_parse_json_logs_success(self, json_logs_data):
        """Test successful JSON logs parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=json_logs_data["data"])

        # Act
        result = await parse_request_data(request, "logs")

        # Assert
        assert isinstance(result, OTELLogsData)
        assert len(result.resource_logs) == 1

    async def test_parse_protobuf_traces_success(self, protobuf_traces_data):
        """Test successful protobuf traces parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=protobuf_traces_data["binary_data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        assert len(result.resource_spans) == 1

    async def test_parse_protobuf_metrics_success(self, protobuf_metrics_data):
        """Test successful protobuf metrics parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=protobuf_metrics_data["binary_data"])

        # Act
        result = await parse_request_data(request, "metrics")

        # Assert
        assert isinstance(result, OTELMetricsData)
        assert len(result.resource_metrics) == 1

    async def test_parse_protobuf_logs_success(self, protobuf_logs_data):
        """Test successful protobuf logs parsing."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=protobuf_logs_data["binary_data"])

        # Act
        result = await parse_request_data(request, "logs")

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
            await parse_request_data(request, "traces")

        assert exc_info.value.status_code == 415
        assert "Unsupported content type" in exc_info.value.detail
        assert "application/xml" in exc_info.value.detail
        assert exc_info.value.headers == {"Accept": "application/json, application/x-protobuf"}

    async def test_missing_content_type_defaults_to_json(self, json_traces_data):
        """Test missing content-type defaults to JSON."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {}  # No content-type header
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        request.json.assert_called_once()

    async def test_empty_content_type_defaults_to_json(self, json_traces_data):
        """Test empty content-type defaults to JSON."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": ""}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        request.json.assert_called_once()

    async def test_content_type_with_charset_normalized(self, json_traces_data):
        """Test content-type with charset is normalized to base type."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json; charset=utf-8"}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        request.json.assert_called_once()

    async def test_content_type_case_insensitive(self, json_traces_data):
        """Test content-type header is case insensitive."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "APPLICATION/JSON"}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        request.json.assert_called_once()

    async def test_json_validation_error_returns_422(self):
        """Test JSON validation error is raised and bubbles up."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value={"resourceSpans": []})  # Invalid - empty

        # Act & Assert - Now expects ValidationError to bubble up
        with pytest.raises(ValidationError) as exc_info:
            await parse_request_data(request, "traces")

        assert "resourceSpans cannot be empty" in str(exc_info.value)

    async def test_json_decode_error_returns_422(self):
        """Test JSON decode error returns HTTP 422."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(side_effect=ValueError("Invalid JSON"))

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await parse_request_data(request, "traces")

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
            await parse_request_data(request, "traces")

        assert exc_info.value.status_code == 422
        assert "Invalid JSON" in exc_info.value.detail

    async def test_json_unexpected_error_returns_400(self):
        """Test unexpected JSON parsing error bubbles up."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(side_effect=RuntimeError("Unexpected error"))

        # Act & Assert - Now expects exception to bubble up
        with pytest.raises(RuntimeError) as exc_info:
            await parse_request_data(request, "traces")

        assert "Unexpected error" in str(exc_info.value)

    async def test_protobuf_parsing_error_returns_400(self):
        """Test protobuf parsing error propagates ProtobufParsingError for custom handler."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=b"invalid_protobuf_data")

        # Act & Assert - Now expects ProtobufParsingError to propagate
        with pytest.raises(ProtobufParsingError) as exc_info:
            await parse_request_data(request, "traces")

        assert "Invalid protobuf" in str(exc_info.value)

    async def test_protobuf_empty_data_error(self):
        """Test protobuf empty data propagates ProtobufParsingError for custom handler."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=b"")

        # Act & Assert - Now expects ProtobufParsingError to propagate
        with pytest.raises(ProtobufParsingError) as exc_info:
            await parse_request_data(request, "traces")

        assert "Empty protobuf data" in str(exc_info.value)

    async def test_protobuf_unexpected_error_returns_400(self):
        """Test unexpected protobuf parsing error bubbles up."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(side_effect=RuntimeError("Unexpected error"))

        # Act & Assert - Now expects exception to bubble up
        with pytest.raises(RuntimeError) as exc_info:
            await parse_request_data(request, "traces")

        assert "Unexpected error" in str(exc_info.value)

    async def test_invalid_data_type_raises_value_error(self, json_traces_data):
        """Test invalid data type raises ValueError."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act & Assert - ValueError is raised directly, not wrapped in HTTPException
        with pytest.raises(ValueError) as exc_info:
            await parse_request_data(request, "invalid_type")

        assert "Unknown data type" in str(exc_info.value)

    async def test_content_type_with_header(self, json_traces_data):
        """Test content type detection with explicit header."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert - should parse successfully as JSON
        assert isinstance(result, OTELTracesData)

    async def test_content_type_without_header(self, json_traces_data):
        """Test content type defaults to JSON when no header."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {}  # No content-type header
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert - should default to JSON parsing
        assert isinstance(result, OTELTracesData)

    async def test_content_type_with_parameters(self, json_traces_data):
        """Test content type strips parameters."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json; charset=utf-8; boundary=something"}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert - should parse as JSON despite parameters
        assert isinstance(result, OTELTracesData)

    async def test_content_type_case_normalization(self, protobuf_traces_data):
        """Test content type normalizes case."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "APPLICATION/X-PROTOBUF"}
        request.body = AsyncMock(return_value=protobuf_traces_data["binary_data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert - should parse as protobuf despite uppercase
        assert isinstance(result, OTELTracesData)

    async def test_content_type_with_whitespace(self, json_traces_data):
        """Test content type handles whitespace."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "  application/json  "}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert - should parse as JSON despite whitespace
        assert isinstance(result, OTELTracesData)

    # Test removed - create_unsupported_media_type_response method no longer exists
    # Unsupported media type responses are now handled by HTTPException in parse_request_data

    async def test_validation_error_serialization_with_context(self):
        """Test validation error with context objects are properly raised."""
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

        # Act & Assert - Now expects ValidationError to bubble up
        with pytest.raises(ValidationError) as exc_info:
            await parse_request_data(request, "traces")

        # Check that the error contains the expected validation message
        assert "Invalid hex string" in str(exc_info.value)

    async def test_content_type_detection_logging(self, json_traces_data):
        """Test that content type detection includes proper logging."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        with patch("app.content_handler.logger") as mock_logger:
            await parse_request_data(request, "traces")

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
                await parse_request_data(request, "traces")

        mock_logger.warning.assert_called_with(
            "Unsupported content type", content_type="application/xml", data_type="traces"
        )
