"""Example of migrated ContentTypeHandler tests using unified fixtures.

This demonstrates how existing tests can be updated to use the new unified
fixtures for reduced duplication and parametrized content-type testing.
"""

from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException, Request
from pydantic import ValidationError

from app.content_handler import parse_request_data
from app.models import OTELLogsData, OTELMetricsData, OTELTracesData
from app.protobuf_parser import ProtobufParsingError


class TestRequestDataParsingUnified:
    """Test suite demonstrating unified fixture usage."""

    @pytest.mark.unit
    async def test_parse_request_data_both_formats(self, unified_traces_data):
        """Test parsing for both JSON and protobuf formats automatically."""
        # Arrange
        request = Mock(spec=Request)
        content_type = unified_traces_data["content_type"]
        request.headers = {"content-type": content_type}

        if content_type == "application/json":
            request.json = AsyncMock(return_value=unified_traces_data["data"])
            request.body = AsyncMock()  # Not used for JSON
        else:  # protobuf
            request.json = AsyncMock()  # Not used for protobuf
            request.body = AsyncMock(return_value=unified_traces_data["binary_data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert - same assertions work for both formats
        assert isinstance(result, OTELTracesData)
        assert len(result.resource_spans) == 1
        assert result.resource_spans[0].resource.attributes[0].key == "service.name"

        # Verify expected span count matches
        total_spans = sum(
            len(scope_span.spans)
            for resource_span in result.resource_spans
            for scope_span in resource_span.scope_spans
        )
        assert total_spans == unified_traces_data["expected_count"]

    @pytest.mark.unit
    async def test_metrics_parsing_both_formats(self, unified_metrics_data):
        """Test metrics parsing for both JSON and protobuf formats."""
        # Arrange
        request = Mock(spec=Request)
        content_type = unified_metrics_data["content_type"]
        request.headers = {"content-type": content_type}

        if content_type == "application/json":
            request.json = AsyncMock(return_value=unified_metrics_data["data"])
            request.body = AsyncMock()
        else:  # protobuf
            request.json = AsyncMock()
            request.body = AsyncMock(return_value=unified_metrics_data["binary_data"])

        # Act
        result = await parse_request_data(request, "metrics")

        # Assert
        assert isinstance(result, OTELMetricsData)
        assert len(result.resource_metrics) == 1

        # Verify expected metric count
        total_metrics = sum(
            len(scope_metric.metrics)
            for resource_metric in result.resource_metrics
            for scope_metric in resource_metric.scope_metrics
        )
        assert total_metrics == unified_metrics_data["expected_count"]

    @pytest.mark.unit
    async def test_logs_parsing_both_formats(self, unified_logs_data):
        """Test logs parsing for both JSON and protobuf formats."""
        # Arrange
        request = Mock(spec=Request)
        content_type = unified_logs_data["content_type"]
        request.headers = {"content-type": content_type}

        if content_type == "application/json":
            request.json = AsyncMock(return_value=unified_logs_data["data"])
            request.body = AsyncMock()
        else:  # protobuf
            request.json = AsyncMock()
            request.body = AsyncMock(return_value=unified_logs_data["binary_data"])

        # Act
        result = await parse_request_data(request, "logs")

        # Assert
        assert isinstance(result, OTELLogsData)
        assert len(result.resource_logs) == 1

        # Verify expected log record count
        total_log_records = sum(
            len(scope_log.log_records)
            for resource_log in result.resource_logs
            for scope_log in resource_log.scope_logs
        )
        assert total_log_records == unified_logs_data["expected_count"]

    @pytest.mark.unit
    async def test_json_only_when_needed(self, json_traces_data):
        """Example of testing only JSON when protobuf is not relevant."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/json"}
        request.json = AsyncMock(return_value=json_traces_data["data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        assert json_traces_data["content_type"] == "application/json"

    @pytest.mark.unit
    async def test_protobuf_only_when_needed(self, protobuf_traces_data):
        """Example of testing only protobuf when JSON is not relevant."""
        # Arrange
        request = Mock(spec=Request)
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=protobuf_traces_data["binary_data"])

        # Act
        result = await parse_request_data(request, "traces")

        # Assert
        assert isinstance(result, OTELTracesData)
        assert protobuf_traces_data["content_type"] == "application/x-protobuf"

    @pytest.mark.unit
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

    @pytest.mark.unit
    async def test_error_handling_both_formats_validation_error(self, unified_traces_data):
        """Test that validation errors bubble up for both content types."""
        # Arrange - create invalid data that will fail validation
        request = Mock(spec=Request)
        content_type = unified_traces_data["content_type"]
        request.headers = {"content-type": content_type}

        if content_type == "application/json":
            # Empty resourceSpans should fail validation
            invalid_data = {"resourceSpans": []}
            request.json = AsyncMock(return_value=invalid_data)
            request.body = AsyncMock()
        else:  # protobuf
            # Empty protobuf should trigger parsing error, not validation error
            request.json = AsyncMock()
            request.body = AsyncMock(return_value=b"")

        # Act & Assert
        if content_type == "application/json":
            with pytest.raises(ValidationError):
                await parse_request_data(request, "traces")
        else:  # protobuf empty data raises ProtobufParsingError
            with pytest.raises(ProtobufParsingError):
                await parse_request_data(request, "traces")
