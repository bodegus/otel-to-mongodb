"""
Integration tests for Protocol Buffer support in OTEL service.

These tests verify the complete pipeline from protobuf request through to MongoDB
storage using real containers and direct service calls.
"""

import pytest

from app.models import OTELLogsData, OTELTracesData

# Import shared fixtures
from app.tests.fixtures.otel_data import (
    multi_logs_data,
    multi_metrics_data,
    multi_span_traces_data,
    sample_logs_data,
    sample_metrics_data,
    sample_traces_data,
)
from app.tests.fixtures.protobuf_data import (
    empty_protobuf_traces_data,
    large_protobuf_traces_data,
    malformed_protobuf_data,
    sample_protobuf_logs_data,
    sample_protobuf_metrics_data,
    sample_protobuf_traces_data,
)

# Import integration fixtures
from .fixtures import otel_integration_context


# Prevent unused import warnings for pytest fixtures
__all__ = [
    "empty_protobuf_traces_data",
    "large_protobuf_traces_data",
    "malformed_protobuf_data",
    "multi_logs_data",
    "multi_metrics_data",
    "multi_span_traces_data",
    "otel_integration_context",
    "sample_logs_data",
    "sample_metrics_data",
    "sample_protobuf_logs_data",
    "sample_protobuf_metrics_data",
    "sample_protobuf_traces_data",
    "sample_traces_data",
]


class TestProtobufIntegrationBasic:
    """Basic integration tests for protobuf support."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_protobuf_traces_integration(
        self, otel_integration_context, sample_protobuf_traces_data
    ):
        """Test complete protobuf traces workflow through to MongoDB."""
        context = otel_integration_context

        # Parse protobuf data using the content handler
        from unittest.mock import AsyncMock, Mock

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_traces_data["binary_data"])

        # Parse the protobuf data
        traces_data = await handler.parse_request_data(request, "traces")

        # Process through service
        result = await context.otel_service.process_traces(
            traces_data, request_id="protobuf-integration-traces"
        )

        # Verify processing succeeded
        assert result.success is True
        assert result.data_type == "traces"
        assert result.records_processed == sample_protobuf_traces_data["expected_count"]
        assert result.document_id is not None

        # Verify MongoDB storage
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "traces"
        assert doc["request_id"] == "protobuf-integration-traces"

        # Verify the structure matches what we expect from protobuf
        assert "resourceSpans" in doc
        assert len(doc["resourceSpans"]) > 0
        assert "scopeSpans" in doc["resourceSpans"][0]

        print(f"✅ Protobuf traces integration: {result.records_processed} spans stored in MongoDB")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_protobuf_metrics_integration(
        self, otel_integration_context, sample_protobuf_metrics_data
    ):
        """Test complete protobuf metrics workflow through to MongoDB."""
        context = otel_integration_context

        # Parse protobuf data using the content handler
        from unittest.mock import AsyncMock, Mock

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_metrics_data["binary_data"])

        # Parse the protobuf data
        metrics_data = await handler.parse_request_data(request, "metrics")

        # Process through service
        result = await context.otel_service.process_metrics(
            metrics_data, request_id="protobuf-integration-metrics"
        )

        # Verify processing succeeded
        assert result.success is True
        assert result.data_type == "metrics"
        assert result.records_processed == sample_protobuf_metrics_data["expected_count"]
        assert result.document_id is not None

        # Verify MongoDB storage
        documents = await context.verify_telemetry_data("metrics", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "metrics"
        assert doc["request_id"] == "protobuf-integration-metrics"

        # Verify the structure matches what we expect from protobuf
        assert "resourceMetrics" in doc
        assert len(doc["resourceMetrics"]) > 0
        assert "scopeMetrics" in doc["resourceMetrics"][0]

        print(
            f"✅ Protobuf metrics integration: {result.records_processed} metrics stored in MongoDB"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_protobuf_logs_integration(
        self, otel_integration_context, sample_protobuf_logs_data
    ):
        """Test complete protobuf logs workflow through to MongoDB."""
        context = otel_integration_context

        # Parse protobuf data using the content handler
        from unittest.mock import AsyncMock, Mock

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_logs_data["binary_data"])

        # Parse the protobuf data
        logs_data = await handler.parse_request_data(request, "logs")

        # Process through service
        result = await context.otel_service.process_logs(
            logs_data, request_id="protobuf-integration-logs"
        )

        # Verify processing succeeded
        assert result.success is True
        assert result.data_type == "logs"
        assert result.records_processed == sample_protobuf_logs_data["expected_count"]
        assert result.document_id is not None

        # Verify MongoDB storage
        documents = await context.verify_telemetry_data("logs", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "logs"
        assert doc["request_id"] == "protobuf-integration-logs"

        # Verify the structure matches what we expect from protobuf
        assert "resourceLogs" in doc
        assert len(doc["resourceLogs"]) > 0
        assert "scopeLogs" in doc["resourceLogs"][0]

        print(
            f"✅ Protobuf logs integration: {result.records_processed} log records stored in MongoDB"
        )


class TestProtobufIntegrationConsistency:
    """Test consistency between JSON and protobuf data processing."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_json_protobuf_consistency_traces(
        self, otel_integration_context, sample_traces_data, sample_protobuf_traces_data
    ):
        """Verify MongoDB documents are consistent between JSON and protobuf inputs."""
        context = otel_integration_context

        # Process JSON data
        json_traces = OTELTracesData(**sample_traces_data["data"])
        json_result = await context.otel_service.process_traces(
            json_traces, request_id="consistency-json-traces"
        )

        # Parse and process protobuf data
        from unittest.mock import AsyncMock, Mock

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_traces_data["binary_data"])

        protobuf_traces = await handler.parse_request_data(request, "traces")
        protobuf_result = await context.otel_service.process_traces(
            protobuf_traces, request_id="consistency-protobuf-traces"
        )

        # Both should have processed the same number of records
        assert json_result.records_processed == protobuf_result.records_processed

        # Retrieve both documents
        documents = await context.verify_telemetry_data("traces", expected_count=2)

        json_doc = None
        protobuf_doc = None
        for doc in documents:
            if doc["request_id"] == "consistency-json-traces":
                json_doc = doc
            elif doc["request_id"] == "consistency-protobuf-traces":
                protobuf_doc = doc

        assert json_doc is not None
        assert protobuf_doc is not None

        # Compare structure (excluding request_id and _id)
        assert json_doc["data_type"] == protobuf_doc["data_type"]
        assert len(json_doc["resourceSpans"]) == len(protobuf_doc["resourceSpans"])

        print("✅ JSON/Protobuf consistency test: Document structures match for traces")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_mixed_requests_same_instance(
        self,
        otel_integration_context,
        sample_traces_data,
        sample_protobuf_metrics_data,
        sample_logs_data,
    ):
        """Test mixed JSON and protobuf requests to same service instance."""
        context = otel_integration_context

        # Process JSON traces
        json_traces = OTELTracesData(**sample_traces_data["data"])
        traces_result = await context.otel_service.process_traces(
            json_traces, request_id="mixed-json-traces"
        )

        # Process protobuf metrics
        from unittest.mock import AsyncMock, Mock

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_metrics_data["binary_data"])

        protobuf_metrics = await handler.parse_request_data(request, "metrics")
        metrics_result = await context.otel_service.process_metrics(
            protobuf_metrics, request_id="mixed-protobuf-metrics"
        )

        # Process JSON logs
        json_logs = OTELLogsData(**sample_logs_data["data"])
        logs_result = await context.otel_service.process_logs(
            json_logs, request_id="mixed-json-logs"
        )

        # Verify all succeeded
        assert traces_result.success is True
        assert metrics_result.success is True
        assert logs_result.success is True

        # Verify each stored in correct collection
        _traces = await context.verify_telemetry_data("traces", expected_count=1)
        _metrics = await context.verify_telemetry_data("metrics", expected_count=1)
        _logs = await context.verify_telemetry_data("logs", expected_count=1)

        print("✅ Mixed request integration: JSON and protobuf requests processed successfully")


class TestProtobufIntegrationWithFailover:
    """Test protobuf support with database failover scenarios."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_protobuf_with_secondary_failover(
        self, otel_integration_context, sample_protobuf_traces_data
    ):
        """Test protobuf data processing continues with secondary DB if available."""
        context = otel_integration_context

        # Note: In the test environment, we don't have a real secondary,
        # but we can verify the service handles protobuf data correctly
        # even when secondary is not available

        from unittest.mock import AsyncMock, Mock

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=sample_protobuf_traces_data["binary_data"])

        # Parse the protobuf data
        traces_data = await handler.parse_request_data(request, "traces")

        # Process through service
        result = await context.otel_service.process_traces(
            traces_data, request_id="protobuf-failover-test"
        )

        # Should succeed even without secondary
        assert result.success is True
        assert result.primary_storage is True
        # Secondary is not configured in test environment
        assert result.secondary_storage is False

        # Verify data was stored
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        assert documents[0]["request_id"] == "protobuf-failover-test"

        print("✅ Protobuf failover test: Data stored successfully with primary database")


class TestProtobufIntegrationErrors:
    """Test error scenarios with protobuf in integration environment."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_malformed_protobuf_handling(
        self, otel_integration_context, malformed_protobuf_data
    ):
        """Test that malformed protobuf data is handled gracefully."""
        from unittest.mock import AsyncMock, Mock

        from fastapi import HTTPException

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=malformed_protobuf_data["binary_data"])

        # Should raise HTTP 400 error
        with pytest.raises(HTTPException) as exc_info:
            await handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 400
        assert "protobuf" in str(exc_info.value.detail).lower()

        print("✅ Malformed protobuf test: Error handled correctly with HTTP 400")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_empty_protobuf_handling(self, otel_integration_context):
        """Test that empty protobuf data is handled gracefully."""
        from unittest.mock import AsyncMock, Mock

        from fastapi import HTTPException

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=b"")

        # Should raise HTTP 400 error
        with pytest.raises(HTTPException) as exc_info:
            await handler.parse_request_data(request, "traces")

        assert exc_info.value.status_code == 400
        assert "empty" in str(exc_info.value.detail).lower()

        print("✅ Empty protobuf test: Error handled correctly with HTTP 400")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_large_protobuf_payload(
        self, otel_integration_context, large_protobuf_traces_data
    ):
        """Test that large protobuf payloads are processed successfully."""
        context = otel_integration_context

        from unittest.mock import AsyncMock, Mock

        from app.content_handler import ContentTypeHandler

        handler = ContentTypeHandler()
        request = Mock()
        request.headers = {"content-type": "application/x-protobuf"}
        request.body = AsyncMock(return_value=large_protobuf_traces_data["binary_data"])

        # Parse the large protobuf data
        traces_data = await handler.parse_request_data(request, "traces")

        # Process through service
        result = await context.otel_service.process_traces(
            traces_data, request_id="large-protobuf-test"
        )

        # Verify processing succeeded
        assert result.success is True
        assert result.records_processed == large_protobuf_traces_data["expected_count"]

        # Verify MongoDB storage
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        assert doc["request_id"] == "large-protobuf-test"

        # Count actual spans in the document
        span_count = 0
        for resource_span in doc.get("resourceSpans", []):
            for scope_span in resource_span.get("scopeSpans", []):
                span_count += len(scope_span.get("spans", []))

        assert span_count == large_protobuf_traces_data["expected_count"]

        print(f"✅ Large protobuf test: Successfully processed {span_count} spans")
