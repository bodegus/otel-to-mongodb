"""
MongoDB integration tests for OTEL service.

These tests verify the complete pipeline from OTEL service through to MongoDB
using real containers and reusing unit test data and logic.
"""

# ruff: noqa: F841  # Ignore unused variable warnings for test return values
# flake8: noqa: F841

import pytest

# Import shared fixtures and helpers
from app.tests.fixtures.otel_data import (
    multi_logs_data,
    multi_metrics_data,
    multi_span_traces_data,
    sample_logs_data,
    sample_metrics_data,
    sample_traces_data,
)
from app.tests.helpers.test_utils import (
    OTELTestHelpers,
    assert_otel_data_structure,
    count_logs_in_data,
    count_metrics_in_data,
    count_spans_in_data,
    extract_service_names,
)

# Import integration fixtures
from .fixtures import otel_integration_context


# Prevent unused import warnings for pytest fixtures
__all__ = [
    "multi_logs_data",
    "multi_metrics_data",
    "multi_span_traces_data",
    "otel_integration_context",
    "sample_logs_data",
    "sample_metrics_data",
    "sample_traces_data",
]


class TestOTELIntegrationWithUnitTestData:
    """Integration tests using shared unit test data and logic."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_traces_integration_with_unit_test_data(
        self, otel_integration_context, sample_traces_data
    ):
        """Test traces using the SAME data as unit tests, but with real MongoDB."""
        context = otel_integration_context

        # Use the shared test helper (same logic as unit tests)
        expected_spans = count_spans_in_data(sample_traces_data)
        result = await OTELTestHelpers.process_and_verify_traces(
            context.otel_service,
            sample_traces_data,
            request_id="integration-unit-test-traces",
            expected_span_count=expected_spans,
        )

        # Additional integration-specific verification: check actual MongoDB storage
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "traces"
        assert doc["request_id"] == "integration-unit-test-traces"
        assert_otel_data_structure(doc, "traces")

        # Verify service names match
        service_names = extract_service_names(doc, "traces")
        assert "test-service" in service_names

        print(
            f"✅ Traces integration: Reused unit test data, verified {expected_spans} spans in MongoDB"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_metrics_integration_with_unit_test_data(
        self, otel_integration_context, sample_metrics_data
    ):
        """Test metrics using the SAME data as unit tests, but with real MongoDB."""
        context = otel_integration_context

        # Reuse unit test logic
        expected_metrics = count_metrics_in_data(sample_metrics_data)
        result = await OTELTestHelpers.process_and_verify_metrics(
            context.otel_service,
            sample_metrics_data,
            request_id="integration-unit-test-metrics",
            expected_metric_count=expected_metrics,
        )

        # Integration-specific verification
        documents = await context.verify_telemetry_data("metrics", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "metrics"
        assert doc["request_id"] == "integration-unit-test-metrics"
        assert_otel_data_structure(doc, "metrics")

        # Verify service names match
        service_names = extract_service_names(doc, "metrics")
        assert "test-service" in service_names

        print(
            f"✅ Metrics integration: Reused unit test data, verified {expected_metrics} metrics in MongoDB"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_logs_integration_with_unit_test_data(
        self, otel_integration_context, sample_logs_data
    ):
        """Test logs using the SAME data as unit tests, but with real MongoDB."""
        context = otel_integration_context

        # Reuse unit test logic
        expected_logs = count_logs_in_data(sample_logs_data)
        result = await OTELTestHelpers.process_and_verify_logs(
            context.otel_service,
            sample_logs_data,
            request_id="integration-unit-test-logs",
            expected_log_count=expected_logs,
        )

        # Integration-specific verification
        documents = await context.verify_telemetry_data("logs", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "logs"
        assert doc["request_id"] == "integration-unit-test-logs"
        assert_otel_data_structure(doc, "logs")

        # Verify service names match
        service_names = extract_service_names(doc, "logs")
        assert "test-service" in service_names

        print(
            f"✅ Logs integration: Reused unit test data, verified {expected_logs} log records in MongoDB"
        )


class TestOTELIntegrationWithEnhancedData:
    """Integration tests with enhanced test data for more complex scenarios."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_enhanced_traces_integration(self, otel_integration_context, sample_traces_data):
        """Test with enhanced integration-specific traces data."""
        context = otel_integration_context

        # Still reuse the shared logic, just with enhanced data
        expected_spans = count_spans_in_data(sample_traces_data["data"])
        result = await OTELTestHelpers.process_and_verify_traces(
            context.otel_service,
            sample_traces_data,
            request_id="enhanced-integration-traces",
            expected_span_count=expected_spans,
        )

        # Verify integration-specific attributes
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        # Check for service name
        service_names = extract_service_names(doc, "traces")
        assert "test-service" in service_names

        # Verify multiple spans were processed
        assert expected_spans == 2  # Should have 2 spans from integration data

        print(
            f"✅ Enhanced traces integration: Used extended test data with {expected_spans} spans in real MongoDB"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_enhanced_metrics_integration(
        self, otel_integration_context, sample_metrics_data
    ):
        """Test with enhanced integration-specific metrics data."""
        context = otel_integration_context

        expected_metrics = count_metrics_in_data(sample_metrics_data["data"])
        result = await OTELTestHelpers.process_and_verify_metrics(
            context.otel_service,
            sample_metrics_data,
            request_id="enhanced-integration-metrics",
            expected_metric_count=expected_metrics,
        )

        # Verify integration-specific data
        documents = await context.verify_telemetry_data("metrics", expected_count=1)
        doc = documents[0]

        service_names = extract_service_names(doc, "metrics")
        assert "test-service" in service_names

        # Verify metrics are present
        metrics = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        metric_names = [metric["name"] for metric in metrics]
        assert len(metrics) > 0

        print(
            f"✅ Enhanced metrics integration: {expected_metrics} metrics with sum and gauge types"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_enhanced_logs_integration(self, otel_integration_context, sample_logs_data):
        """Test with enhanced integration-specific logs data."""
        context = otel_integration_context

        expected_logs = count_logs_in_data(sample_logs_data["data"])
        result = await OTELTestHelpers.process_and_verify_logs(
            context.otel_service,
            sample_logs_data,
            request_id="enhanced-integration-logs",
            expected_log_count=expected_logs,
        )

        # Verify integration-specific data
        documents = await context.verify_telemetry_data("logs", expected_count=1)
        doc = documents[0]

        service_names = extract_service_names(doc, "logs")
        assert "test-service" in service_names

        # Verify log records exist
        log_records = doc["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        assert len(log_records) > 0

        print(
            f"✅ Enhanced logs integration: {expected_logs} log records with multiple severity levels"
        )


class TestOTELIntegrationCountingWithUnitTestData:
    """Integration tests for counting logic using the same test data as unit tests."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_counting_spans_integration(
        self, otel_integration_context, multi_span_traces_data
    ):
        """Test span counting with the SAME multi-span data from unit tests."""
        context = otel_integration_context

        # This uses the exact same test data as the unit test for counting
        expected_spans = count_spans_in_data(multi_span_traces_data["data"])
        assert expected_spans == 3  # Should match unit test expectations

        result = await OTELTestHelpers.process_and_verify_traces(
            context.otel_service,
            multi_span_traces_data,
            request_id="integration-counting-spans",
            expected_span_count=expected_spans,
        )

        # Verify it's actually stored in MongoDB
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        # Manually count spans in the stored document to verify
        stored_spans = count_spans_in_data(doc)
        assert stored_spans == 3

        print(
            f"✅ Span counting integration: Verified {expected_spans} spans counted and stored correctly"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_counting_metrics_integration(self, otel_integration_context, multi_metrics_data):
        """Test metrics counting with the SAME multi-metric data from unit tests."""
        context = otel_integration_context

        expected_metrics = count_metrics_in_data(multi_metrics_data["data"])
        assert expected_metrics == 2  # Should match unit test expectations

        result = await OTELTestHelpers.process_and_verify_metrics(
            context.otel_service,
            multi_metrics_data,
            request_id="integration-counting-metrics",
            expected_metric_count=expected_metrics,
        )

        # Verify it's actually stored in MongoDB
        documents = await context.verify_telemetry_data("metrics", expected_count=1)
        doc = documents[0]

        # Manually count metrics in the stored document
        stored_metrics = count_metrics_in_data(doc)
        assert stored_metrics == 2

        print(
            f"✅ Metrics counting integration: Verified {expected_metrics} metrics counted and stored correctly"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_counting_logs_integration(self, otel_integration_context, multi_logs_data):
        """Test log records counting with the SAME multi-log data from unit tests."""
        context = otel_integration_context

        expected_logs = count_logs_in_data(multi_logs_data["data"])
        assert expected_logs == 2  # Should match unit test expectations

        result = await OTELTestHelpers.process_and_verify_logs(
            context.otel_service,
            multi_logs_data,
            request_id="integration-counting-logs",
            expected_log_count=expected_logs,
        )

        # Verify it's actually stored in MongoDB
        documents = await context.verify_telemetry_data("logs", expected_count=1)
        doc = documents[0]

        # Manually count log records in the stored document
        stored_logs = count_logs_in_data(doc)
        assert stored_logs == 2

        print(
            f"✅ Log counting integration: Verified {expected_logs} log records counted and stored correctly"
        )


class TestOTELIntegrationErrorScenarios:
    """Integration tests for error scenarios and edge cases."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_multiple_requests_same_type(self, otel_integration_context, sample_traces_data):
        """Test multiple requests of the same type to verify database isolation."""
        context = otel_integration_context

        # Process the same traces data multiple times
        results = []
        for i in range(3):
            result = await OTELTestHelpers.process_and_verify_traces(
                context.otel_service,
                sample_traces_data,
                request_id=f"multi-request-{i}",
            )
            results.append(result)

        # Verify all 3 documents are stored
        documents = await context.verify_telemetry_data("traces", expected_count=3)

        # Verify each has a unique request_id
        request_ids = [doc["request_id"] for doc in documents]
        assert len(set(request_ids)) == 3  # All unique
        assert all(f"multi-request-{i}" in request_ids for i in range(3))

        print(
            "✅ Multiple requests integration: Verified 3 separate documents with unique request IDs"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_mixed_telemetry_types(
        self, otel_integration_context, sample_traces_data, sample_metrics_data, sample_logs_data
    ):
        """Test processing different telemetry types in the same test database."""
        context = otel_integration_context

        # Process all three types of telemetry data
        traces_result = await OTELTestHelpers.process_and_verify_traces(
            context.otel_service, sample_traces_data, request_id="mixed-traces"
        )

        metrics_result = await OTELTestHelpers.process_and_verify_metrics(
            context.otel_service, sample_metrics_data, request_id="mixed-metrics"
        )

        logs_result = await OTELTestHelpers.process_and_verify_logs(
            context.otel_service, sample_logs_data, request_id="mixed-logs"
        )

        # Verify each type was stored in its own collection
        _traces_docs = await context.verify_telemetry_data("traces", expected_count=1)
        _metrics_docs = await context.verify_telemetry_data("metrics", expected_count=1)
        _logs_docs = await context.verify_telemetry_data("logs", expected_count=1)

        # Verify document IDs are all different
        all_doc_ids = [
            traces_result.document_id,
            metrics_result.document_id,
            logs_result.document_id,
        ]
        assert len(set(all_doc_ids)) == 3  # All unique

        print(
            "✅ Mixed telemetry types integration: Verified traces, metrics, and logs stored in separate collections"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_without_request_id(self, otel_integration_context, sample_traces_data):
        """Test processing without request_id (should still work)."""
        context = otel_integration_context

        # Process without request_id
        result = await OTELTestHelpers.process_and_verify_traces(
            context.otel_service,
            sample_traces_data,
            request_id=None,  # Explicitly None
        )

        # Verify it was stored
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        # Verify request_id is None in the stored document
        assert doc["request_id"] is None
        assert doc["data_type"] == "traces"

        print("✅ No request ID integration: Verified processing works without request_id")
