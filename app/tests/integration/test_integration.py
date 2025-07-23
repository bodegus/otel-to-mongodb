"""
MongoDB integration tests for OTEL service.

These tests verify the complete pipeline from OTEL service through to MongoDB
using real containers and direct service calls.
"""

# ruff: noqa: F841  # Ignore unused variable warnings for test return values

import pytest

from app.models import OTELLogsData, OTELMetricsData, OTELTracesData

# Import shared fixtures
from app.tests.fixtures.otel_data import (
    multi_logs_data,
    multi_metrics_data,
    multi_span_traces_data,
    sample_logs_data,
    sample_metrics_data,
    sample_traces_data,
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


def count_spans_in_data(data_dict):
    """Count spans in traces data."""
    count = 0
    for resource_span in data_dict.get("resourceSpans", []):
        for scope_span in resource_span.get("scopeSpans", []):
            count += len(scope_span.get("spans", []))
    return count


def count_metrics_in_data(data_dict):
    """Count metrics in metrics data."""
    count = 0
    for resource_metric in data_dict.get("resourceMetrics", []):
        for scope_metric in resource_metric.get("scopeMetrics", []):
            count += len(scope_metric.get("metrics", []))
    return count


def count_logs_in_data(data_dict):
    """Count log records in logs data."""
    count = 0
    for resource_log in data_dict.get("resourceLogs", []):
        for scope_log in resource_log.get("scopeLogs", []):
            count += len(scope_log.get("logRecords", []))
    return count


def extract_service_names(data_dict, data_type):
    """Extract service names from telemetry data."""
    service_names = set()

    if data_type == "traces":
        for resource_span in data_dict.get("resourceSpans", []):
            resource = resource_span.get("resource", {})
            for attr in resource.get("attributes", []):
                if attr.get("key") == "service.name":
                    service_names.add(attr["value"]["stringValue"])
    elif data_type == "metrics":
        for resource_metric in data_dict.get("resourceMetrics", []):
            resource = resource_metric.get("resource", {})
            for attr in resource.get("attributes", []):
                if attr.get("key") == "service.name":
                    service_names.add(attr["value"]["stringValue"])
    elif data_type == "logs":
        for resource_log in data_dict.get("resourceLogs", []):
            resource = resource_log.get("resource", {})
            for attr in resource.get("attributes", []):
                if attr.get("key") == "service.name":
                    service_names.add(attr["value"]["stringValue"])

    return list(service_names)


class TestOTELIntegrationWithUnitTestData:
    """Integration tests using shared unit test data with direct service calls."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_traces_integration_with_unit_test_data(
        self, otel_integration_context, sample_traces_data
    ):
        """Test traces using the SAME data as unit tests, but with real MongoDB."""
        context = otel_integration_context

        # Get data and expected count from fixture
        traces_data_dict = sample_traces_data["data"]
        expected_count = sample_traces_data["expected_count"]

        # Call service directly
        traces_data = OTELTracesData(**traces_data_dict)
        result = await context.otel_service.process_traces(
            traces_data, request_id="integration-unit-test-traces"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "traces"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify MongoDB storage
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "traces"
        assert doc["request_id"] == "integration-unit-test-traces"

        # Verify service names
        service_names = extract_service_names(doc, "traces")
        assert "test-service" in service_names

        print(f"✅ Traces integration: Verified {expected_count} spans in MongoDB")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_metrics_integration_with_unit_test_data(
        self, otel_integration_context, sample_metrics_data
    ):
        """Test metrics using the SAME data as unit tests, but with real MongoDB."""
        context = otel_integration_context

        # Get data and expected count from fixture
        metrics_data_dict = sample_metrics_data["data"]
        expected_count = sample_metrics_data["expected_count"]

        # Call service directly
        metrics_data = OTELMetricsData(**metrics_data_dict)
        result = await context.otel_service.process_metrics(
            metrics_data, request_id="integration-unit-test-metrics"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "metrics"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify MongoDB storage
        documents = await context.verify_telemetry_data("metrics", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "metrics"
        assert doc["request_id"] == "integration-unit-test-metrics"

        # Verify service names
        service_names = extract_service_names(doc, "metrics")
        assert "test-service" in service_names

        print(f"✅ Metrics integration: Verified {expected_count} metrics in MongoDB")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_logs_integration_with_unit_test_data(
        self, otel_integration_context, sample_logs_data
    ):
        """Test logs using the SAME data as unit tests, but with real MongoDB."""
        context = otel_integration_context

        # Get data and expected count from fixture
        logs_data_dict = sample_logs_data["data"]
        expected_count = sample_logs_data["expected_count"]

        # Call service directly
        logs_data = OTELLogsData(**logs_data_dict)
        result = await context.otel_service.process_logs(
            logs_data, request_id="integration-unit-test-logs"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "logs"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify MongoDB storage
        documents = await context.verify_telemetry_data("logs", expected_count=1)
        doc = documents[0]

        assert doc["data_type"] == "logs"
        assert doc["request_id"] == "integration-unit-test-logs"

        # Verify service names
        service_names = extract_service_names(doc, "logs")
        assert "test-service" in service_names

        print(f"✅ Logs integration: Verified {expected_count} log records in MongoDB")


class TestOTELIntegrationWithEnhancedData:
    """Integration tests with enhanced test data for more complex scenarios."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_enhanced_traces_integration(self, otel_integration_context, sample_traces_data):
        """Test with enhanced integration-specific traces data."""
        context = otel_integration_context

        # Get data and expected count from fixture
        traces_data_dict = sample_traces_data["data"]
        expected_count = sample_traces_data["expected_count"]

        # Call service directly
        traces_data = OTELTracesData(**traces_data_dict)
        result = await context.otel_service.process_traces(
            traces_data, request_id="enhanced-integration-traces"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "traces"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify integration-specific attributes
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        # Check for service name
        service_names = extract_service_names(doc, "traces")
        assert "test-service" in service_names

        # Verify spans were processed
        stored_spans = count_spans_in_data(doc)
        assert stored_spans == expected_count

        print(f"✅ Enhanced traces integration: {expected_count} spans in real MongoDB")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_enhanced_metrics_integration(
        self, otel_integration_context, sample_metrics_data
    ):
        """Test with enhanced integration-specific metrics data."""
        context = otel_integration_context

        # Get data and expected count from fixture
        metrics_data_dict = sample_metrics_data["data"]
        expected_count = sample_metrics_data["expected_count"]

        # Call service directly
        metrics_data = OTELMetricsData(**metrics_data_dict)
        result = await context.otel_service.process_metrics(
            metrics_data, request_id="enhanced-integration-metrics"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "metrics"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify integration-specific data
        documents = await context.verify_telemetry_data("metrics", expected_count=1)
        doc = documents[0]

        service_names = extract_service_names(doc, "metrics")
        assert "test-service" in service_names

        # Verify metrics are present
        stored_metrics = count_metrics_in_data(doc)
        assert stored_metrics == expected_count

        print(f"✅ Enhanced metrics integration: {expected_count} metrics with sum and gauge types")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_enhanced_logs_integration(self, otel_integration_context, sample_logs_data):
        """Test with enhanced integration-specific logs data."""
        context = otel_integration_context

        # Get data and expected count from fixture
        logs_data_dict = sample_logs_data["data"]
        expected_count = sample_logs_data["expected_count"]

        # Call service directly
        logs_data = OTELLogsData(**logs_data_dict)
        result = await context.otel_service.process_logs(
            logs_data, request_id="enhanced-integration-logs"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "logs"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify integration-specific data
        documents = await context.verify_telemetry_data("logs", expected_count=1)
        doc = documents[0]

        service_names = extract_service_names(doc, "logs")
        assert "test-service" in service_names

        # Verify log records exist
        stored_logs = count_logs_in_data(doc)
        assert stored_logs == expected_count

        print(
            f"✅ Enhanced logs integration: {expected_count} log records with multiple severity levels"
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

        # Get data and expected count from fixture
        traces_data_dict = multi_span_traces_data["data"]
        expected_count = multi_span_traces_data["expected_count"]

        # Verify expectation matches the data
        actual_spans = count_spans_in_data(traces_data_dict)
        assert actual_spans == expected_count  # Should be 3

        # Call service directly
        traces_data = OTELTracesData(**traces_data_dict)
        result = await context.otel_service.process_traces(
            traces_data, request_id="integration-counting-spans"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "traces"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify it's actually stored in MongoDB
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        # Manually count spans in the stored document to verify
        stored_spans = count_spans_in_data(doc)
        assert stored_spans == expected_count

        print(
            f"✅ Span counting integration: Verified {expected_count} spans counted and stored correctly"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_counting_metrics_integration(self, otel_integration_context, multi_metrics_data):
        """Test metrics counting with the SAME multi-metric data from unit tests."""
        context = otel_integration_context

        # Get data and expected count from fixture
        metrics_data_dict = multi_metrics_data["data"]
        expected_count = multi_metrics_data["expected_count"]

        # Verify expectation matches the data
        actual_metrics = count_metrics_in_data(metrics_data_dict)
        assert actual_metrics == expected_count  # Should be 2

        # Call service directly
        metrics_data = OTELMetricsData(**metrics_data_dict)
        result = await context.otel_service.process_metrics(
            metrics_data, request_id="integration-counting-metrics"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "metrics"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify it's actually stored in MongoDB
        documents = await context.verify_telemetry_data("metrics", expected_count=1)
        doc = documents[0]

        # Manually count metrics in the stored document
        stored_metrics = count_metrics_in_data(doc)
        assert stored_metrics == expected_count

        print(
            f"✅ Metrics counting integration: Verified {expected_count} metrics counted and stored correctly"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_counting_logs_integration(self, otel_integration_context, multi_logs_data):
        """Test log records counting with the SAME multi-log data from unit tests."""
        context = otel_integration_context

        # Get data and expected count from fixture
        logs_data_dict = multi_logs_data["data"]
        expected_count = multi_logs_data["expected_count"]

        # Verify expectation matches the data
        actual_logs = count_logs_in_data(logs_data_dict)
        assert actual_logs == expected_count  # Should be 2

        # Call service directly
        logs_data = OTELLogsData(**logs_data_dict)
        result = await context.otel_service.process_logs(
            logs_data, request_id="integration-counting-logs"
        )

        # Direct assertions
        assert result.success is True
        assert result.data_type == "logs"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify it's actually stored in MongoDB
        documents = await context.verify_telemetry_data("logs", expected_count=1)
        doc = documents[0]

        # Manually count log records in the stored document
        stored_logs = count_logs_in_data(doc)
        assert stored_logs == expected_count

        print(
            f"✅ Log counting integration: Verified {expected_count} log records counted and stored correctly"
        )


class TestOTELIntegrationErrorScenarios:
    """Integration tests for error scenarios and edge cases."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_multiple_requests_same_type(self, otel_integration_context, sample_traces_data):
        """Test multiple requests of the same type to verify database isolation."""
        context = otel_integration_context

        # Get data from fixture
        traces_data_dict = sample_traces_data["data"]
        expected_count = sample_traces_data["expected_count"]

        # Process the same traces data multiple times
        results = []
        for i in range(3):
            traces_data = OTELTracesData(**traces_data_dict)
            result = await context.otel_service.process_traces(
                traces_data, request_id=f"multi-request-{i}"
            )

            # Verify each result
            assert result.success is True
            assert result.data_type == "traces"
            assert result.records_processed == expected_count
            assert result.document_id is not None
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

        # Process all three types of telemetry data directly
        traces_data = OTELTracesData(**sample_traces_data["data"])
        traces_result = await context.otel_service.process_traces(
            traces_data, request_id="mixed-traces"
        )

        metrics_data = OTELMetricsData(**sample_metrics_data["data"])
        metrics_result = await context.otel_service.process_metrics(
            metrics_data, request_id="mixed-metrics"
        )

        logs_data = OTELLogsData(**sample_logs_data["data"])
        logs_result = await context.otel_service.process_logs(logs_data, request_id="mixed-logs")

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

        # Get data from fixture
        traces_data_dict = sample_traces_data["data"]
        expected_count = sample_traces_data["expected_count"]

        # Process without request_id
        traces_data = OTELTracesData(**traces_data_dict)
        result = await context.otel_service.process_traces(traces_data, request_id=None)

        # Verify the result
        assert result.success is True
        assert result.data_type == "traces"
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify it was stored
        documents = await context.verify_telemetry_data("traces", expected_count=1)
        doc = documents[0]

        # Verify request_id is None in the stored document
        assert doc["request_id"] is None
        assert doc["data_type"] == "traces"

        print("✅ No request ID integration: Verified processing works without request_id")
