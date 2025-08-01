"""
Unified MongoDB integration tests for OTEL service.

These tests verify the complete pipeline from OTEL service through to MongoDB
using real containers and direct service calls, testing both JSON and protobuf
formats automatically through parametrized fixtures.
"""

import pytest

from app.models import OTELLogsData, OTELMetricsData, OTELTracesData

# Import integration fixtures
from .fixtures import otel_integration_context


# Prevent unused import warnings for pytest fixtures
__all__ = [
    "otel_integration_context",
]


# Model mapping for telemetry types
TELEMETRY_MODELS = {
    "traces": OTELTracesData,
    "metrics": OTELMetricsData,
    "logs": OTELLogsData,
}


async def parse_protobuf_data_via_handler(binary_data, data_type):
    """Helper to parse protobuf data using the main parsing function."""
    from unittest.mock import AsyncMock, Mock

    from app.content_handler import parse_request_data

    request = Mock()
    request.headers = {"content-type": "application/x-protobuf"}
    request.body = AsyncMock(return_value=binary_data)

    return await parse_request_data(request, data_type)


async def process_telemetry_data(context, telemetry_data, data_type, request_id):
    """Helper to process telemetry data through the service layer."""
    if data_type == "traces":
        return await context.otel_service.process_traces(telemetry_data, request_id=request_id)
    elif data_type == "metrics":
        return await context.otel_service.process_metrics(telemetry_data, request_id=request_id)
    elif data_type == "logs":
        return await context.otel_service.process_logs(telemetry_data, request_id=request_id)
    else:
        raise ValueError(f"Unknown data type: {data_type}")


def extract_service_names_from_fixture_data(fixture_data, data_type):
    """Extract expected service names from fixture data for validation."""
    service_names = set()
    data = fixture_data["data"]

    if data_type == "traces":
        resource_key = "resourceSpans"
    elif data_type == "metrics":
        resource_key = "resourceMetrics"
    elif data_type == "logs":
        resource_key = "resourceLogs"
    else:
        return []

    for resource_item in data.get(resource_key, []):
        resource = resource_item.get("resource", {})
        for attr in resource.get("attributes", []):
            if attr.get("key") == "service.name":
                service_names.add(attr["value"]["stringValue"])

    return list(service_names)


def validate_stored_data_against_fixture(stored_doc, fixture_data, data_type):
    """Validate that stored MongoDB document matches expectations from fixture data."""
    expected_count = fixture_data["expected_count"]

    # Resource key mapping
    resource_key = f"resource{data_type.title()}" if data_type != "traces" else "resourceSpans"
    scope_key = f"scope{data_type.title()}" if data_type != "traces" else "scopeSpans"
    record_key = {"traces": "spans", "metrics": "metrics", "logs": "logRecords"}[data_type]

    # Validate structure exists
    assert resource_key in stored_doc

    # Count actual records
    actual_count = sum(
        len(scope_item[record_key])
        for resource_item in stored_doc[resource_key]
        for scope_item in resource_item[scope_key]
    )
    assert actual_count == expected_count

    # Validate service name exists (from test fixtures)
    service_names = {
        attr["value"]["stringValue"]
        for resource_item in stored_doc[resource_key]
        for attr in resource_item.get("resource", {}).get("attributes", [])
        if attr.get("key") == "service.name"
    }
    assert "test-service" in service_names


class TestUnifiedOTELIntegration:
    """Unified integration tests using parametrized fixtures for all telemetry types."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_traces_integration_unified(self, otel_integration_context, unified_traces_data):
        """Test traces processing with both JSON and protobuf formats automatically."""
        await self._test_telemetry_integration(
            otel_integration_context, "traces", unified_traces_data
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_metrics_integration_unified(
        self, otel_integration_context, unified_metrics_data
    ):
        """Test metrics processing with both JSON and protobuf formats automatically."""
        await self._test_telemetry_integration(
            otel_integration_context, "metrics", unified_metrics_data
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_logs_integration_unified(self, otel_integration_context, unified_logs_data):
        """Test logs processing with both JSON and protobuf formats automatically."""
        await self._test_telemetry_integration(otel_integration_context, "logs", unified_logs_data)

    async def _test_telemetry_integration(self, context, data_type, fixture_data):
        """Helper method for unified telemetry integration testing."""
        content_type = fixture_data["content_type"]
        expected_count = fixture_data["expected_count"]

        # Parse data based on content type
        if content_type == "application/json":
            model_class = TELEMETRY_MODELS[data_type]
            telemetry_data = model_class(**fixture_data["data"])
        else:
            telemetry_data = await parse_protobuf_data_via_handler(
                fixture_data["binary_data"], data_type
            )

        # Process data
        request_id = f"unified-{data_type}-{content_type.split('/')[-1]}"
        result = await process_telemetry_data(context, telemetry_data, data_type, request_id)

        # Validate results
        assert result.success is True
        assert result.data_type == data_type
        assert result.records_processed == expected_count
        assert result.document_id is not None

        # Verify MongoDB storage using specific request_id
        documents = await context.verify_telemetry_data(
            data_type, expected_count=1, request_id=request_id
        )
        assert documents[0]["data_type"] == data_type
        validate_stored_data_against_fixture(documents[0], fixture_data, data_type)

        print(
            f"✅ Unified {data_type} integration ({content_type}): {expected_count} records validated"
        )


class TestFormatConsistencyIntegration:
    """Test consistency between JSON and protobuf processing results."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_traces_json_protobuf_consistency(
        self, otel_integration_context, json_traces_data, protobuf_traces_data
    ):
        """Verify MongoDB documents are consistent between JSON and protobuf traces."""
        await self._test_format_consistency(
            otel_integration_context, "traces", json_traces_data, protobuf_traces_data
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_metrics_json_protobuf_consistency(
        self, otel_integration_context, json_metrics_data, protobuf_metrics_data
    ):
        """Verify MongoDB documents are consistent between JSON and protobuf metrics."""
        await self._test_format_consistency(
            otel_integration_context, "metrics", json_metrics_data, protobuf_metrics_data
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_logs_json_protobuf_consistency(
        self, otel_integration_context, json_logs_data, protobuf_logs_data
    ):
        """Verify MongoDB documents are consistent between JSON and protobuf logs."""
        await self._test_format_consistency(
            otel_integration_context, "logs", json_logs_data, protobuf_logs_data
        )

    async def _test_format_consistency(self, context, data_type, json_data, protobuf_data):
        """Helper method for format consistency testing."""
        # Process both formats
        json_request_id = f"consistency-json-{data_type}"
        protobuf_request_id = f"consistency-protobuf-{data_type}"

        model_class = TELEMETRY_MODELS[data_type]
        json_telemetry = model_class(**json_data["data"])
        json_result = await process_telemetry_data(
            context, json_telemetry, data_type, json_request_id
        )

        protobuf_telemetry = await parse_protobuf_data_via_handler(
            protobuf_data["binary_data"], data_type
        )
        protobuf_result = await process_telemetry_data(
            context, protobuf_telemetry, data_type, protobuf_request_id
        )

        # Validate consistency
        assert json_result.records_processed == protobuf_result.records_processed
        assert json_data["expected_count"] == protobuf_data["expected_count"]

        # Verify both documents using specific request_ids
        json_docs = await context.verify_telemetry_data(
            data_type, expected_count=1, request_id=json_request_id
        )
        protobuf_docs = await context.verify_telemetry_data(
            data_type, expected_count=1, request_id=protobuf_request_id
        )

        json_doc = json_docs[0]
        protobuf_doc = protobuf_docs[0]

        # Validate both against fixtures
        validate_stored_data_against_fixture(json_doc, json_data, data_type)
        validate_stored_data_against_fixture(protobuf_doc, protobuf_data, data_type)

        # Compare structure consistency
        resource_key = f"resource{data_type.title()}" if data_type != "traces" else "resourceSpans"
        assert len(json_doc[resource_key]) == len(protobuf_doc[resource_key])

        print(f"✅ JSON/Protobuf consistency test: {data_type} documents validated")


class TestMixedWorkflowsIntegration:
    """Test mixed workflows and complex scenarios."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_mixed_telemetry_types_and_formats(
        self, otel_integration_context, json_traces_data, protobuf_metrics_data, json_logs_data
    ):
        """Test processing different telemetry types and formats in the same session."""
        context = otel_integration_context

        # Process JSON traces
        traces_model = OTELTracesData(**json_traces_data["data"])
        traces_result = await context.otel_service.process_traces(
            traces_model, request_id="mixed-json-traces"
        )

        # Process protobuf metrics
        metrics_model = await parse_protobuf_data_via_handler(
            protobuf_metrics_data["binary_data"], "metrics"
        )
        metrics_result = await context.otel_service.process_metrics(
            metrics_model, request_id="mixed-protobuf-metrics"
        )

        # Process JSON logs
        logs_model = OTELLogsData(**json_logs_data["data"])
        logs_result = await context.otel_service.process_logs(
            logs_model, request_id="mixed-json-logs"
        )

        # Verify all succeeded
        assert traces_result.success is True
        assert metrics_result.success is True
        assert logs_result.success is True

        # Verify each stored in correct collection and validate against fixture data
        traces_docs = await context.verify_telemetry_data(
            "traces", expected_count=1, request_id="mixed-json-traces"
        )
        validate_stored_data_against_fixture(traces_docs[0], json_traces_data, "traces")

        metrics_docs = await context.verify_telemetry_data(
            "metrics", expected_count=1, request_id="mixed-protobuf-metrics"
        )
        validate_stored_data_against_fixture(metrics_docs[0], protobuf_metrics_data, "metrics")

        logs_docs = await context.verify_telemetry_data(
            "logs", expected_count=1, request_id="mixed-json-logs"
        )
        validate_stored_data_against_fixture(logs_docs[0], json_logs_data, "logs")

        print("✅ Mixed workflows: All telemetry types validated against their fixture data")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_multiple_requests_same_type(self, otel_integration_context, json_traces_data):
        """Test multiple requests of the same type to verify database isolation."""
        context = otel_integration_context
        expected_count = json_traces_data["expected_count"]

        # Process the same traces data multiple times
        results = []
        for i in range(3):
            traces_data = OTELTracesData(**json_traces_data["data"])
            result = await context.otel_service.process_traces(
                traces_data, request_id=f"multi-request-{i}"
            )

            # Verify each result matches fixture expectations
            assert result.success is True
            assert result.data_type == "traces"
            assert result.records_processed == expected_count
            assert result.document_id is not None
            results.append(result)

        # Verify each document individually by request_id for better isolation
        expected_request_ids = [f"multi-request-{i}" for i in range(3)]

        for request_id in expected_request_ids:
            docs = await context.verify_telemetry_data(
                "traces", expected_count=1, request_id=request_id
            )
            validate_stored_data_against_fixture(docs[0], json_traces_data, "traces")

        # Also verify total count as a sanity check
        all_docs = await context.verify_telemetry_data("traces", expected_count=3)
        request_ids = [doc["request_id"] for doc in all_docs]
        assert len(set(request_ids)) == 3  # All unique

        print("✅ Multiple requests: All 3 documents validated against fixture data")


class TestProtobufSpecificIntegration:
    """Tests specific to protobuf edge cases that can't be unified."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_malformed_protobuf_handling(
        self, otel_integration_context, malformed_protobuf_data
    ):
        """Test that malformed protobuf data is handled gracefully."""
        from app.protobuf_parser import ProtobufParsingError

        # Use fixture data for the test
        with pytest.raises(ProtobufParsingError) as exc_info:
            await parse_protobuf_data_via_handler(malformed_protobuf_data["binary_data"], "traces")

        # Validate error message matches fixture expectation
        assert "protobuf" in str(exc_info.value).lower()

        print("✅ Malformed protobuf: Error handling validated against fixture data")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_empty_protobuf_handling(
        self, otel_integration_context, empty_protobuf_traces_data
    ):
        """Test that empty protobuf data is handled gracefully."""
        from app.protobuf_parser import ProtobufParsingError

        # Use fixture data for the test
        with pytest.raises(ProtobufParsingError) as exc_info:
            await parse_protobuf_data_via_handler(
                empty_protobuf_traces_data["binary_data"], "traces"
            )

        assert "empty" in str(exc_info.value).lower() or "protobuf" in str(exc_info.value).lower()

        print("✅ Empty protobuf: Error handling validated against fixture data")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_large_protobuf_payload(
        self, otel_integration_context, large_protobuf_traces_data
    ):
        """Test that large protobuf payloads are processed successfully."""
        context = otel_integration_context

        # Parse the large protobuf data from fixture
        traces_data = await parse_protobuf_data_via_handler(
            large_protobuf_traces_data["binary_data"], "traces"
        )

        # Process through service
        result = await context.otel_service.process_traces(
            traces_data, request_id="large-protobuf-test"
        )

        # Verify processing succeeded with fixture expectations
        assert result.success is True
        assert result.records_processed == large_protobuf_traces_data["expected_count"]

        # Verify MongoDB storage matches fixture expectations
        documents = await context.verify_telemetry_data(
            "traces", expected_count=1, request_id="large-protobuf-test"
        )
        stored_doc = documents[0]

        # Validate against fixture data
        validate_stored_data_against_fixture(stored_doc, large_protobuf_traces_data, "traces")

        print(f"✅ Large protobuf: {result.records_processed} spans validated against fixture data")


class TestDatabaseFailoverIntegration:
    """Test database failover scenarios with fixture validation."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_primary_only_failover(self, otel_integration_context, json_traces_data):
        """Test data processing continues with primary DB when secondary is not available."""
        context = otel_integration_context

        # Process data using fixture
        traces_data = OTELTracesData(**json_traces_data["data"])
        result = await context.otel_service.process_traces(
            traces_data, request_id="failover-primary-only"
        )

        # Should succeed with primary only
        assert result.success is True
        assert result.primary_storage is True
        assert result.secondary_storage is False
        assert result.records_processed == json_traces_data["expected_count"]

        # Verify data was stored and matches fixture expectations
        documents = await context.verify_telemetry_data(
            "traces", expected_count=1, request_id="failover-primary-only"
        )
        stored_doc = documents[0]
        validate_stored_data_against_fixture(stored_doc, json_traces_data, "traces")

        print("✅ Failover test: Primary database storage validated against fixture data")
