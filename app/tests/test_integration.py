"""
Unified MongoDB integration tests for OTEL service.

These tests verify the complete pipeline from OTEL service through to MongoDB
using real containers and direct service calls, testing both JSON and protobuf
formats automatically through parametrized fixtures.
"""

import pytest

from app.models import OTELLogsData, OTELMetricsData, OTELTracesData


# Integration fixtures are now in conftest.py


# Note: otel_integration_context fixture is now imported from conftest.py


# Model mapping for telemetry types
TELEMETRY_MODELS = {
    "traces": OTELTracesData,
    "metrics": OTELMetricsData,
    "logs": OTELLogsData,
}


async def parse_protobuf_data_via_handler(binary_data, data_type):
    """Helper to parse protobuf data directly using MessageToDict."""
    from google.protobuf.json_format import MessageToDict
    from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceRequest,
    )
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

    from app.models import OTELLogsData, OTELMetricsData, OTELTracesData

    if not binary_data:
        raise ValueError("Empty protobuf data")

    if data_type == "traces":
        pb_request = ExportTraceServiceRequest()
        pb_request.ParseFromString(binary_data)
        traces_dict = MessageToDict(
            pb_request, preserving_proto_field_name=False, use_integers_for_enums=True
        )
        return OTELTracesData(**traces_dict)
    elif data_type == "metrics":
        pb_request = ExportMetricsServiceRequest()
        pb_request.ParseFromString(binary_data)
        metrics_dict = MessageToDict(
            pb_request, preserving_proto_field_name=False, use_integers_for_enums=True
        )
        return OTELMetricsData(**metrics_dict)
    elif data_type == "logs":
        pb_request = ExportLogsServiceRequest()
        pb_request.ParseFromString(binary_data)
        logs_dict = MessageToDict(
            pb_request, preserving_proto_field_name=False, use_integers_for_enums=True
        )
        return OTELLogsData(**logs_dict)
    else:
        raise ValueError(f"Unknown data type: {data_type}")


async def process_telemetry_data(context, telemetry_data, data_type, request_id):
    """Helper to process telemetry data through the service layer."""
    if data_type == "traces":
        await context.otel_service.process_traces(telemetry_data, request_id=request_id)
    elif data_type == "metrics":
        await context.otel_service.process_metrics(telemetry_data, request_id=request_id)
    elif data_type == "logs":
        await context.otel_service.process_logs(telemetry_data, request_id=request_id)
    else:
        raise ValueError(f"Unknown data type: {data_type}")

    # Success indicated by no exception raised


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
        await process_telemetry_data(context, telemetry_data, data_type, request_id)

        # Success indicated by no exception raised

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
        await process_telemetry_data(context, json_telemetry, data_type, json_request_id)

        protobuf_telemetry = await parse_protobuf_data_via_handler(
            protobuf_data["binary_data"], data_type
        )
        await process_telemetry_data(context, protobuf_telemetry, data_type, protobuf_request_id)

        # Validate consistency - both succeeded (no exceptions)
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
        await context.otel_service.process_traces(traces_model, request_id="mixed-json-traces")

        # Process protobuf metrics
        metrics_model = await parse_protobuf_data_via_handler(
            protobuf_metrics_data["binary_data"], "metrics"
        )
        await context.otel_service.process_metrics(
            metrics_model, request_id="mixed-protobuf-metrics"
        )

        # Process JSON logs
        logs_model = OTELLogsData(**json_logs_data["data"])
        await context.otel_service.process_logs(logs_model, request_id="mixed-json-logs")

        # All succeeded (no exceptions raised)

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

        # Process the same traces data multiple times
        for i in range(3):
            traces_data = OTELTracesData(**json_traces_data["data"])
            await context.otel_service.process_traces(traces_data, request_id=f"multi-request-{i}")
            # Success indicated by no exception raised

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
        # Use both context and fixture data for the test
        _ = otel_integration_context  # Mark as used
        with pytest.raises(Exception) as exc_info:
            await parse_protobuf_data_via_handler(malformed_protobuf_data["binary_data"], "traces")

        # Validate error message contains protobuf-related keywords
        error_msg = str(exc_info.value).lower()
        assert "proto" in error_msg or "parsing" in error_msg

        print("✅ Malformed protobuf: Error handling validated against fixture data")

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_empty_protobuf_handling(
        self, otel_integration_context, empty_protobuf_traces_data
    ):
        """Test that empty protobuf data is handled gracefully."""
        # Use both context and fixture data for the test
        _ = otel_integration_context  # Mark as used
        with pytest.raises(Exception) as exc_info:
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
        await context.otel_service.process_traces(traces_data, request_id="large-protobuf-test")

        # Success indicated by no exception raised

        # Verify MongoDB storage matches fixture expectations
        documents = await context.verify_telemetry_data(
            "traces", expected_count=1, request_id="large-protobuf-test"
        )
        stored_doc = documents[0]

        # Validate against fixture data
        validate_stored_data_against_fixture(stored_doc, large_protobuf_traces_data, "traces")

        expected_count = large_protobuf_traces_data["expected_count"]
        print(f"✅ Large protobuf: {expected_count} spans validated against fixture data")


class TestWritePersistenceIntegration:
    """Test write persistence to catch premature termination issues."""

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_write_persistence_with_immediate_verification(
        self, otel_integration_context, json_metrics_data
    ):
        """Test that writes are fully persisted before returning success."""
        context = otel_integration_context

        # Generate unique request ID with timestamp for precise tracking
        import time

        request_id = f"persistence-test-{int(time.time() * 1000)}"

        # Process data
        metrics_data = OTELMetricsData(**json_metrics_data["data"])
        await context.otel_service.process_metrics(metrics_data, request_id=request_id)

        # CRITICAL: Immediately verify data exists (no delay)
        # This catches the premature termination bug where process_metrics returns
        # success but data isn't actually persisted yet
        documents = await context.verify_telemetry_data(
            "metrics", expected_count=1, request_id=request_id
        )

        # Validate the document was actually written and persisted
        assert len(documents) == 1
        stored_doc = documents[0]
        assert stored_doc["request_id"] == request_id
        assert stored_doc["data_type"] == "metrics"

        # Validate against fixture data
        validate_stored_data_against_fixture(stored_doc, json_metrics_data, "metrics")

        print(
            f"✅ Write persistence: Data immediately available after process_metrics() with request_id={request_id}"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_write_concern_durability(self, otel_integration_context, json_traces_data):
        """Test that write concern ensures durability across connection changes."""
        import asyncio

        context = otel_integration_context

        request_id = f"durability-test-{int(asyncio.get_event_loop().time() * 1000)}"

        # Process data
        traces_data = OTELTracesData(**json_traces_data["data"])
        await context.otel_service.process_traces(traces_data, request_id=request_id)

        # Simulate connection pool refresh (like lambda container reuse)
        # This tests that data survives connection changes
        await context.mongo_client.disconnect()
        await context.mongo_client.connect()

        # Verify data still exists after connection refresh
        documents = await context.verify_telemetry_data(
            "traces", expected_count=1, request_id=request_id
        )

        assert len(documents) == 1
        stored_doc = documents[0]
        assert stored_doc["request_id"] == request_id
        validate_stored_data_against_fixture(stored_doc, json_traces_data, "traces")

        print(
            f"✅ Write durability: Data persisted across connection refresh with request_id={request_id}"
        )

    @pytest.mark.integration
    @pytest.mark.requires_mongodb
    @pytest.mark.asyncio
    async def test_concurrent_writes_persistence(self, otel_integration_context, json_logs_data):
        """Test that concurrent writes all persist correctly."""
        import asyncio

        context = otel_integration_context

        # Create multiple concurrent write tasks
        concurrent_count = 5
        request_ids = [
            f"concurrent-{i}-{int(asyncio.get_event_loop().time() * 1000)}"
            for i in range(concurrent_count)
        ]

        async def write_single_log(req_id):
            logs_data = OTELLogsData(**json_logs_data["data"])
            await context.otel_service.process_logs(logs_data, request_id=req_id)
            return req_id

        # Execute all writes concurrently
        tasks = [write_single_log(req_id) for req_id in request_ids]
        completed_request_ids = await asyncio.gather(*tasks)

        # Verify ALL writes persisted
        assert len(completed_request_ids) == concurrent_count

        for req_id in completed_request_ids:
            documents = await context.verify_telemetry_data(
                "logs", expected_count=1, request_id=req_id
            )
            assert len(documents) == 1
            assert documents[0]["request_id"] == req_id
            validate_stored_data_against_fixture(documents[0], json_logs_data, "logs")

        print(f"✅ Concurrent writes: All {concurrent_count} writes persisted correctly")


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
        await context.otel_service.process_traces(traces_data, request_id="failover-primary-only")

        # Success indicated by no exception raised (primary only working)

        # Verify data was stored and matches fixture expectations
        documents = await context.verify_telemetry_data(
            "traces", expected_count=1, request_id="failover-primary-only"
        )
        stored_doc = documents[0]
        validate_stored_data_against_fixture(stored_doc, json_traces_data, "traces")

        print("✅ Failover test: Primary database storage validated against fixture data")
