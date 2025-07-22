"""Shared test utilities and assertion helpers."""

from app.models import OTELLogsData, OTELMetricsData, OTELTracesData


class OTELTestHelpers:
    """Shared test logic that can work with both mocked and real backends."""

    @staticmethod
    async def process_and_verify_traces(
        otel_service, traces_data_fixture, request_id=None, expected_span_count=None
    ):
        """
        Process traces and return both result and verification data.

        This method works with both mocked (unit test) and real (integration test) backends.
        traces_data_fixture: dict with 'data' and 'expected_count' keys
        """
        # Extract data and expected count from fixture
        traces_data_dict = traces_data_fixture["data"]
        expected_count = expected_span_count or traces_data_fixture["expected_count"]

        traces_data = OTELTracesData(**traces_data_dict)
        result = await otel_service.process_traces(traces_data, request_id=request_id)

        # Common assertions that work for both unit and integration tests
        assert result.success is True
        assert result.data_type == "traces"
        assert result.document_id is not None
        assert result.records_processed == expected_count

        return result

    @staticmethod
    async def process_and_verify_metrics(
        otel_service, metrics_data_fixture, request_id=None, expected_metric_count=None
    ):
        """
        Process metrics and return both result and verification data.

        This method works with both mocked (unit test) and real (integration test) backends.
        metrics_data_fixture: dict with 'data' and 'expected_count' keys
        """
        # Extract data and expected count from fixture
        metrics_data_dict = metrics_data_fixture["data"]
        expected_count = expected_metric_count or metrics_data_fixture["expected_count"]

        metrics_data = OTELMetricsData(**metrics_data_dict)
        result = await otel_service.process_metrics(metrics_data, request_id=request_id)

        # Common assertions
        assert result.success is True
        assert result.data_type == "metrics"
        assert result.document_id is not None
        assert result.records_processed == expected_count

        return result

    @staticmethod
    async def process_and_verify_logs(
        otel_service, logs_data_fixture, request_id=None, expected_log_count=None
    ):
        """
        Process logs and return both result and verification data.

        This method works with both mocked (unit test) and real (integration test) backends.
        logs_data_fixture: dict with 'data' and 'expected_count' keys
        """
        # Extract data and expected count from fixture
        logs_data_dict = logs_data_fixture["data"]
        expected_count = expected_log_count or logs_data_fixture["expected_count"]

        logs_data = OTELLogsData(**logs_data_dict)
        result = await otel_service.process_logs(logs_data, request_id=request_id)

        # Common assertions
        assert result.success is True
        assert result.data_type == "logs"
        assert result.document_id is not None
        assert result.records_processed == expected_count

        return result

    @staticmethod
    def verify_mock_calls(mock_mongodb_client, expected_data_type, expected_request_id=None):
        """
        Verify mock calls for unit tests.

        This is used only in unit tests to verify that the MongoDB client was called correctly.
        """
        # Verify the call was made
        mock_mongodb_client.write_telemetry_data.assert_called_once()

        # Check the arguments passed to write_telemetry_data
        call_args = mock_mongodb_client.write_telemetry_data.call_args
        assert call_args[1]["data_type"] == expected_data_type
        assert "data" in call_args[1]

        if expected_request_id is not None:
            assert call_args[1]["request_id"] == expected_request_id
        else:
            assert call_args[1]["request_id"] is None


# Simple counting utilities for integration tests that need to verify stored data
# These are only used for integration test verification of data stored in MongoDB
def count_spans_in_data(traces_data_dict):
    """Count total spans in traces data - for integration test verification only."""
    total = 0
    for resource_span in traces_data_dict.get("resourceSpans", []):
        for scope_span in resource_span.get("scopeSpans", []):
            total += len(scope_span.get("spans", []))
    return total


def count_metrics_in_data(metrics_data_dict):
    """Count total metrics in metrics data - for integration test verification only."""
    total = 0
    for resource_metric in metrics_data_dict.get("resourceMetrics", []):
        for scope_metric in resource_metric.get("scopeMetrics", []):
            total += len(scope_metric.get("metrics", []))
    return total


def count_logs_in_data(logs_data_dict):
    """Count total log records in logs data - for integration test verification only."""
    total = 0
    for resource_log in logs_data_dict.get("resourceLogs", []):
        for scope_log in resource_log.get("scopeLogs", []):
            total += len(scope_log.get("logRecords", []))
    return total


# Data validation utilities
def assert_otel_data_structure(data_dict, data_type):
    """Assert that OTEL data has the expected structure."""
    if data_type == "traces":
        assert "resourceSpans" in data_dict
        assert isinstance(data_dict["resourceSpans"], list)
        assert len(data_dict["resourceSpans"]) > 0
    elif data_type == "metrics":
        assert "resourceMetrics" in data_dict
        assert isinstance(data_dict["resourceMetrics"], list)
        assert len(data_dict["resourceMetrics"]) > 0
    elif data_type == "logs":
        assert "resourceLogs" in data_dict
        assert isinstance(data_dict["resourceLogs"], list)
        assert len(data_dict["resourceLogs"]) > 0
    else:
        raise ValueError(f"Unknown data type: {data_type}")


def extract_service_names(data_dict, data_type):
    """Extract service names from OTEL data for verification."""
    service_names = []

    if data_type == "traces":
        resources = data_dict.get("resourceSpans", [])
    elif data_type == "metrics":
        resources = data_dict.get("resourceMetrics", [])
    elif data_type == "logs":
        resources = data_dict.get("resourceLogs", [])
    else:
        return service_names

    for resource in resources:
        attributes = resource.get("resource", {}).get("attributes", [])
        for attr in attributes:
            if attr.get("key") == "service.name":
                service_names.append(attr.get("value", {}).get("stringValue"))

    return service_names
