"""Tests for Pydantic models edge cases and validation."""

import pytest
from pydantic import ValidationError

from app.models import OTELLogsData, OTELMetric, OTELMetricsData, OTELTracesData


class TestModelValidationEdgeCases:
    """Test edge cases in model validation."""

    @pytest.mark.unit
    def test_empty_metric_name_validation(self):
        """Test that empty metric names are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            OTELMetric(name="", description="test")

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "value_error"
        assert "cannot be empty" in errors[0]["msg"]

    @pytest.mark.unit
    def test_whitespace_only_metric_name_validation(self):
        """Test that whitespace-only metric names are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            OTELMetric(name="   ", description="test")

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "value_error"
        assert "cannot be empty" in errors[0]["msg"]

    @pytest.mark.unit
    def test_empty_resource_spans_validation(self):
        """Test that empty resourceSpans array is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            OTELTracesData(resourceSpans=[])

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "value_error"
        assert "resourceSpans cannot be empty" in errors[0]["msg"]

    @pytest.mark.unit
    def test_empty_resource_metrics_validation(self):
        """Test that empty resourceMetrics array is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            OTELMetricsData(resourceMetrics=[])

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "value_error"
        assert "resourceMetrics cannot be empty" in errors[0]["msg"]

    @pytest.mark.unit
    def test_empty_resource_logs_validation(self):
        """Test that empty resourceLogs array is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            OTELLogsData(resourceLogs=[])

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "value_error"
        assert "resourceLogs cannot be empty" in errors[0]["msg"]

    @pytest.mark.unit
    def test_valid_metric_name_with_whitespace(self):
        """Test that metric names with leading/trailing whitespace are trimmed."""
        metric = OTELMetric(name="  valid_name  ", description="test")
        assert metric.name == "valid_name"

    @pytest.mark.unit
    def test_valid_traces_data_creation(self, json_traces_data):
        """Test that valid traces data successfully creates model."""
        # This should hit the return v line in validate_non_empty for traces
        traces_data = OTELTracesData(**json_traces_data["data"])
        assert traces_data.resource_spans
        assert len(traces_data.resource_spans) > 0

    @pytest.mark.unit
    def test_valid_metrics_data_creation(self, json_metrics_data):
        """Test that valid metrics data successfully creates model."""
        # This should hit the return v line in validate_non_empty for metrics
        metrics_data = OTELMetricsData(**json_metrics_data["data"])
        assert metrics_data.resource_metrics
        assert len(metrics_data.resource_metrics) > 0

    @pytest.mark.unit
    def test_valid_logs_data_creation(self, json_logs_data):
        """Test that valid logs data successfully creates model."""
        # This should hit the return v line in validate_non_empty for logs
        logs_data = OTELLogsData(**json_logs_data["data"])
        assert logs_data.resource_logs
        assert len(logs_data.resource_logs) > 0
