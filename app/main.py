"""
FastAPI application for OpenTelemetry to MongoDB collection.

This is an OTLP (OpenTelemetry Protocol) receiver that:
- Accepts OTLP data in JSON or protobuf format via HTTP POST
- Provides OTLP-compliant response messages
- Stores telemetry data in MongoDB (local and optionally cloud)
- Supports traces, metrics, and logs
- Handles content-type based routing (application/json, application/x-protobuf)
"""

import logging
from contextlib import asynccontextmanager

import structlog
from fastapi import Depends, FastAPI, Request
from google.protobuf.json_format import MessageToDict
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from .handlers import (
    ProtobufParsingError,
    register_exception_handlers,
    unsupported_content_type_error,
)
from .models import OTELLogsData, OTELMetricsData, OTELTracesData
from .mongo_client import MongoDBClient, get_mongodb_client
from .otel_service import OTELService


# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting OTEL to MongoDB API")

    # Initialize MongoDB connections
    mongodb_client = MongoDBClient()
    await mongodb_client.connect()
    app.state.mongodb_client = mongodb_client

    yield

    # Cleanup
    logger.info("Shutting down OTEL to MongoDB API")
    await mongodb_client.disconnect()


def create_app() -> FastAPI:  # noqa: PLR0915
    """
    Create FastAPI application.

    Nesting in a method allows for easier mocking.
    """
    app = FastAPI(
        title="OpenTelemetry to MongoDB API",
        description="JSON-based OTLP receiver for OpenTelemetry data",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Register exception handlers
    register_exception_handlers(app)

    # Health endpoints
    @app.get("/health")
    async def health_check():
        """Return basic health status."""
        return {"status": "healthy", "service": "otel-to-mongodb-api"}

    @app.get("/health/detailed")
    async def detailed_health_check(mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
        """Detailed health check with database status."""
        health_status = await mongodb_client.health_check()
        # Healthy if ANY database is connected
        primary_healthy = health_status["primary"]["connected"]
        secondary_healthy = health_status["secondary"]["connected"]
        is_healthy = primary_healthy or secondary_healthy
        status = "healthy" if is_healthy else "unhealthy"
        return {
            "status": status,
            "primary_database": health_status["primary"],
            "secondary_database": health_status["secondary"],
        }

    # Telemetry endpoints
    @app.post("/v1/traces")
    async def submit_traces(
        request: Request,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client),
    ):
        """Submit OpenTelemetry traces (JSON or protobuf format)."""
        # Get and normalize content type, default to json
        content_type = request.headers.get("content-type", "application/json")
        content_type = content_type.split(";")[0].strip().lower()

        # Parse based on content type
        if "application/json" in content_type:
            json_data = await request.json()
            traces_data = OTELTracesData(**json_data)
        elif "application/x-protobuf" in content_type:
            raw_data = await request.body()
            if not raw_data:
                raise ProtobufParsingError("Empty protobuf data")

            # Parse protobuf directly using Google's MessageToDict
            pb_request = ExportTraceServiceRequest()
            pb_request.ParseFromString(raw_data)
            traces_dict = MessageToDict(
                pb_request,
                preserving_proto_field_name=False,  # Use camelCase for Pydantic aliases
                use_integers_for_enums=True,
            )
            traces_data = OTELTracesData(**traces_dict)
        else:
            raise unsupported_content_type_error(content_type)

        service = OTELService(mongodb_client)
        await service.process_traces(traces_data)

        # Return OTLP-compliant response (success case)
        return {}

    @app.post("/v1/metrics")
    async def submit_metrics(
        request: Request,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client),
    ):
        """Submit OpenTelemetry metrics (JSON or protobuf format)."""
        # Get and normalize content type
        content_type = request.headers.get("content-type", "application/json")
        content_type = content_type.split(";")[0].strip().lower()

        # Parse based on content type
        if "application/json" in content_type:
            json_data = await request.json()
            metrics_data = OTELMetricsData(**json_data)
        elif "application/x-protobuf" in content_type:
            raw_data = await request.body()
            if not raw_data:
                raise ProtobufParsingError("Empty protobuf data")

            # Parse protobuf directly using Google's MessageToDict
            pb_request = ExportMetricsServiceRequest()
            pb_request.ParseFromString(raw_data)
            metrics_dict = MessageToDict(
                pb_request,
                preserving_proto_field_name=False,  # Use camelCase for Pydantic aliases
                use_integers_for_enums=True,
            )
            metrics_data = OTELMetricsData(**metrics_dict)
        else:
            raise unsupported_content_type_error(content_type)

        service = OTELService(mongodb_client)
        await service.process_metrics(metrics_data)

        # Return OTLP-compliant response (success case)
        return {}

    @app.post("/v1/logs")
    async def submit_logs(
        request: Request,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client),
    ):
        """Submit OpenTelemetry logs (JSON or protobuf format)."""
        # Get and normalize content type
        content_type = request.headers.get("content-type", "application/json")
        content_type = content_type.split(";")[0].strip().lower()

        # Parse based on content type
        if "application/json" in content_type:
            json_data = await request.json()
            logs_data = OTELLogsData(**json_data)
        elif "application/x-protobuf" in content_type:
            raw_data = await request.body()
            if not raw_data:
                raise ProtobufParsingError("Empty protobuf data")

            # Parse protobuf directly using Google's MessageToDict
            pb_request = ExportLogsServiceRequest()
            pb_request.ParseFromString(raw_data)
            logs_dict = MessageToDict(
                pb_request,
                preserving_proto_field_name=False,  # Use camelCase for Pydantic aliases
                use_integers_for_enums=True,
            )
            logs_data = OTELLogsData(**logs_dict)
        else:
            raise unsupported_content_type_error(content_type)

        service = OTELService(mongodb_client)
        await service.process_logs(logs_data)

        # Return OTLP-compliant response (success case)
        return {}

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
