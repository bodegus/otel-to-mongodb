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
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from pydantic import ValidationError

from .models import (
    ErrorResponse,
    ExportLogsServiceResponse,
    ExportMetricsServiceResponse,
    ExportTraceServiceResponse,
    OTELLogsData,
    OTELMetricsData,
    OTELTracesData,
    Status,
)
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


class ProtobufParsingError(Exception):
    """Exception raised when protobuf parsing fails."""


def unsupported_content_type_error(content_type: str) -> HTTPException:
    """Create HTTP 415 error for unsupported content types."""
    return HTTPException(
        status_code=415,
        detail=f"Unsupported content type: {content_type}. Supported types: application/json, application/x-protobuf",
        headers={"Accept": "application/json, application/x-protobuf"},
    )


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

    # Exception handlers for specific error types
    @app.exception_handler(ProtobufParsingError)
    async def protobuf_parsing_exception_handler(request: Request, exc: ProtobufParsingError):
        """Handle protobuf parsing errors with OTLP-compliant error response."""
        logger.error("Protobuf parsing error", error=str(exc), exc_info=True)
        return JSONResponse(
            status_code=400,
            content=Status(
                code=3,  # INVALID_ARGUMENT in OTLP status codes
                message=f"Invalid protobuf data: {exc!s}",
                details=[
                    {"@type": "type.googleapis.com/google.rpc.BadRequest", "field_violations": []}
                ],
            ).model_dump(),
        )

    @app.exception_handler(DecodeError)
    async def decode_error_exception_handler(request: Request, exc: DecodeError):
        """Handle protobuf decode errors with OTLP-compliant error response."""
        logger.error("Protobuf decode error", error=str(exc), exc_info=True)
        return JSONResponse(
            status_code=400,
            content=Status(
                code=3,  # INVALID_ARGUMENT in OTLP status codes
                message=f"Invalid protobuf data: {exc!s}",
                details=[
                    {"@type": "type.googleapis.com/google.rpc.BadRequest", "field_violations": []}
                ],
            ).model_dump(),
        )

    @app.exception_handler(ValidationError)
    async def validation_exception_handler(request: Request, exc: ValidationError):
        """Handle Pydantic validation errors with OTLP-compliant error response."""
        logger.error("Validation error", error=str(exc), exc_info=True)
        # Extract field errors for OTLP-compliant response
        field_violations = []
        for error in exc.errors():
            field_violations.append(
                {"field": ".".join(str(loc) for loc in error["loc"]), "description": error["msg"]}
            )

        return JSONResponse(
            status_code=422,
            content=Status(
                code=3,  # INVALID_ARGUMENT in OTLP status codes
                message="Validation error in telemetry data",
                details=[
                    {
                        "@type": "type.googleapis.com/google.rpc.BadRequest",
                        "field_violations": field_violations,
                    }
                ],
            ).model_dump(),
        )

    @app.exception_handler(ValueError)
    async def json_parsing_exception_handler(request: Request, exc: ValueError):
        """Handle JSON parsing errors (ValueError, UnicodeDecodeError)."""
        logger.error("JSON parsing error", error=str(exc), exc_info=True)
        return JSONResponse(
            status_code=422,
            content={"detail": f"Invalid JSON: {exc!s}"},
        )

    @app.exception_handler(UnicodeDecodeError)
    async def unicode_decode_exception_handler(request: Request, exc: UnicodeDecodeError):
        """Handle Unicode decode errors in JSON parsing."""
        logger.error("Unicode decode error", error=str(exc), exc_info=True)
        return JSONResponse(
            status_code=422,
            content={"detail": f"Invalid JSON: {exc!s}"},
        )

    # General exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled exception", error=str(exc), exc_info=True)
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                success=False, message="Internal server error", error_code="INTERNAL_ERROR"
            ).model_dump(),
        )

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
    @app.post("/v1/traces", response_model=ExportTraceServiceResponse)
    async def submit_traces(
        request: Request,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client),
    ):
        """Submit OpenTelemetry traces (JSON or protobuf format)."""
        # Get and normalize content type
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
        return ExportTraceServiceResponse()

    @app.post("/v1/metrics", response_model=ExportMetricsServiceResponse)
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
        return ExportMetricsServiceResponse()

    @app.post("/v1/logs", response_model=ExportLogsServiceResponse)
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
        return ExportLogsServiceResponse()

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
