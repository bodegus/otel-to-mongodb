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
from pydantic import ValidationError

from .content_handler import ContentTypeHandler
from .models import (
    ErrorResponse,
    ExportLogsServiceResponse,
    ExportMetricsServiceResponse,
    ExportTraceServiceResponse,
    Status,
)
from .mongo_client import MongoDBClient, get_mongodb_client
from .otel_service import OTELService
from .protobuf_parser import ProtobufParsingError


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


# Global content handler instance
_content_handler: ContentTypeHandler | None = None


def get_content_handler() -> ContentTypeHandler:
    """Get ContentTypeHandler dependency."""
    global _content_handler
    if _content_handler is None:
        _content_handler = ContentTypeHandler()
    return _content_handler


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
        content_handler: ContentTypeHandler = Depends(get_content_handler),
    ):
        """Submit OpenTelemetry traces (JSON or protobuf format)."""
        try:
            # Parse request data based on content type
            traces_data = await content_handler.parse_request_data(request, "traces")

            service = OTELService(mongodb_client)
            await service.process_traces(traces_data)

            # Return OTLP-compliant response (success case)
            return ExportTraceServiceResponse()

        except HTTPException:
            # Let HTTPExceptions (like 422, 415) propagate to FastAPI
            raise
        except (ProtobufParsingError, ValidationError):
            # Let these exceptions propagate to their custom handlers
            raise
        except Exception as e:
            logger.error("Failed to process traces", error=str(e))
            error_msg = f"Internal server error: {e!s}"
            return JSONResponse(status_code=500, content=Status(message=error_msg).model_dump())

    @app.post("/v1/metrics", response_model=ExportMetricsServiceResponse)
    async def submit_metrics(
        request: Request,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client),
        content_handler: ContentTypeHandler = Depends(get_content_handler),
    ):
        """Submit OpenTelemetry metrics (JSON or protobuf format)."""
        try:
            # Parse request data based on content type
            metrics_data = await content_handler.parse_request_data(request, "metrics")

            service = OTELService(mongodb_client)
            await service.process_metrics(metrics_data)

            # Return OTLP-compliant response (success case)
            return ExportMetricsServiceResponse()

        except HTTPException:
            # Let HTTPExceptions (like 422, 415) propagate to FastAPI
            raise
        except (ProtobufParsingError, ValidationError):
            # Let these exceptions propagate to their custom handlers
            raise
        except Exception as e:
            logger.error("Failed to process metrics", error=str(e))
            error_msg = f"Internal server error: {e!s}"
            return JSONResponse(status_code=500, content=Status(message=error_msg).model_dump())

    @app.post("/v1/logs", response_model=ExportLogsServiceResponse)
    async def submit_logs(
        request: Request,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client),
        content_handler: ContentTypeHandler = Depends(get_content_handler),
    ):
        """Submit OpenTelemetry logs (JSON or protobuf format)."""
        try:
            # Parse request data based on content type
            logs_data = await content_handler.parse_request_data(request, "logs")

            service = OTELService(mongodb_client)
            await service.process_logs(logs_data)

            # Return OTLP-compliant response (success case)
            return ExportLogsServiceResponse()

        except HTTPException:
            # Let HTTPExceptions (like 422, 415) propagate to FastAPI
            raise
        except (ProtobufParsingError, ValidationError):
            # Let these exceptions propagate to their custom handlers
            raise
        except Exception as e:
            logger.error("Failed to process logs", error=str(e))
            error_msg = f"Internal server error: {e!s}"
            return JSONResponse(status_code=500, content=Status(message=error_msg).model_dump())

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
