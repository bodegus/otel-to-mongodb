"""FastAPI application for OpenTelemetry to MongoDB collection.

This is a JSON-only OTLP (OpenTelemetry Protocol) receiver that:
- Accepts OTLP data in JSON format via HTTP POST
- Provides OTLP-compliant response messages
- Stores telemetry data in MongoDB (local and optionally cloud)
- Supports traces, metrics, and logs

Note: Binary protobuf format is not supported - only JSON.
"""

import logging
from contextlib import asynccontextmanager

import structlog
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

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


def create_app() -> FastAPI:
    """Create FastAPI application.
    Nesting in a metho allows for easier mocking"""
    app = FastAPI(
        title="OpenTelemetry to MongoDB API",
        description="JSON-based OTLP receiver for OpenTelemetry data",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Exception handler
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
        """Basic health check."""
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
        traces_data: OTELTracesData, mongodb_client: MongoDBClient = Depends(get_mongodb_client)
    ):
        """Submit OpenTelemetry traces (JSON format only)."""
        try:
            service = OTELService(mongodb_client)
            await service.process_traces(traces_data)

            # Return OTLP-compliant response (success case)
            return ExportTraceServiceResponse()

        except Exception as e:
            logger.error("Failed to process traces", error=str(e))
            error_msg = f"Internal server error: {str(e)}"
            return JSONResponse(status_code=500, content=Status(message=error_msg).model_dump())

    @app.post("/v1/metrics", response_model=ExportMetricsServiceResponse)
    async def submit_metrics(
        metrics_data: OTELMetricsData, mongodb_client: MongoDBClient = Depends(get_mongodb_client)
    ):
        """Submit OpenTelemetry metrics (JSON format only)."""
        try:
            service = OTELService(mongodb_client)
            await service.process_metrics(metrics_data)

            # Return OTLP-compliant response (success case)
            return ExportMetricsServiceResponse()

        except Exception as e:
            logger.error("Failed to process metrics", error=str(e))
            error_msg = f"Internal server error: {str(e)}"
            return JSONResponse(status_code=500, content=Status(message=error_msg).model_dump())

    @app.post("/v1/logs", response_model=ExportLogsServiceResponse)
    async def submit_logs(
        logs_data: OTELLogsData, mongodb_client: MongoDBClient = Depends(get_mongodb_client)
    ):
        """Submit OpenTelemetry logs (JSON format only)."""
        try:
            service = OTELService(mongodb_client)
            await service.process_logs(logs_data)

            # Return OTLP-compliant response (success case)
            return ExportLogsServiceResponse()

        except Exception as e:
            logger.error("Failed to process logs", error=str(e))
            error_msg = f"Internal server error: {str(e)}"
            return JSONResponse(status_code=500, content=Status(message=error_msg).model_dump())

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
