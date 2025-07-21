"""FastAPI application for OpenTelemetry to MongoDB collection."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
import structlog

from .mongo_client import MongoDBClient, get_mongodb_client
from .otel_service import OTELService
from .models import (OTELTracesData, OTELMetricsData,
                     OTELLogsData, ErrorResponse)

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
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
        description="Simple API for collecting OpenTelemetry data",
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
                success=False,
                message="Internal server error",
                error_code="INTERNAL_ERROR"
            ).model_dump()
        )

    # Health endpoints
    @app.get("/health")
    async def health_check():
        """Basic health check."""
        return {"status": "healthy", "service": "otel-to-mongodb-api"}

    @app.get("/health/detailed")
    async def detailed_health_check(
        mongodb_client: MongoDBClient = Depends(get_mongodb_client)
    ):
        """Detailed health check with database status."""
        health_status = await mongodb_client.health_check()
        is_healthy = health_status["local"]["connected"]
        status = "healthy" if is_healthy else "unhealthy"
        return {
            "status": status,
            "local_database": health_status["local"],
            "cloud_database": health_status["cloud"],
        }

    # Telemetry endpoints
    @app.post("/v1/traces")
    async def submit_traces(
        traces_data: OTELTracesData,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client)
    ):
        """Submit OpenTelemetry traces."""
        service = OTELService(mongodb_client)
        result = await service.process_traces(traces_data)
        return result

    @app.post("/v1/metrics")
    async def submit_metrics(
        metrics_data: OTELMetricsData,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client)
    ):
        """Submit OpenTelemetry metrics."""
        service = OTELService(mongodb_client)
        result = await service.process_metrics(metrics_data)
        return result

    @app.post("/v1/logs")
    async def submit_logs(
        logs_data: OTELLogsData,
        mongodb_client: MongoDBClient = Depends(get_mongodb_client)
    ):
        """Submit OpenTelemetry logs."""
        service = OTELService(mongodb_client)
        result = await service.process_logs(logs_data)
        return result

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
