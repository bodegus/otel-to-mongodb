"""Exception handlers for the FastAPI application."""

import structlog
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from google.protobuf.message import DecodeError
from pydantic import ValidationError

from .models import ErrorResponse, Status


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


async def json_parsing_exception_handler(request: Request, exc: ValueError):
    """Handle JSON parsing errors (ValueError, UnicodeDecodeError)."""
    logger.error("JSON parsing error", error=str(exc), exc_info=True)
    return JSONResponse(
        status_code=422,
        content={"detail": f"Invalid JSON: {exc!s}"},
    )


async def unicode_decode_exception_handler(request: Request, exc: UnicodeDecodeError):
    """Handle Unicode decode errors in JSON parsing."""
    logger.error("Unicode decode error", error=str(exc), exc_info=True)
    return JSONResponse(
        status_code=422,
        content={"detail": f"Invalid JSON: {exc!s}"},
    )


async def global_exception_handler(request: Request, exc: Exception):
    """Handle all unhandled exceptions."""
    logger.error("Unhandled exception", error=str(exc), exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            success=False, message="Internal server error", error_code="INTERNAL_ERROR"
        ).model_dump(),
    )


def register_exception_handlers(app):
    """Register all exception handlers with the FastAPI app."""
    app.exception_handler(ProtobufParsingError)(protobuf_parsing_exception_handler)
    app.exception_handler(DecodeError)(decode_error_exception_handler)
    app.exception_handler(ValidationError)(validation_exception_handler)
    app.exception_handler(ValueError)(json_parsing_exception_handler)
    app.exception_handler(UnicodeDecodeError)(unicode_decode_exception_handler)
    app.exception_handler(Exception)(global_exception_handler)
