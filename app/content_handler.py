"""Content-type detection and routing handler for OpenTelemetry data."""

import structlog
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .models import OTELLogsData, OTELMetricsData, OTELTracesData, Status
from .protobuf_parser import ProtobufParser, ProtobufParsingError


logger = structlog.get_logger()


class ContentTypeHandler:
    """Handles content-type detection and routing for OpenTelemetry data."""

    def __init__(self) -> None:
        self.protobuf_parser = ProtobufParser()

    async def parse_request_data(
        self,
        request: Request,
        data_type: str,
    ) -> OTELTracesData | OTELMetricsData | OTELLogsData:
        """Parse request data based on content type."""
        content_type = self._get_content_type(request)

        logger.info(
            "Processing request with content type",
            content_type=content_type,
            data_type=data_type,
        )

        if content_type == "application/json":
            return await self._parse_json_data(request, data_type)
        if content_type == "application/x-protobuf":
            return await self._parse_protobuf_data(request, data_type)
        # HTTP 415 Unsupported Media Type
        error_msg = (
            f"Unsupported content type: {content_type}. "
            "Supported types: application/json, application/x-protobuf"
        )
        logger.warning("Unsupported content type", content_type=content_type, data_type=data_type)
        raise HTTPException(
            status_code=415,
            detail=error_msg,
            headers={"Accept": "application/json, application/x-protobuf"},
        )

    def _get_content_type(self, request: Request) -> str:
        """Extract and normalize content type from request headers."""
        content_type = request.headers.get("content-type", "application/json")

        # Normalize content type (remove charset and other parameters)
        content_type = content_type.split(";")[0].strip().lower()

        # Default to JSON for backward compatibility
        if not content_type:
            content_type = "application/json"

        logger.debug(
            "Detected content type",
            content_type=content_type,
            raw_header=request.headers.get("content-type"),
        )
        return content_type

    async def _parse_json_data(
        self,
        request: Request,
        data_type: str,
    ) -> OTELTracesData | OTELMetricsData | OTELLogsData:
        """Parse JSON request data using existing FastAPI JSON parsing."""
        try:
            # Get JSON data from request body
            json_data = await request.json()

            # Convert to appropriate Pydantic model based on data type
            if data_type == "traces":
                return OTELTracesData(**json_data)
            if data_type == "metrics":
                return OTELMetricsData(**json_data)
            if data_type == "logs":
                return OTELLogsData(**json_data)
            raise ValueError(f"Unknown data type: {data_type}")

        except ValidationError as e:
            logger.error("JSON validation failed", error=str(e), data_type=data_type)
            # Re-raise ValidationError as HTTPException with 422 status to match FastAPI behavior
            # Convert validation errors to JSON-serializable format
            detail = []
            for error in e.errors():
                error_dict = dict(error)
                # Convert non-serializable objects to strings
                if "ctx" in error_dict and isinstance(error_dict["ctx"], dict):
                    for key, value in error_dict["ctx"].items():
                        if not isinstance(
                            value, str | int | float | bool | list | dict | type(None)
                        ):
                            error_dict["ctx"][key] = str(value)
                detail.append(error_dict)

            raise HTTPException(
                status_code=422,
                detail=detail,
            ) from e
        except (ValueError, UnicodeDecodeError) as e:
            logger.error("JSON parsing failed", error=str(e), data_type=data_type)
            # JSON decode errors should be 422 to match FastAPI behavior
            raise HTTPException(
                status_code=422,
                detail=f"Invalid JSON: {e!s}",
            ) from e
        except Exception as e:
            logger.error(
                "Unexpected error parsing JSON", error=str(e), data_type=data_type, exc_info=True
            )
            raise HTTPException(
                status_code=400,
                detail=f"Invalid JSON data: {e!s}",
            ) from e

    async def _parse_protobuf_data(
        self,
        request: Request,
        data_type: str,
    ) -> OTELTracesData | OTELMetricsData | OTELLogsData:
        """Parse protobuf request data using ProtobufParser."""
        try:
            # Get raw bytes from request body
            raw_data = await request.body()

            if not raw_data:
                raise ProtobufParsingError("Empty protobuf data")

            # Parse based on data type
            if data_type == "traces":
                return self.protobuf_parser.parse_traces(raw_data)
            if data_type == "metrics":
                return self.protobuf_parser.parse_metrics(raw_data)
            if data_type == "logs":
                return self.protobuf_parser.parse_logs(raw_data)
            raise ValueError(f"Unknown data type: {data_type}")

        except ProtobufParsingError as e:
            logger.error("Protobuf parsing failed", error=str(e), data_type=data_type)
            # Re-raise ProtobufParsingError to be caught by the custom exception handler
            raise
        except Exception as e:
            logger.error(
                "Unexpected error parsing protobuf",
                error=str(e),
                data_type=data_type,
                exc_info=True,
            )
            raise HTTPException(
                status_code=400,
                detail=f"Invalid protobuf data: {e!s}",
            ) from e

    def create_unsupported_media_type_response(self, content_type: str) -> JSONResponse:
        """Create HTTP 415 Unsupported Media Type response."""
        error_msg = f"Unsupported media type: {content_type}"
        logger.warning("Unsupported media type", content_type=content_type)

        return JSONResponse(
            status_code=415,
            content=Status(message=error_msg).model_dump(),
            headers={"Accept": "application/json, application/x-protobuf"},
        )
