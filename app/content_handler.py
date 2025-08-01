"""Request content parsing utilities."""

import structlog
from fastapi import HTTPException, Request
from pydantic import ValidationError

from .models import OTELLogsData, OTELMetricsData, OTELTracesData
from .protobuf_parser import ProtobufParser, ProtobufParsingError


logger = structlog.get_logger()


async def parse_request_data(
    request: Request, data_type: str
) -> OTELTracesData | OTELMetricsData | OTELLogsData:
    """Parse request data based on content type."""
    # Get and normalize content type
    content_type = request.headers.get("content-type", "application/json")
    content_type = content_type.split(";")[0].strip().lower()

    if not content_type:
        content_type = "application/json"

    logger.debug(
        "Detected content type",
        content_type=content_type,
        raw_header=request.headers.get("content-type"),
    )
    logger.info(
        "Processing request with content type", content_type=content_type, data_type=data_type
    )

    # Model mapping
    models = {"traces": OTELTracesData, "metrics": OTELMetricsData, "logs": OTELLogsData}
    if data_type not in models:
        raise ValueError(f"Unknown data type: {data_type}")

    model_class = models[data_type]

    if content_type == "application/json":
        # Parse JSON data
        try:
            json_data = await request.json()
            return model_class(**json_data)
        except ValidationError:
            logger.error("JSON validation failed", data_type=data_type, exc_info=True)
            raise
        except (ValueError, UnicodeDecodeError) as e:
            logger.error("JSON parsing failed", error=str(e), data_type=data_type)
            raise HTTPException(status_code=422, detail=f"Invalid JSON: {e!s}") from e

    elif content_type == "application/x-protobuf":
        # Parse protobuf data
        try:
            raw_data = await request.body()
            if not raw_data:
                raise ProtobufParsingError("Empty protobuf data")

            parser = ProtobufParser()
            if data_type == "traces":
                return parser.parse_traces(raw_data)
            elif data_type == "metrics":
                return parser.parse_metrics(raw_data)
            elif data_type == "logs":
                return parser.parse_logs(raw_data)
        except ProtobufParsingError:
            logger.error("Protobuf parsing failed", data_type=data_type, exc_info=True)
            raise

    else:
        # Unsupported content type
        error_msg = f"Unsupported content type: {content_type}. Supported types: application/json, application/x-protobuf"
        logger.warning("Unsupported content type", content_type=content_type, data_type=data_type)
        raise HTTPException(
            status_code=415,
            detail=error_msg,
            headers={"Accept": "application/json, application/x-protobuf"},
        )
