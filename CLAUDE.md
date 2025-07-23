# OpenTelemetry to MongoDB API

## Overview

Production-ready FastAPI service that accepts OpenTelemetry JSON data and writes to MongoDB with primary/secondary failover support.

**Endpoints**: `/v1/traces`, `/v1/metrics`, `/v1/logs`, `/health`, `/health/detailed`

## Architecture

```
app/
├── main.py           # FastAPI application, lifespan management
├── models.py         # Pydantic models for OTLP data/responses
├── mongo_client.py   # MongoDB client with primary/secondary support
├── otel_service.py   # OTEL data processing with telemetry counting
└── tests/           # Unit tests (mongomock) + integration tests (Docker)
```

**Data Flow**: FastAPI → OTELService → MongoDBClient → Primary/Secondary databases

## Configuration

Environment variables:
- `PRIMARY_MONGODB_URI` - Primary MongoDB connection string
- `SECONDARY_MONGODB_URI` - Secondary MongoDB connection string (optional)
- `MONGODB_DATABASE` - Database name (default: "otel_db")

## Technology Stack

**Core**: FastAPI, Pydantic, Motor (async MongoDB), structlog
**Testing**: pytest, mongomock, Docker containers
**Tooling**: Ruff (linting/formatting), mypy (type checking), pre-commit

## Development Standards

**Code Quality**:
- 100-character line length
- Python 3.12 type annotations
- Ruff for linting/formatting (replaces Black + isort + flake8)
- Structured logging with structlog (JSON output)
- Environment-based configuration

**Testing**:
- Unit tests: mongomock for fast execution
- Integration tests: Docker MongoDB containers
- Markers: `@pytest.mark.unit`, `@pytest.mark.integration`

## Development Workflow

### Setup
```bash
python -m venv venv && source venv/bin/activate
pip install -e .[dev]
pre-commit install
```

### Commands
```bash
# Code Quality
ruff check app/ --fix              # Linting with auto-fix
ruff format app/                   # Formatting
mypy app/                          # Type checking

# Testing
pytest -m unit                     # Unit tests (run first)
pytest -m integration              # Integration tests
pytest --cov=app --cov-report=html # Coverage

# Development
uvicorn app.main:app --reload --port 8000
```

## Key Implementation Notes

**Error Handling**: Global exception handler, graceful degradation with partial DB connectivity
**Database**: Primary/secondary MongoDB with automatic failover
**Logging**: Structured JSON logs with correlation IDs via structlog
**Testing**: Unit tests with mongomock, integration tests with Docker containers

## Next Priorities

- [ ] API authentication/authorization
- [ ] Rate limiting and request throttling
- [ ] OpenTelemetry self-instrumentation
- [ ] Circuit breaker pattern for DB resilience
- [ ] Dead letter queue for failed writes
