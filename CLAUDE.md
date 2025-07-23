# OpenTelemetry to MongoDB API - Project Specification

## Project Overview

This project implements a production-ready Python FastAPI service that accepts OpenTelemetry (OTEL) JSON documents and writes them to MongoDB instances. The service supports primary/secondary MongoDB storage with automatic failover, designed for deployment in Docker containers or AWS Fargate.

### Core Requirements
- **Simple API**: Accept JSON documents via FastAPI endpoints (/v1/traces, /v1/metrics, /v1/logs)
- **Dual MongoDB**: Write to primary and secondary MongoDB instances with graceful degradation
- **Production Ready**: Comprehensive testing, error handling, structured logging with structlog
- **Containerized**: Docker deployment with Fargate support
- **Well Tested**: Extensive unit tests with MongoDB mocking, integration tests with real containers

## Current Project Status

### Implementation Progress

**✅ Completed Features:**
- FastAPI application with OTLP endpoints (/v1/traces, /v1/metrics, /v1/logs)
- Primary/secondary MongoDB architecture with graceful degradation
- Structured logging with structlog
- Comprehensive health check endpoints (/health, /health/detailed)
- OTLP-compliant request/response models using Pydantic
- Modern Python tooling with Ruff (unified linting/formatting)
- Unit tests with mongomock integration
- Integration tests with real MongoDB containers
- Pre-commit hooks and CI/CD pipeline

**Architecture Components:**
- `app/main.py`: FastAPI application with lifespan management and global exception handling
- `app/mongo_client.py`: MongoDB client with primary/secondary database support
- `app/otel_service.py`: OpenTelemetry data processing service with telemetry counting
- `app/models.py`: Pydantic models for OTLP data structures and responses
- `app/tests/`: Comprehensive test suite with unit and integration tests

**Current Strengths:**
- Clean separation of concerns between API, service, and database layers
- Environment-based configuration (PRIMARY_MONGODB_URI, SECONDARY_MONGODB_URI)
- Graceful degradation - service continues if only one database is available
- Detailed telemetry processing with record counting and timing metrics
- Modern Python 3.12 codebase with full type annotations

**🔄 Areas for Enhancement:**
- Authentication and authorization mechanisms
- Rate limiting and request throttling
- OpenTelemetry instrumentation for self-monitoring
- Circuit breaker pattern for database connectivity
- Dead letter queue for failed writes
- Background sync service for consistency recovery
- Performance optimization and caching strategies

### Current Technology Stack

**Core Dependencies:**
```
# API Framework
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
pydantic>=2.5.0
pydantic-settings>=2.1.0

# Database
pymongo>=4.6.0
motor>=3.3.0

# Observability
opentelemetry-api>=1.21.0
opentelemetry-sdk>=1.21.0
structlog>=23.2.0

# Utilities
tenacity>=8.2.0  # For retry logic
python-json-logger>=2.0.0
```

**Development Tools:**
```
# Testing
pytest>=7.4.0
pytest-asyncio>=0.21.0
pytest-mock>=3.12.0
pytest-xdist>=3.0.0  # Parallel testing
mongomock>=4.1.0

# Code Quality (Modern Unified Tooling)
ruff>=0.1.8  # Replaces black, isort, flake8, bandit
mypy>=1.7.0
pre-commit>=3.5.0
```

## Current Project Structure

**Actual Implementation Structure:**
```
otel-to-mongodb/
├── app/                          # Main application code
│   ├── __init__.py
│   ├── main.py                   # FastAPI application entry point
│   ├── models.py                 # Pydantic models for OTLP data
│   ├── mongo_client.py           # MongoDB client with primary/secondary support
│   ├── otel_service.py           # OpenTelemetry data processing service
│   └── tests/                    # Test suite
│       ├── __init__.py
│       ├── conftest.py           # Global test configuration
│       ├── fixtures/             # Test data and utilities
│       │   ├── __init__.py
│       │   └── otel_data.py      # OTEL test data fixtures
│       ├── integration/          # Integration tests
│       │   ├── __init__.py
│       │   ├── conftest.py       # Integration test config
│       │   ├── fixtures.py       # MongoDB container fixtures
│       │   └── test_integration.py # End-to-end tests
│       ├── test_main.py          # FastAPI endpoint tests
│       ├── test_mongo_client.py  # MongoDB client unit tests
│       └── test_otel_service.py  # OTEL service unit tests
├── pyproject.toml                # Modern Python project configuration
├── Dockerfile                    # Container configuration
├── .pre-commit-config.yaml       # Pre-commit hooks with Ruff
├── .vscode/                      # VS Code workspace settings
│   ├── settings.json
│   └── extensions.json
└── .github/workflows/            # CI/CD pipeline
    └── ci.yml
```

## Implementation Standards and Patterns

**Modern Build System (pyproject.toml):**
- PEP 621 compliant project metadata
- Python 3.12 requirement with modern features
- Unified tooling configuration (Ruff, mypy, pytest)
- Optional dependencies for development vs. production
- Script entry points for CLI tools

**Database Architecture:**
- Primary/secondary MongoDB client pattern with `MongoDBClient` class
- Environment-based configuration: `PRIMARY_MONGODB_URI`, `SECONDARY_MONGODB_URI`
- Graceful degradation - continues operation with single database
- Connection health monitoring with automatic failover
- Structured logging for all database operations

**Testing Strategy:**
- **Unit Tests**: Full MongoDB mocking with mongomock
- **Integration Tests**: Real MongoDB containers with Docker
- **Test Markers**: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.requires_mongodb`
- **Parallel Execution**: `pytest -n auto` for performance
- **Fixtures**: Centralized in `app/tests/fixtures/` with reusable OTEL data

**Modern Development Tooling:**
- **Ruff**: Unified linting, formatting, and import sorting (replaces Black + isort + flake8 + bandit)
- **MyPy**: Strict type checking with comprehensive annotations
- **Pre-commit**: Automated code quality checks on every commit
- **VS Code**: Configured workspace with Ruff integration and Python debugging

**Code Quality Standards:**
- 100-character line length consistently applied
- Comprehensive type annotations using Python 3.12 syntax
- Structured logging with structlog for JSON output
- Environment-driven configuration with sensible defaults
- Clean separation of concerns (API → Service → Database)

## Development Roadmap

### ✅ Epic 1: Bootstrap Project Setup (COMPLETED)

**Objective:** Create a modern Python development environment following established patterns

**✅ Completed Acceptance Criteria:**
- [x] `pyproject.toml` with modern build system configuration and PEP 621 metadata
- [x] Python 3.12 requirement with modern language features
- [x] Virtual environment setup with optional dev dependencies
- [x] Pre-commit hooks configured with Ruff (unified tooling)
- [x] VS Code workspace configuration with Ruff integration and Python debugging
- [x] Clean directory structure (`app/`, `app/tests/`)
- [x] Modern `.gitignore` for Python projects
- [x] `conftest.py` with MongoDB testing fixtures and Docker integration
- [x] CI/CD pipeline with GitHub Actions

**✅ Modern Technology Stack:**
- **Unified Tooling**: Ruff replaced Black + isort + flake8 + bandit for performance and consistency
- **Containerized Testing**: Docker integration for real MongoDB integration tests
- **Structured Logging**: structlog with JSON output for production observability
- **Type Safety**: Comprehensive mypy configuration with Python 3.12 features

### ✅ Epic 2: Core API Implementation (COMPLETED)

**Objective:** Implement production-ready OTLP API with modern Python patterns

**✅ Completed Acceptance Criteria:**
- [x] FastAPI application with clean architecture in `app/main.py`
- [x] Comprehensive Pydantic models for OTLP request/response schemas in `app/models.py`
- [x] Environment-based configuration (PRIMARY_MONGODB_URI, SECONDARY_MONGODB_URI)
- [x] Global exception handling with structured error responses
- [x] Structured logging with structlog throughout the application
- [x] Health check endpoints (/health, /health/detailed) with database status
- [x] Auto-generated API documentation with OpenAPI/Swagger
- [x] Comprehensive input validation through Pydantic models
- [x] OTLP-compliant endpoint responses

**✅ Implemented Architecture:**
- **Clean Separation**: API layer (`main.py`) → Service layer (`otel_service.py`) → Database layer (`mongo_client.py`)
- **OTLP Compliance**: Full support for OpenTelemetry JSON protocol with proper response formats
- **Dual Database Support**: Primary/secondary MongoDB with graceful degradation
- **Production Logging**: Structured JSON logs with correlation and timing metrics
- **Health Monitoring**: Basic and detailed health checks with database connection status

**🔄 Remaining Enhancements:**
- [ ] Authentication and authorization mechanisms
- [ ] Rate limiting and request throttling
- [ ] OpenTelemetry self-instrumentation
- [ ] Circuit breaker pattern for database resilience
- [ ] Custom exception hierarchy with specific error types
- [ ] API versioning strategy for future changes

### ✅ Epic 3: Comprehensive Unit Test Suite (COMPLETED)

**Objective:** Create thorough unit tests with full MongoDB mocking following established patterns

**✅ Completed Acceptance Criteria:**
- [x] Comprehensive unit tests for all modules (main, mongo_client, otel_service)
- [x] MongoDB operations fully mocked using mongomock
- [x] Parametrized tests for different OTEL data scenarios
- [x] Exception path testing with connection failures and database errors
- [x] FastAPI test client integration for all endpoint testing
- [x] Mock strategies with proper dependency injection
- [x] Centralized test fixtures in `app/tests/fixtures/otel_data.py`
- [x] Parallel test execution with `pytest -n auto`
- [x] Comprehensive API endpoint testing (success and error cases)

**✅ Implemented Test Architecture:**
```
app/tests/
├── __init__.py
├── conftest.py                     # Global test configuration with mongomock
├── fixtures/
│   ├── __init__.py
│   └── otel_data.py               # Comprehensive OTEL test data fixtures
├── integration/                    # Real MongoDB container tests
│   ├── __init__.py
│   ├── conftest.py                # Docker MongoDB fixtures
│   ├── fixtures.py                # Container management utilities
│   └── test_integration.py        # End-to-end integration tests
├── test_main.py                   # FastAPI endpoint unit tests
├── test_mongo_client.py           # MongoDB client unit tests
└── test_otel_service.py           # OTEL service unit tests
```

**✅ Implemented Testing Patterns:**
- **Mongomock Integration**: All MongoDB operations mocked for fast unit tests
- **Fixture Strategy**: Reusable OTEL data fixtures for traces, metrics, and logs
- **Connection Testing**: Mock database failures and recovery scenarios
- **Parametrized Tests**: Multiple data scenarios with `@pytest.mark.parametrize`
- **Fast Execution**: Unit tests run in under 10 seconds with parallel execution
- **Comprehensive Coverage**: All API endpoints, service methods, and database operations tested

### ✅ Epic 4: Integration Test Suite (COMPLETED)

**Objective:** Create standalone integration tests using real MongoDB containers

**✅ Completed Acceptance Criteria:**
- [x] Docker-based MongoDB container for testing with automatic lifecycle management
- [x] End-to-end API testing with real MongoDB operations and data persistence
- [x] Primary/secondary database scenario testing with failover validation
- [x] Service integration testing with actual OTEL data processing
- [x] Container lifecycle management in tests with proper cleanup
- [x] Data persistence validation across test scenarios
- [x] Test data isolation with unique database instances per test
- [x] Automated container setup and teardown

**✅ Implemented Integration Testing:**
- **Docker Integration**: MongoDB containers managed through pytest fixtures
- **Real Data Flow**: Complete OTEL data processing from API to database storage
- **Database Validation**: Verify actual document insertion and retrieval
- **Primary/Secondary Testing**: Test failover scenarios with database connection failures
- **Performance Validation**: Test processing times and resource usage
- **Clean Isolation**: Each test uses fresh database instances with proper cleanup

**🔄 Future Enhancements for Integration Testing:**
- [ ] Load testing with concurrent requests and high throughput scenarios
- [ ] Memory usage monitoring during batch operations
- [ ] Network failure simulation and recovery testing
- [ ] Performance benchmarking with SLA validation
- [ ] Multi-container scenarios for distributed testing

## Development Workflow

### Environment Setup
```bash
# Python 3.12 environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -e .[dev]

# Setup pre-commit hooks
pre-commit install
```

### Development Commands
```bash
# Code Quality (Unified with Ruff)
ruff check app/ --fix              # Linting with auto-fix
ruff format app/                   # Code formatting
mypy app/                          # Type checking

# Testing
pytest -n auto                    # Parallel unit tests
pytest -m integration            # Integration tests only
pytest --cov=app --cov-report=html  # Coverage report

# Pre-commit (runs automatically on commit)
pre-commit run --all-files         # Manual run

# Local development server
uvicorn app.main:app --reload --port 8000
```

### Current Implementation Principles

**✅ Module Architecture:**
- **Clean Separation**: `main.py` (API) → `otel_service.py` (Business Logic) → `mongo_client.py` (Data Layer)
- **Single Responsibility**: Each module has a focused purpose
- **Dependency Injection**: Services receive dependencies via constructors
- **Type Safety**: Comprehensive type annotations throughout

**✅ Error Handling:**
- **Global Exception Handler**: Catches all unhandled exceptions with structured logging
- **Graceful Degradation**: Service continues with partial database connectivity
- **OTLP Compliance**: Error responses follow OpenTelemetry protocol standards
- **Structured Logging**: All errors logged with context and correlation IDs

**✅ Configuration Management:**
- **Environment Variables**: `PRIMARY_MONGODB_URI`, `SECONDARY_MONGODB_URI`, `MONGODB_DATABASE`
- **Sensible Defaults**: Development-friendly defaults for optional configuration
- **No Hardcoded Values**: All configuration externalized

**Git Workflow:**
- Feature branches with descriptive names
- Pre-commit hooks for code quality
- Conventional commit messages
- Pull request reviews required
- Automated testing in CI/CD

### Deployment Considerations

**Docker Configuration:**
- Multi-stage build for optimization
- Non-root user for security
- Health checks for container orchestration
- Environment variable configuration
- Secrets management integration

**AWS Fargate Deployment:**
- Task definition with resource limits
- Service discovery configuration
- Load balancer integration
- Auto-scaling policies
- Monitoring and alerting setup

## Success Criteria

**Epic 1 Complete When:**
- All development tools configured and working
- Project structure matches established patterns
- Pre-commit hooks passing on sample code
- Virtual environment and dependencies installed
- Basic CI/CD pipeline executing

**Epic 2 Complete When:**
- All API endpoints implemented and documented
- Configuration externalized and type-safe
- Error handling comprehensive and tested
- Logging structured and correlated
- OpenTelemetry integration functional

**Epic 3 Complete When:**
- 95%+ test coverage achieved
- All MongoDB operations mocked appropriately
- Parallel test execution under 30 seconds
- Exception paths thoroughly tested
- Performance benchmarks established

**Epic 4 Complete When:**
- Integration tests passing with real MongoDB
- Performance requirements met under load
- Resilience testing validates recovery scenarios
- Container lifecycle automated and reliable
- End-to-end workflows validated

**Project Complete When:**
- All acceptance criteria met
- Documentation comprehensive and current
- Deployment pipeline functional
- Monitoring and alerting operational
- Performance SLAs satisfied

## Next Steps

1. **Review and approve this CLAUDE.md specification**
2. **Begin Epic 1: Bootstrap Project Setup**
3. **Iterative development with testing at each stage**
4. **Regular reviews against acceptance criteria**
5. **Performance validation throughout development**

This specification serves as the definitive guide for transforming the prototype into a production-ready service following established Python best practices and patterns.
