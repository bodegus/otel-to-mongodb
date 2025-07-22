# OpenTelemetry to MongoDB API - Project Specification

## Project Overview

This project implements a production-ready Python FastAPI service that accepts OpenTelemetry (OTEL) JSON documents and writes them to multiple MongoDB instances. The service supports local MongoDB storage with optional cloud synchronization, designed for deployment in Docker containers or AWS Fargate.

### Core Requirements
- **Simple API**: Accept JSON documents via FastAPI endpoints
- **Multi-MongoDB**: Write to local instances with optional cloud sync
- **Production Ready**: Comprehensive testing, error handling, observability
- **Containerized**: Docker deployment with Fargate support
- **Well Tested**: Extensive unit tests with MongoDB mocking, integration tests with ephemeral containers

## Reference Project Analysis

### Bridge Prototype Assessment

#### Current State (reference/bridge/)

**Strengths:**
- Dual database architecture (local + cloud) with graceful degradation
- Support for both JSON and protobuf OTLP formats
- Background sync service for eventual consistency
- Comprehensive health check endpoints
- Good test coverage with appropriate mocking
- Production-ready Docker configuration

**Architecture Components:**
- `main.py`: FastAPI application with OTLP endpoints (/v1/traces, /v1/metrics, /v1/logs)
- `database_manager.py`: Dual MongoDB backend with failure handling and dead letter queues
- `sync_service.py`: Background service for failed record recovery
- Test suite with mongomock for database operations

**Areas for Improvement:**
- Missing observability (metrics, tracing, structured logging)
- No authentication, authorization, or input validation
- Hard-coded configuration values
- Limited error recovery strategies
- No rate limiting or resource controls
- Missing integration tests with real databases
- Basic error handling without circuit breaker patterns

#### Dependencies Used:
```
fastapi==0.104.1
uvicorn==0.24.0
pymongo==4.6.0
motor==3.3.2
pydantic==2.5.0
opentelemetry-proto==1.20.0
pytest==7.4.3
pytest-asyncio==0.21.1
pytest-mock==3.12.0
```

### Good Python Project Patterns (Target Standards)

#### Project Structure:
```
app/
├── subgraphs/                     # Modular components
│   ├── base/                      # Core framework
│   │   ├── litellm_component.py   # Primary base class
│   │   ├── state.py              # BaseState with annotations
│   │   ├── mongodb_manager.py    # Connection management
│   │   └── prompt_manager.py     # Template management
│   └── production_components/     # Feature implementations
├── tests/                        # Comprehensive testing
│   ├── fixtures/                 # MongoDB and test data
│   │   ├── mongodb_fixtures.py   # Database test utilities
│   │   └── prompt_seeds.py       # Test data generation
│   ├── subgraphs/               # Component-specific tests
│   └── conftest.py              # Global test configuration
└── utils/                        # Helper utilities
```

#### Key Patterns and Standards:

**Build System (pyproject.toml):**
- Modern setuptools with PEP 621 metadata
- Python 3.12 requirement
- Comprehensive dependency management with pinned versions
- Tool configuration integration (Black, isort, mypy, flake8, pytest)
- Coverage configuration with source path specification

**MongoDB Testing Strategy:**
- Dual MongoDB setup: Test MongoDB (port 27018) with Atlas Local for features
- Automatic test isolation with worker-aware database namespacing
- Comprehensive fixtures: `mongodb_test_db`, `mongodb_collections`, `seeded_mongodb_collections`
- Atlas Local container for advanced MongoDB features (search/vector)
- Cleanup automation with `--keep-db` flag for debugging

**Connection Management:**
- Singleton `MongoDBManager` class with thread-safe implementation
- Auto-configuration via environment variables (`MONGODB_URI`, `MONGODB_DATABASE`)
- Connection pooling with configured timeouts
- Health monitoring with automatic reconnection
- Retry logic with exponential backoff (`@mongodb_retry` decorator)

**Testing Architecture:**
- Marker-based organization: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.requires_mongodb`
- Parallel execution with `-n auto` for performance
- Mock strategies: Auto-mocking for unit tests, dependency injection for integration
- 3A pattern: Arrange, Act, Assert structure
- Naming convention: `test_<method>_<condition>_<expected>`

**Development Tools:**
- Pre-commit hooks with Black (line length 100), isort, flake8
- Type checking with mypy strict settings
- Automated unit testing on every commit
- Virtual environment mandatory for all operations

**Code Quality Standards:**
- 100-character line length across all tools
- Modern Python 3.12 features
- Comprehensive type annotations
- Structured logging and observability
- Environment-based configuration

## Major Epic Specifications

### Epic 1: Bootstrap Project Setup

**Objective:** Create a modern Python development environment following established patterns

**Acceptance Criteria:**
- [ ] `pyproject.toml` with modern build system configuration
- [ ] `.python-version` file specifying Python 3.12
- [ ] Virtual environment setup with `requirements.txt` and `constraints.txt`
- [ ] Pre-commit hooks configured (Black, isort, flake8, mypy)
- [ ] VS Code workspace configuration with Python debugging
- [ ] Directory structure following good patterns (`app/`, `tests/`, `utils/`)
- [ ] Basic `.gitignore` for Python projects
- [ ] `conftest.py` with MongoDB testing fixtures
- [ ] CI/CD foundation (GitHub Actions or equivalent)

**Key Dependencies:**
```
# Core API
fastapi>=0.104.0
uvicorn>=0.24.0
pydantic>=2.5.0

# Database
pymongo>=4.6.0
motor>=3.3.0

# Testing
pytest>=7.4.0
pytest-asyncio>=0.21.0
pytest-mock>=3.12.0
pytest-xdist>=3.0.0  # For parallel testing
mongomock>=4.1.0

# Development Tools
black>=23.0.0
isort>=5.12.0
flake8>=6.0.0
mypy>=1.7.0
pre-commit>=3.5.0
```

**Tool Configurations:**
- Black: 100-character line length
- isort: Black profile compatibility
- flake8: Ignore E203, W503 for Black compatibility
- mypy: Strict type checking enabled
- pytest: Parallel execution with `-n auto`

### Epic 2: Copy and Refactor Original API

**Objective:** Modernize the bridge prototype following good Python patterns

**Acceptance Criteria:**
- [ ] FastAPI application structure in `app/api/` directory
- [ ] Pydantic models for all request/response schemas
- [ ] Environment-based configuration (no hardcoded values)
- [ ] Proper error handling with custom exception classes
- [ ] Structured logging throughout the application
- [ ] OpenTelemetry integration for observability
- [ ] Health check endpoints with detailed status
- [ ] API documentation with OpenAPI/Swagger
- [ ] Input validation and sanitization
- [ ] Rate limiting and basic security measures

**API Structure:**
```
app/
├── api/
│   ├── __init__.py
│   ├── main.py              # FastAPI application
│   ├── endpoints/
│   │   ├── __init__.py
│   │   ├── health.py        # Health check endpoints
│   │   ├── telemetry.py     # OTEL data endpoints
│   │   └── admin.py         # Administrative endpoints
│   ├── models/
│   │   ├── __init__.py
│   │   ├── requests.py      # Request schemas
│   │   ├── responses.py     # Response schemas
│   │   └── telemetry.py     # OTEL data models
│   └── middleware/
│       ├── __init__.py
│       ├── logging.py       # Request logging
│       ├── error_handler.py # Global error handling
│       └── rate_limiting.py # Rate limiting
├── core/
│   ├── __init__.py
│   ├── config.py           # Environment configuration
│   ├── database.py         # MongoDB manager
│   ├── exceptions.py       # Custom exceptions
│   └── sync_service.py     # Background sync
└── utils/
    ├── __init__.py
    ├── logging.py          # Logging utilities
    └── retry.py            # Retry decorators
```

**Key Improvements from Prototype:**
- Replace hardcoded values with environment configuration
- Add comprehensive input validation with Pydantic
- Implement structured logging with correlation IDs
- Add OpenTelemetry tracing to all operations
- Create custom exception hierarchy for better error handling
- Implement circuit breaker pattern for cloud connectivity
- Add rate limiting to prevent abuse
- Include API versioning strategy

### Epic 3: Comprehensive Unit Test Suite

**Objective:** Create thorough unit tests with full MongoDB mocking following established patterns

**Acceptance Criteria:**
- [ ] 95%+ code coverage across all modules
- [ ] MongoDB operations fully mocked using mongomock
- [ ] Parametrized tests for multiple input scenarios
- [ ] Exception path testing with proper error conditions
- [ ] FastAPI test client integration for endpoint testing
- [ ] Mock strategies for external dependencies
- [ ] Test fixtures for common data scenarios
- [ ] Performance benchmarks for critical paths
- [ ] Parallel test execution with worker isolation
- [ ] Comprehensive API endpoint testing (happy path and error cases)

**Test Structure:**
```
tests/
├── unit/
│   ├── __init__.py
│   ├── api/
│   │   ├── test_endpoints.py      # API endpoint unit tests
│   │   ├── test_middleware.py     # Middleware unit tests
│   │   └── test_models.py         # Pydantic model tests
│   ├── core/
│   │   ├── test_database.py       # Database manager tests
│   │   ├── test_config.py         # Configuration tests
│   │   └── test_sync_service.py   # Sync service tests
│   └── utils/
│       ├── test_logging.py        # Logging utility tests
│       └── test_retry.py          # Retry decorator tests
├── fixtures/
│   ├── __init__.py
│   ├── mongodb_fixtures.py        # MongoDB test utilities
│   ├── api_fixtures.py            # API test data
│   └── telemetry_fixtures.py      # OTEL test data
├── conftest.py                     # Global test configuration
└── test_template.py               # Test template for consistency
```

**MongoDB Mocking Strategy:**
- Use mongomock for all database operations in unit tests
- Create comprehensive fixtures for different data scenarios
- Mock connection failures and retry scenarios
- Test dead letter queue operations
- Validate sync metadata handling
- Test batch operations and pagination

**Key Testing Patterns:**
- 3A pattern: Arrange, Act, Assert
- Descriptive test names: `test_<method>_<condition>_<expected>`
- Parametrized testing for multiple input scenarios
- Exception testing with `pytest.raises`
- Mock verification for external calls
- Fixture reuse for common setup

### Epic 4: Integration Test Suite with Ephemeral MongoDB

**Objective:** Create standalone integration tests using ephemeral MongoDB Atlas container

**Acceptance Criteria:**
- [ ] Docker-based MongoDB Atlas Local container for testing
- [ ] End-to-end API testing with real MongoDB operations
- [ ] Multi-database scenario testing (local + cloud)
- [ ] Sync service integration testing
- [ ] Performance testing under load
- [ ] Container lifecycle management in tests
- [ ] Data persistence validation across restarts
- [ ] Network failure simulation and recovery testing
- [ ] Memory and resource usage monitoring
- [ ] Test data cleanup and isolation

**Integration Test Structure:**
```
tests/
├── integration/
│   ├── __init__.py
│   ├── test_api_integration.py     # End-to-end API tests
│   ├── test_database_integration.py # Real MongoDB operations
│   ├── test_sync_integration.py    # Multi-database sync testing
│   ├── test_performance.py         # Load and performance tests
│   └── test_resilience.py          # Failure recovery tests
├── containers/
│   ├── __init__.py
│   ├── mongodb_container.py        # Container management
│   └── docker-compose.test.yml     # Test infrastructure
└── data/
    ├── sample_traces.json          # Test OTEL data
    ├── sample_metrics.json
    └── sample_logs.json
```

**Container Strategy:**
- MongoDB Atlas Local container with authentication
- Separate test databases for parallel execution
- Automatic container cleanup after tests
- Health check validation before test execution
- Volume mounting for test data injection
- Network isolation for security testing

**Performance Testing:**
- Load testing with concurrent requests
- Memory usage monitoring during batch operations
- Database connection pool validation
- Sync service performance under load
- Rate limiting effectiveness testing

## Technical Implementation Standards

### Code Organization Principles

**Module Structure:**
- Single responsibility per module
- Clear separation of concerns (API, core, utils)
- Dependency injection for testability
- Interface-based design for swappable components

**Error Handling:**
- Custom exception hierarchy with specific error types
- Global exception handling with structured logging
- Graceful degradation for non-critical failures
- Circuit breaker pattern for external dependencies

**Configuration Management:**
- Environment-based configuration with `.env` support
- Type-safe configuration with Pydantic models
- Secret management for sensitive values
- Default values for development convenience

### Development Workflow

**Required Commands:**
```bash
# Environment setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .

# Development
pre-commit install
pre-commit run --all-files

# Testing
pytest -n auto                    # Parallel unit tests
pytest -m integration            # Integration tests only
pytest --cov=app --cov-report=html  # Coverage report

# Quality checks
black app/ tests/
isort app/ tests/
flake8 app/ tests/
mypy app/

# Local development
uvicorn app.api.main:app --reload --port 8000
```

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
