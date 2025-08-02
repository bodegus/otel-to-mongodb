# Implementation Tasks: Protocol Buffer Support for OpenTelemetry API

## Task Overview
This implementation adds Protocol Buffer support to the existing JSON-only OpenTelemetry API by creating a thin protobuf parsing layer that converts binary OTLP messages to the same internal Pydantic models. All existing core logic, database operations, and response handling remain unchanged.

## Implementation Tasks

- [x] 1. Add Protocol Buffer dependency and project configuration
  - Add `opentelemetry-proto>=1.25.0` to pyproject.toml dependencies
  - Verify compatibility with existing dependencies
  - Update development tooling configuration for new import patterns
  - _Leverage: pyproject.toml existing dependency structure, ruff configuration_
  - _Requirements: 2.1, 5.1_

- [x] 2. Create Protocol Buffer parser module
  - Create `app/protobuf_parser.py` with ProtobufParser class
  - Implement `parse_traces()`, `parse_metrics()`, `parse_logs()` methods
  - Add protobuf-to-dict conversion functions for each telemetry type
  - Include comprehensive error handling with structured logging
  - _Leverage: app/models.py Pydantic model structures, structlog logging patterns_
  - _Requirements: 2.1, 2.2, 2.5, 4.2_

- [x] 3. Create content-type detection and routing handler
  - Create `app/content_handler.py` with ContentTypeHandler class
  - Implement content-type detection logic (JSON/protobuf/unsupported)
  - Add request data parsing method that routes to appropriate parser
  - Include HTTP 415 error handling for unsupported content types
  - _Leverage: app/main.py Request handling patterns, FastAPI error responses_
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [x] 4. Modify FastAPI endpoint handlers for dual content-type support
  - Update `/v1/traces` endpoint in app/main.py to handle both content types
  - Update `/v1/metrics` endpoint in app/main.py to handle both content types
  - Update `/v1/logs` endpoint in app/main.py to handle both content types
  - Maintain existing JSON processing path completely unchanged
  - _Leverage: app/main.py existing endpoint structure, dependency injection, error handling_
  - _Requirements: 1.1, 1.2, 3.1, 6.1, 6.2_

- [x] 5. Create protobuf test fixtures and sample data
  - Create `app/tests/fixtures/protobuf_data.py` with sample protobuf messages
  - Generate protobuf equivalents of existing JSON test data
  - Add protobuf serialization helpers for test data creation
  - Include edge cases: empty data, malformed data, large payloads
  - _Leverage: app/tests/fixtures/otel_data.py existing fixture patterns and data structures_
  - _Requirements: 7.3_

- [x] 6. Implement protobuf parser unit tests
  - Create `app/tests/test_protobuf_parser.py` with comprehensive test coverage
  - Test valid protobuf parsing for traces, metrics, logs
  - Test invalid protobuf data error handling and logging
  - Test protobuf-to-Pydantic model conversion accuracy
  - Test memory usage and performance characteristics
  - _Leverage: app/tests/test_otel_service.py testing patterns, pytest configuration, mongomock setup_
  - _Requirements: 7.1, 7.4_

- [x] 7. Implement content handler unit tests
  - Create `app/tests/test_content_handler.py` with content-type detection tests
  - Test JSON content-type routing (maintain existing behavior)
  - Test protobuf content-type routing
  - Test unsupported content-type error handling (HTTP 415)
  - Test missing content-type default behavior (JSON)
  - _Leverage: app/tests/test_main.py existing HTTP testing patterns, FastAPI test client_
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 7.1_

- [x] 8. Update main endpoint tests for dual content-type support
  - Modify `app/tests/test_main.py` to test both JSON and protobuf requests
  - Add protobuf-specific test cases for all three endpoints (/v1/traces, /v1/metrics, /v1/logs)
  - Test HTTP 415 responses for unsupported content types
  - Verify backward compatibility - existing JSON tests continue passing unchanged
  - Test content-type case sensitivity and edge cases
  - _Leverage: app/tests/test_main.py existing test structure, FastAPI TestClient, pytest fixtures_
  - _Requirements: 1.4, 6.1, 6.3, 6.4, 7.1_

- [x] 9. Create protobuf integration tests
  - Create `app/tests/integration/test_protobuf_integration.py` for end-to-end testing
  - Test complete protobuf workflow: request → parsing → validation → MongoDB storage
  - Test mixed JSON and protobuf requests to same service instance
  - Verify MongoDB document format consistency between JSON and protobuf inputs
  - Test primary/secondary database failover with protobuf data
  - _Leverage: app/tests/integration/test_integration.py Docker MongoDB setup, async test patterns_
  - _Requirements: 3.4, 7.2, 7.4_

- [x] 10. Update error handling for protobuf-specific scenarios
  - Enhance global exception handler in app/main.py for protobuf parsing errors
  - Add specific error messages for protobuf validation failures
  - Ensure protobuf errors use existing OTLP Status message format
  - Test error response consistency between JSON and protobuf parsing failures
  - _Leverage: app/main.py existing global exception handler (lines 80-88), Status and ErrorResponse models_
  - _Requirements: 2.4, 4.2, 4.3, 5.4_

- [x] 11. Run comprehensive testing and validation
  - Execute full test suite: `pytest -m unit` for fast unit tests
  - Execute integration tests: `pytest -m integration` with Docker containers
  - Run code quality checks: `ruff check app/ --fix` and `mypy app/`
  - Verify test coverage includes all new protobuf code paths
  - Validate backward compatibility - no existing functionality broken
  - _Leverage: pyproject.toml existing test configuration, ruff and mypy setup_
  - _Requirements: 6.1, 6.3, 7.1, 7.2, 7.5_

- [x] 12. Consolidate error handling with FastAPI middleware
  - Create `app/middleware/error_middleware.py` with centralized exception handling
  - Implement middleware for ProtobufParsingError, ValidationError, and HTTPException
  - Remove duplicate exception handlers from main.py (4 handlers → 1 middleware)
  - Update all modules to let exceptions bubble up to middleware
  - Test error response consistency and OTLP compliance
  - _Leverage: FastAPI middleware patterns, existing Status model_
  - _Requirements: Reduce error handling boilerplate (80-100 lines reduction)_

- [x] 13. Refactor test fixtures to eliminate duplication
  - Create parametrized fixtures in `app/tests/conftest.py` that generate both JSON and protobuf data
  - Replace separate JSON/protobuf fixtures with unified `@pytest.mark.parametrize` approach
  - Update all test files to use parametrized content-type testing
  - Remove redundant fixture files and consolidate test data generation
  - Maintain test coverage while reducing fixture code by 60-70%
  - _Leverage: pytest parametrize patterns, existing fixture infrastructure_
  - _Requirements: Eliminate fixture duplication (300-400 lines reduction)_

- [x] 14. Simplify ContentTypeHandler architecture
  - Move content-type detection logic directly into endpoint functions
  - Replace ContentTypeHandler class with simple utility functions
  - Eliminate unnecessary abstraction layer and dependency injection
  - Inline protobuf/JSON parsing logic in main.py endpoints
  - Reduce architectural complexity while maintaining functionality
  - _Leverage: FastAPI Request object, direct function calls_
  - _Requirements: Reduce over-engineering (100-150 lines reduction)_

- [ ] 15. Optimize ProtobufParser for simplicity
  - Refactor ProtobufParser methods to pure functions
  - Remove class-based structure and instance state
  - Inline smaller conversion methods to reduce function call overhead
  - Simplify protobuf-to-dict conversion with direct field mapping
  - Maintain parsing accuracy while reducing code complexity
  - _Leverage: Direct protobuf field access, simplified data structures_
  - _Requirements: Reduce parser complexity (100-150 lines reduction)_

- [ ] 16. Update imports and clean up unused code
  - Remove unused imports across all modified modules
  - Clean up deprecated error handling imports
  - Update test imports to reflect simplified fixture structure
  - Remove unused utility functions and helper methods
  - Run comprehensive linting to catch any orphaned code
  - _Leverage: ruff unused import detection, automated cleanup_
  - _Requirements: Final cleanup (30-60 lines reduction)_

- [ ] 17. Validate simplified implementation
  - Run full test suite to ensure no functionality regression
  - Verify error handling maintains OTLP compliance
  - Test performance impact of architectural changes
  - Validate backward compatibility with existing API contracts
  - Run integration tests with real protobuf and JSON data
  - Measure actual lines of code reduction achieved
  - _Leverage: existing test suite, pytest markers, Docker integration tests_
  - _Requirements: Ensure refactoring maintains all functionality_

## Task Dependencies

**Phase 1: Core Implementation (Tasks 1-11)**
- Tasks 1-3 can be completed in parallel (foundation components)
- Task 4 depends on tasks 2-3 (endpoint modification needs parser and handler)
- Tasks 5-7 can be completed in parallel (test infrastructure)
- Tasks 8-9 depend on tasks 4-7 (endpoint tests need working implementation and fixtures)
- Task 10 depends on tasks 1-9 (error handling needs complete implementation)
- Task 11 is final validation that depends on all previous tasks

**Phase 2: Simplification and Refactoring (Tasks 12-17)**
- Task 12 (error middleware) can be done independently and should be first
- Task 13 (test fixtures) can be done in parallel with task 12
- Task 14 (ContentTypeHandler) depends on task 12 (centralized error handling)
- Task 15 (ProtobufParser) can be done in parallel with task 14
- Task 16 (cleanup) depends on tasks 12-15 (needs all refactoring complete)
- Task 17 (validation) depends on tasks 12-16 (final verification of all changes)

## Success Criteria
- All existing JSON functionality remains unchanged and tests pass
- All three endpoints (/v1/traces, /v1/metrics, /v1/logs) accept both JSON and protobuf
- Protobuf requests convert to identical Pydantic models as JSON equivalents
- Error handling maintains OTLP compliance for both content types
- Test coverage includes comprehensive protobuf scenarios
- Code quality checks pass (ruff, mypy) with comprehensive test coverage
