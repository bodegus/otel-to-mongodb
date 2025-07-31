# Requirements Document: Protocol Buffer Support for OpenTelemetry API

## Introduction

This feature expands the OpenTelemetry to MongoDB API to support both JSON and Protocol Buffer (protobuf) content types for OTEL spans, metrics, and logs. Currently, the API only accepts JSON format (as noted in main.py:10 "Binary protobuf format is not supported - only JSON"). This enhancement will make the service compatible with standard OTLP clients that send binary protobuf data, significantly improving performance and reducing payload sizes.

## Codebase Reuse Analysis

**Existing Components to Leverage:**
- **FastAPI Application Structure** (app/main.py): Endpoint definitions, error handling, dependency injection
- **Pydantic Models** (app/models.py): OTEL data structures and validation logic
- **OTELService Processing** (app/otel_service.py): Core telemetry processing logic and counting methods
- **MongoDB Client** (app/mongo_client.py): Database storage with primary/secondary failover
- **Testing Framework** (app/tests/): Unit tests with mongomock, integration tests with Docker
- **Structured Logging** (structlog configuration): JSON logging with correlation IDs
- **Development Tooling** (pyproject.toml): Ruff linting, mypy type checking, pytest configuration

**Integration Points:**
- FastAPI endpoint handlers in main.py (lines 112-161)
- Content-Type detection and request parsing
- Pydantic model validation and conversion
- OTELService processing methods (process_traces, process_metrics, process_logs)

## Requirements

### Requirement 1: Content-Type Detection and Routing
**User Story:** As an OTLP client, I want to send telemetry data in either JSON or protobuf format, so that I can choose the most efficient encoding for my use case.

#### Acceptance Criteria
1. WHEN a client sends a request with `Content-Type: application/json` THEN the system SHALL process the data using existing JSON parsing
2. WHEN a client sends a request with `Content-Type: application/x-protobuf` THEN the system SHALL process the data using protobuf parsing
3. IF no Content-Type header is provided THEN the system SHALL default to JSON parsing for backward compatibility
4. IF an unsupported Content-Type is provided THEN the system SHALL return HTTP 415 Unsupported Media Type

### Requirement 2: Protocol Buffer Message Parsing
**User Story:** As a system administrator, I want the API to correctly parse OTLP protobuf messages, so that binary telemetry data is accurately converted to internal data structures.

#### Acceptance Criteria
1. WHEN protobuf traces data is received THEN the system SHALL parse it into OTELTracesData models
2. WHEN protobuf metrics data is received THEN the system SHALL parse it into OTELMetricsData models
3. WHEN protobuf logs data is received THEN the system SHALL parse it into OTELLogsData models
4. IF protobuf parsing fails due to malformed data THEN the system SHALL return HTTP 400 Bad Request with error details
5. WHEN protobuf data is successfully parsed THEN the system SHALL validate it using existing Pydantic models

### Requirement 3: Unified Processing Pipeline
**User Story:** As a developer, I want both JSON and protobuf data to follow the same processing pipeline, so that telemetry handling is consistent regardless of input format.

#### Acceptance Criteria
1. WHEN data is parsed from either JSON or protobuf THEN it SHALL be converted to the same internal Pydantic models
2. WHEN internal models are created THEN the system SHALL use existing OTELService processing methods
3. WHEN telemetry counting is performed THEN it SHALL use existing _count_spans, _count_metrics, _count_log_records methods
4. WHEN data is stored THEN it SHALL use the existing MongoDB client with primary/secondary failover
5. WHEN responses are generated THEN they SHALL use existing OTLP-compliant response models

### Requirement 4: Performance and Error Handling
**User Story:** As a system operator, I want protobuf processing to be performant and have proper error handling, so that the system remains reliable under load.

#### Acceptance Criteria
1. WHEN protobuf data is processed THEN it SHALL complete within the same performance characteristics as JSON processing
2. WHEN protobuf parsing errors occur THEN they SHALL be logged using existing structured logging
3. WHEN global exceptions occur during protobuf processing THEN they SHALL be handled by the existing global exception handler
4. IF memory usage becomes excessive THEN the system SHALL handle large protobuf payloads efficiently
5. WHEN processing succeeds THEN response times SHALL be tracked using existing processing_time_ms metrics

### Requirement 5: OTLP Protocol Compliance
**User Story:** As an OTLP client developer, I want the API to be fully compliant with OTLP specifications, so that standard OTLP libraries work seamlessly.

#### Acceptance Criteria
1. WHEN protobuf messages are received THEN they SHALL conform to official OTLP proto definitions
2. WHEN responses are sent THEN they SHALL match existing OTLP-compliant response formats
3. WHEN partial success occurs THEN the system SHALL use existing ExportTracePartialSuccess, ExportMetricsPartialSuccess, ExportLogsPartialSuccess models
4. IF data validation fails THEN error responses SHALL maintain OTLP Status message format
5. WHEN endpoints are accessed THEN they SHALL accept both content types on the same URLs (/v1/traces, /v1/metrics, /v1/logs)

### Requirement 6: Backward Compatibility
**User Story:** As an existing API user, I want JSON functionality to remain unchanged, so that my current integrations continue working without modification.

#### Acceptance Criteria
1. WHEN JSON requests are sent THEN they SHALL be processed exactly as before
2. WHEN no Content-Type is specified THEN the system SHALL default to JSON processing
3. WHEN existing clients connect THEN they SHALL experience no breaking changes
4. WHEN JSON validation fails THEN error messages SHALL remain the same format
5. IF new dependencies are added THEN they SHALL not affect JSON processing performance

### Requirement 7: Testing and Validation
**User Story:** As a developer, I want comprehensive tests for protobuf functionality, so that the feature is reliable and maintainable.

#### Acceptance Criteria
1. WHEN unit tests are run THEN they SHALL cover protobuf parsing, validation, and error cases
2. WHEN integration tests are run THEN they SHALL test end-to-end protobuf workflows with MongoDB
3. WHEN test fixtures are created THEN they SHALL include sample protobuf data for all telemetry types
4. IF protobuf functionality breaks THEN tests SHALL fail and provide clear error messages
5. WHEN code coverage is measured THEN protobuf code paths SHALL be included in coverage reports

## Edge Cases and Technical Constraints

### Content-Type Handling
- Mixed content types in batch requests (not supported - single content type per request)
- Malformed Content-Type headers (default to JSON with warning log)
- Case-insensitive Content-Type matching

### Data Size Limits
- Large protobuf payloads (leverage existing FastAPI request size limits)
- Memory efficiency for binary parsing vs JSON parsing
- Streaming vs buffered parsing for large payloads

### Error Scenarios
- Corrupt protobuf data (return 400 Bad Request)
- Version mismatches in protobuf schema (graceful degradation where possible)
- Network interruptions during binary data transfer

### Performance Considerations
- Protobuf parsing should be faster than JSON for large payloads
- Memory usage should be comparable or better than JSON
- Latency impact should be minimal for existing JSON workflows
