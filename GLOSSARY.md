# Glossary

**OTLP receiver**: This FastAPI service's HTTP ingestion surface for OpenTelemetry traces, metrics and logs, accepting JSON and protobuf payloads. See [app/main.py](app/main.py).

**Trace payload**: Telemetry submitted to `/v1/traces` using the `resourceSpans → scopeSpans → spans` hierarchy. See [app/models.py](app/models.py) and [app/otel_service.py](app/otel_service.py).

**Metric payload**: Telemetry submitted to `/v1/metrics` using the `resourceMetrics → scopeMetrics → metrics` hierarchy. See [app/models.py](app/models.py) and [app/otel_service.py](app/otel_service.py).

**Log payload**: Telemetry submitted to `/v1/logs` using the `resourceLogs → scopeLogs → logRecords` hierarchy. See [app/models.py](app/models.py) and [app/otel_service.py](app/otel_service.py).

**Resource**: The attribute-bearing telemetry container represented by `OTELResource` and attached to each resource-level traces, metrics or logs group in [app/models.py](app/models.py).

**Instrumentation scope**: The name and optional version represented by `OTELScope`, grouping spans, metrics or log records within a resource in [app/models.py](app/models.py).

**Span**: A trace record containing trace/span identifiers, name, kind, start/end timestamps and optional attributes, represented by `OTELSpan` in [app/models.py](app/models.py).

**Attribute**: A telemetry key and typed-value dictionary represented by `OTELAttribute` in [app/models.py](app/models.py).

**Gauge**: A metric form holding data points, represented by `OTELGauge` in [app/models.py](app/models.py).

**Sum**: A metric form holding data points together with aggregation temporality and monotonicity, represented by `OTELSum` in [app/models.py](app/models.py).

**OTELService**: The processing layer that serializes validated telemetry with OTLP field aliases, counts records and delegates storage to the MongoDB client. See [app/otel_service.py](app/otel_service.py).

**Record count**: The number of innermost spans, metrics or log records counted across a payload's resource and scope groups by `OTELService._count_records`. See [app/otel_service.py](app/otel_service.py).

**Detailed health**: The `/health/detailed` response reporting both configured database connections and marking the service healthy when either connection is healthy. See [app/main.py](app/main.py).
