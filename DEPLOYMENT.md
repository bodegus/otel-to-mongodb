# OpenTelemetry to MongoDB API - Deployment Guide

## Overview
Production-ready FastAPI service that accepts OpenTelemetry JSON and protobuf data and writes to MongoDB Atlas.

## Infrastructure Requirements

### Container Configuration
- **Base Image**: Python 3.12
- **Exposed Port**: `8083`
- **Health Check**: `GET /health`
- **Detailed Health**: `GET /health/detailed`

### Environment Variables

#### Required
- `PRIMARY_MONGODB_URI` - MongoDB Atlas connection string with authentication
  - Format: `mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority`
  - Must include database credentials and cluster endpoint

#### Optional
- `MONGODB_DATABASE` - Database name (default: `otel_db`)
  - Collections `traces`, `metrics`, `logs` are auto-created
  - Indexes on `created_at` are auto-created

#### Not Required for AWS Deployment
- `SECONDARY_MONGODB_URI` - Not needed for Atlas (only primary URI used)

### Network Configuration
- **Inbound Traffic**: Port `8083` (HTTP)
- **Outbound Traffic**:
  - MongoDB Atlas (port 27017 over TLS)
  - DNS resolution for cluster discovery

### Resource Requirements
- **CPU**: 0.5 vCPU minimum, 1 vCPU recommended
- **Memory**: 512MB minimum, 1GB recommended
- **Storage**: Minimal (stateless application)

### Security Considerations
- Application runs as non-root user (UID 1000)
- No sensitive data stored in container
- MongoDB credentials via environment variables only
- Structured JSON logging to stdout/stderr

## API Endpoints

### Data Ingestion
- `POST /v1/traces` - Accept OpenTelemetry trace data
- `POST /v1/metrics` - Accept OpenTelemetry metrics data
- `POST /v1/logs` - Accept OpenTelemetry log data

**Content Types Supported:**
- `application/json` (JSON format)
- `application/x-protobuf` (Protocol Buffers)

### Health & Monitoring
- `GET /health` - Basic health check
- `GET /health/detailed` - Database connectivity status

## Database Setup
- **Auto-Initialization**: Database, collections, and indexes created automatically
- **Collections**: `traces`, `metrics`, `logs`
- **Indexes**: `created_at` field for time-based queries
- **Connection Validation**: Health checks validate Atlas connectivity

## Logging
- **Format**: Structured JSON logs
- **Level**: INFO (configurable)
- **Output**: stdout/stderr (CloudWatch compatible)
- **Correlation**: Request IDs for tracing

## Monitoring Integration
- Health endpoint for load balancer checks
- Detailed health shows MongoDB Atlas connection status
- Structured logs for centralized monitoring
- Request correlation IDs for distributed tracing

## Example Docker Commands

### Build
```bash
docker build -t otel-to-mongodb:latest .
```

### Run (Local Testing)
```bash
docker run -d \
  --name otel-to-mongodb \
  -p 8083:8083 \
  -e PRIMARY_MONGODB_URI="mongodb+srv://user:pass@cluster.mongodb.net/" \
  -e MONGODB_DATABASE="otel_production" \
  --restart unless-stopped \
  otel-to-mongodb:latest
```

## Load Balancer Configuration
- **Health Check Path**: `/health`
- **Health Check Port**: `8083`
- **Expected Response**: `200 OK` with `{"status": "healthy"}`
- **Timeout**: 5 seconds
- **Interval**: 30 seconds

## AWS-Specific Considerations
- Use ECS/EKS for container orchestration
- Store MongoDB URI in AWS Secrets Manager or Parameter Store
- Configure CloudWatch for log aggregation
- Use Application Load Balancer for health checks
- Consider auto-scaling based on CPU/memory metrics
