# OpenTelemetry to MongoDB API

Production-ready FastAPI service that accepts OpenTelemetry data (JSON and protobuf) and writes to MongoDB.

## Features

- Accepts OpenTelemetry traces, metrics, and logs
- Supports both JSON and protobuf formats
- Auto-creates database, collections, and indexes
- Health monitoring endpoints
- Structured JSON logging
- Docker-ready with health checks

## Quick Start

### Using Docker Compose (API+Database)

The easiest way to get started is with Docker Compose, which sets up both the API and MongoDB:

```bash
# Clone the repository
git clone https://github.com/bodegus/otel-to-mongodb.git
cd otel-to-mongodb

# Start the services
docker-compose up -d

# Verify the service is running
curl http://localhost:8083/health
```

The API will be available at `http://localhost:8083` with MongoDB at `localhost:27017`.

### Using Docker (API with MongoDB Atlas and Charts)

You can also use [MongoDB Atlas Free Tier](https://www.mongodb.com/docs/atlas/tutorial/deploy-free-tier-cluster/), and take advantage [free dashboard](https://www.mongodb.com/docs/charts/launch-charts/)!

```bash
# Build the image
docker build -t otel-to-mongodb:latest .

# Run the container
docker run -d \
  --name otel-to-mongodb \
  -p 8083:8083 \
  -e PRIMARY_MONGODB_URI="mongodb+srv://username:password@cluster.mongodb.net/" \
  -e SECONDARY_MONGODB_URI="mongodb://admin:password@mongodb:27017/otel_db?authSource=admin" \
  -e MONGODB_DATABASE="otel_db" \
  otel-to-mongodb:latest
```



### Testing with Claude Code

Configure Claude Code to send telemetry to your local instance.  You can add additional key/value attributes (like agent=$AGENT_LABEL)

```bash
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:8083"
export OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"
export OTEL_SERVICE_NAME="claude-code"
export OTEL_RESOURCE_ATTRIBUTES="service.name=claude-code,service.version=1.0.0,environment=local"
```



### Local Development

```bash
# Setup environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .[dev]

# Run tests
pytest -m unit          # Unit tests
pytest -m integration   # Integration tests (requires Docker)

# Start server
uvicorn app.main:app --reload --port 8083
```

## API Endpoints

### Data Ingestion
- `POST /v1/traces` - Submit OpenTelemetry trace data
- `POST /v1/metrics` - Submit OpenTelemetry metrics data
- `POST /v1/logs` - Submit OpenTelemetry log data

**Supported Content Types:**
- `application/json`
- `application/x-protobuf`

### Health Monitoring
- `GET /health` - Basic health check
- `GET /health/detailed` - Database connectivity status

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PRIMARY_MONGODB_URI` | Yes | - | MongoDB connection string |
| `MONGODB_DATABASE` | No | `otel_db` | Database name |
| `LOG_LEVEL` | No | `INFO` | Logging level |

### MongoDB Setup

The service automatically:
- Creates the database if it doesn't exist
- Creates collections: `traces`, `metrics`, `logs`
- Creates indexes on `created_at` field for efficient queries

## Testing with Sample Data

Send sample OpenTelemetry data:

```bash
# JSON format
curl -X POST http://localhost:8083/v1/traces \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": []}]}'

# Check health
curl http://localhost:8083/health/detailed
```


## Project Structure

```
app/
├── main.py             # FastAPI application
├── models.py           # Pydantic models for OTLP data
├── mongo_client.py     # MongoDB client with auto-initialization
├── otel_service.py     # OTEL data processing
├── content_handler.py  # Content type detection and parsing
└── handlers.py         # Exception handlers
```

## Development

### Code Quality

```bash
# Linting and formatting
ruff check app/ --fix
ruff format app/

# Run tests with coverage
pytest --cov=app --cov-report=html
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## License

MIT

## Support

For issues and questions, please use [GitHub Issues](https://github.com/yourusername/otel-to-mongodb/issues).
