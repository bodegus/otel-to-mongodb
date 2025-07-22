# OpenTelemetry to MongoDB API

Simple FastAPI service that accepts OpenTelemetry JSON documents and writes them to MongoDB (local + optional cloud sync).

## Quick Start

### Development Setup

```bash
# Setup environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set environment variables
export MONGODB_URI="mongodb://localhost:27017"
export MONGODB_DATABASE="otel_db"
export ENABLE_CLOUD_SYNC="false"

# Run tests
pytest

# Start server
python -m app.main
```

### Environment Variables

```bash
# Required
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=otel_db

# Optional cloud sync
CLOUD_MONGODB_URI=mongodb://cloud:27017
CLOUD_MONGODB_DATABASE=otel_cloud_db
ENABLE_CLOUD_SYNC=true
```

## API Endpoints

- `POST /v1/traces` - Submit OpenTelemetry traces
- `POST /v1/metrics` - Submit OpenTelemetry metrics
- `POST /v1/logs` - Submit OpenTelemetry logs
- `GET /health` - Basic health check
- `GET /health/detailed` - Detailed health with database status

## Docker

```bash
docker build -t otel-to-mongodb .
docker run -p 8000:8000 -e MONGODB_URI=mongodb://host.docker.internal:27017 otel-to-mongodb
```

## Project Structure

```
app/
├── main.py          # FastAPI app and endpoints
├── otel_service.py  # OTEL data processing logic
├── mongo_client.py  # MongoDB dual database client
└── models.py        # Pydantic models for OTEL data
```

Simple, focused, and gets the job done.
