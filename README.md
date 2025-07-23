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

## Docker Deployment

### Quick Start with Docker

```bash
# Build the image
docker build -t otel-to-mongodb .

# Run container (connects to MongoDB on port 27017, avoids port collisions)
docker run -d \
  --name otel-to-mongodb \
  --network otel_otel-network \
  -p 8083:8083 \
  -e PRIMARY_MONGODB_URI=mongodb://otel-mongodb:27017/otel_db \
  -e MONGODB_DATABASE=otel_db \
  --restart unless-stopped \
  otel-to-mongodb
```

### Docker Compose Setup

For production deployment with MongoDB:

```yaml
version: '3.8'

services:
  otel-mongodb-api:
    build: .
    container_name: otel-mongodb-api
    ports:
      - "8083:8083"
    environment:
      - PRIMARY_MONGODB_URI=mongodb://mongodb:27017/otel_db
      - MONGODB_DATABASE=otel_db
    depends_on:
      - mongodb
    restart: unless-stopped
    networks:
      - otel-network

  mongodb:
    image: mongo:8.0
    container_name: otel-mongodb-new
    restart: always
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password
      MONGO_INITDB_DATABASE: otel_db
    ports:
      - "27018:27017"  # Different port to avoid collision
    volumes:
      - mongodb_api_data:/data/db
    networks:
      - otel-network
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  mongodb_api_data:

networks:
  otel-network:
    driver: bridge
```

### Connect to Existing MongoDB

To use your existing MongoDB container on port 27017:

```bash
docker run -d \
  --name otel-to-mongodb \
  --network otel_otel-network \
  -p 8083:8083 \
  -e PRIMARY_MONGODB_URI=mongodb://otel-mongodb:27017/otel_db \
  -e MONGODB_DATABASE=otel_db \
  --restart unless-stopped \
  otel-to-mongodb
```

### Health Check

Once running, verify the service:

```bash
# Basic health check
curl http://localhost:8083/health

# Detailed health check
curl http://localhost:8083/health/detailed
```

### Container Management

```bash
# View logs
docker logs otel-to-mongodb

# Stop container
docker stop otel-to-mongodb

# Remove container
docker rm otel-to-mongodb

# Remove image
docker rmi otel-to-mongodb
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
