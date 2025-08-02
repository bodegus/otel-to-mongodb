# Remove old
docker stop otel-to-mongodb && docker rm otel-to-mongodb

# Build the image
docker build -t otel-to-mongodb:latest .

# Run container (connects to MongoDB on port 27017, avoids port collisions)
docker run -d \
  --name otel-to-mongodb \
  --env-file .env \
  -p 8083:8083 \
  --restart unless-stopped \
  otel-to-mongodb:latest

docker container ls
