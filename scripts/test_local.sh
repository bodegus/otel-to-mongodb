#!/bin/bash

# Test local Docker deployment with protobuf
# This tests against the locally running Docker container on port 8083

# Local endpoint configuration
API_ENDPOINT="http://localhost:8083"

# Check if lambda_test_event.json exists
if [ ! -f "lambda_test_event.json" ]; then
    echo "Error: lambda_test_event.json not found in current directory"
    echo "Please ensure you have the test event file"
    exit 1
fi

# Extract base64 protobuf data from lambda test event
BASE64_DATA=$(jq -r '.body' lambda_test_event.json)

if [ -z "$BASE64_DATA" ] || [ "$BASE64_DATA" = "null" ]; then
    echo "Error: Could not extract body from lambda_test_event.json"
    exit 1
fi

# Decode base64 to binary file
echo "$BASE64_DATA" | base64 -d > temp_protobuf.bin

echo "Testing local Docker deployment with protobuf data..."
echo "Endpoint: $API_ENDPOINT"
echo "Protobuf size: $(stat -f%z temp_protobuf.bin 2>/dev/null || stat -c%s temp_protobuf.bin) bytes"
echo ""

# Send request to local Docker container
echo "Sending POST request to ${API_ENDPOINT}/v1/metrics..."
curl -X POST "${API_ENDPOINT}/v1/metrics" \
    -H "Content-Type: application/x-protobuf" \
    -H "User-Agent: OTel-OTLP-Exporter-JavaScript/0.200.0" \
    -H "Accept: */*" \
    --data-binary @temp_protobuf.bin \
    --verbose \
    --output local_response.json

echo ""
echo "Response saved to: local_response.json"
echo "Response:"
cat local_response.json | jq . 2>/dev/null || cat local_response.json

# Cleanup
rm -f temp_protobuf.bin

echo ""
echo "This tests the local deployment path:"
echo "curl -> localhost:8083 -> FastAPI in Docker container"