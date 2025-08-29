#!/bin/bash

# Test API Gateway with protobuf using OTEL environment variables
# This mimics what Claude sends through API Gateway (not direct Lambda invoke)

# Use OTEL environment variables
API_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT}"
API_KEY=$(echo "${OTEL_EXPORTER_OTLP_HEADERS}" | grep -o 'x-api-key=[^,]*' | cut -d'=' -f2)

# Extract base64 protobuf data from lambda test event
BASE64_DATA=$(jq -r '.body' lambda_test_event.json)

# Decode base64 to binary file
echo "$BASE64_DATA" | base64 -d > temp_protobuf.bin

echo "Testing API Gateway with protobuf data..."
echo "Endpoint: $API_ENDPOINT"
echo "API Key: ${API_KEY:0:10}..."
echo "Protobuf size: $(stat -f%z temp_protobuf.bin 2>/dev/null || stat -c%s temp_protobuf.bin) bytes"

# Send request using OTEL environment settings
curl -X POST "${API_ENDPOINT}/v1/metrics" \
    -H "Content-Type: application/x-protobuf" \
    -H "User-Agent: OTel-OTLP-Exporter-JavaScript/0.200.0" \
    -H "x-api-key: ${API_KEY}" \
    -H "Accept: */*" \
    --data-binary @temp_protobuf.bin \
    --verbose \
    --output api_response.json

echo ""
echo "Response saved to: api_response.json"
echo "Response:"
cat api_response.json | jq . 2>/dev/null || cat api_response.json

# Cleanup
rm -f temp_protobuf.bin

echo ""
echo "This tests the same path as Claude:"
echo "curl -> API Gateway -> Lambda -> FastAPI (should show corruption if it exists)"