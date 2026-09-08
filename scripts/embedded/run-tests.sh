#!/bin/sh
# Run the test suite on Linux, where the embedded MongoDB engine can be installed.
#
# The engine is a compiled extension: only Linux wheels are vendored, so on macOS the
# integration tests cannot run on the host and skip themselves. This runs them in a container
# built for the host's own architecture -- no emulation, and no MongoDB container.
#
# Any arguments are passed through to pytest:
#
#     ./scripts/embedded/run-tests.sh
#     ./scripts/embedded/run-tests.sh app/tests/test_integration.py -v
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)

# arm64 because that is the architecture the vendored wheel is built for -- native on an
# Apple Silicon host, and the same architecture CI runs on. On an x86_64 host this runs under
# emulation and will be slow; building an amd64 wheel for it is described in build-wheels.sh.
platform=${PLATFORM:-linux/arm64}

docker build \
    --platform "$platform" \
    -f "$repo_root/scripts/embedded/Dockerfile.test" \
    -t "otel-to-mongodb-tests:${platform#linux/}" \
    "$repo_root"

exec docker run --rm --platform "$platform" \
    "otel-to-mongodb-tests:${platform#linux/}" "$@"
