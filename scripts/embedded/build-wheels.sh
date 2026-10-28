#!/bin/sh
# Build the pymongo-embedded wheels this repository vendors, and drop them in vendor/.
#
# Upstream publishes the package neither to PyPI nor as a wheel on its releases, so we build
# our own. What it does NOT build is MongoDB: embedded-mongodb-sys downloads a prebuilt,
# sha256-pinned engine, and only the Rust glue around it compiles here.
#
# linux/arm64 only, and deliberately so. The extension is abi3, so one wheel spans Python
# versions but not machines, and arm64 is the one architecture this project can always build
# natively: CI runs on arm64 runners and run-tests.sh builds an arm64 container on Apple
# Silicon.
#
# Do not expect linux/amd64 to work here on an arm64 host. Under qemu the build deadlocks:
# cargo re-enters itself and both processes sit at 0% CPU forever, waiting on a file lock
# qemu does not translate. It gets as far as downloading the prebuilt engine and then stops.
# An amd64 wheel needs a real x86_64 machine -- build it there and drop it in vendor/.
#
#     ./scripts/embedded/build-wheels.sh
#     PLATFORMS=linux/amd64 ./scripts/embedded/build-wheels.sh   # on an x86_64 host only
#
# Set EMBEDDED_MONGO_REF to move the pinned upstream commit.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
platforms=${PLATFORMS:-linux/arm64}
ref=${EMBEDDED_MONGO_REF:-master}

mkdir -p "$repo_root/vendor"

for platform in $platforms; do
    tag="pymongo-embedded-wheel:${platform#linux/}"

    echo "==> building $platform (upstream ref: $ref)"
    docker build \
        --platform "$platform" \
        --build-arg "EMBEDDED_MONGO_REF=$ref" \
        -f "$repo_root/scripts/embedded/Dockerfile.wheel" \
        -t "$tag" \
        "$repo_root/scripts/embedded"

    # `docker cp` from a created-but-never-started container: nothing needs to run, and
    # nothing foreign has to execute for a wheel built under emulation.
    container=$(docker create --platform "$platform" "$tag")
    docker cp "$container:/wheels/." "$repo_root/vendor/"
    docker rm "$container" >/dev/null
done

echo
echo "==> vendor/"
ls -l "$repo_root/vendor"
echo
echo "Upstream commit: $(cat "$repo_root/vendor/SOURCE_COMMIT")"
echo "Run 'uv lock' if the wheel filenames changed."
