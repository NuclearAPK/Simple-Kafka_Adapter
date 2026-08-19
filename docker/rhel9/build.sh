#!/usr/bin/env sh
# Builds SimpleKafka1C.so for RHEL 9 inside a container and copies the
# artifact to out/rhel9 on the host.
#
#   sh docker/rhel9/build.sh
#   OUTPUT=dist VCPKG_REF=2025.06.13 sh docker/rhel9/build.sh

set -eu

OUTPUT="${OUTPUT:-out/rhel9}"
BASE_IMAGE="${BASE_IMAGE:-rockylinux/rockylinux:9}"
VCPKG_REF="${VCPKG_REF:-master}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
TRIPLET="${TRIPLET:-x64-linux}"

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repo_root"

docker build \
    -f docker/rhel9/Dockerfile \
    --target export \
    --output "$OUTPUT" \
    --build-arg "BASE_IMAGE=$BASE_IMAGE" \
    --build-arg "VCPKG_REF=$VCPKG_REF" \
    --build-arg "BUILD_TYPE=$BUILD_TYPE" \
    --build-arg "TRIPLET=$TRIPLET" \
    .

echo
echo "Artifacts in $OUTPUT:"
ls -l "$OUTPUT"
