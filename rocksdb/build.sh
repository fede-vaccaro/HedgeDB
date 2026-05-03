#!/usr/bin/env bash
set -euo pipefail

ROCKSDB_VERSION="11.1.1"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"

if [ ! -d "$SCRIPT_DIR/rocksdb-${ROCKSDB_VERSION}" ]; then
    echo "downloading RocksDB v" ${ROCKSDB_VERSION}
    wget -P "$SCRIPT_DIR" "https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.zip"
    unzip -d "$SCRIPT_DIR" "$SCRIPT_DIR/v${ROCKSDB_VERSION}.zip"
fi

echo "Configuring with RocksDB v" ${ROCKSDB_VERSION}
cmake "$SCRIPT_DIR" \
    -B "$BUILD_DIR" \
    -D ROCKSDB_VERSION=${ROCKSDB_VERSION} \
    -DCMAKE_BUILD_TYPE=Release \

echo "Building..."
cmake --build "$BUILD_DIR" --target benchtool -j"$(nproc)"

echo ""
echo "Done: $BUILD_DIR/benchtool"
