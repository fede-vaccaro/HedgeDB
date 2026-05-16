#!/bin/bash

set -e

LIBURING_VERSION="liburing-2.14"
LIBURING_URL="https://github.com/axboe/liburing/archive/refs/tags/${LIBURING_VERSION}.zip"
BUILD_DIR="/tmp/liburing-build"

echo "=== Building liburing ${LIBURING_VERSION} ==="

# Clean up any previous build
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

# Download liburing
echo "Downloading liburing..."
cd "${BUILD_DIR}"
curl -L -o liburing.zip "${LIBURING_URL}"
unzip -q liburing.zip
cd liburing-"${LIBURING_VERSION}"

# Configure build
echo "Configuring..."
./configure --cc=gcc --cxx=g++

# Build liburing
echo "Building..."
make -j$(nproc)

# Build liburing.pc
echo "Building pkg-config file..."
make liburing.pc

# Install liburing
echo "Installing..."
sudo make install

# Clean up
echo "Cleaning up..."
cd /
rm -rf "${BUILD_DIR}"

echo "=== liburing ${LIBURING_VERSION} installed successfully ==="
