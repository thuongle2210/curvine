#!/bin/bash

#
# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# build-spdk.sh — Download, configure, and compile SPDK from source
# Used inside Dockerfiles and optionally for local builds.
#
# Usage:
#   build-spdk.sh [--prefix /opt/spdk] [--rdma] [--arch native] [--jobs N] [--tag v25.03]

set -euo pipefail

# Defaults
SPDK_TAG="v25.09"
PREFIX="/opt/spdk"
TARGET_ARCH="native"
JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
ENABLE_RDMA=0
SPDK_SRC_DIR="/tmp/spdk-src"

print_info() {
    echo -e "\033[34m[INFO]\033[0m $1"
}

print_success() {
    echo -e "\033[32m[SUCCESS]\033[0m $1"
}

print_error() {
    echo -e "\033[31m[ERROR]\033[0m $1"
}

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --prefix PATH     Install prefix (default: /opt/spdk)"
    echo "  --rdma            Enable RDMA transport support"
    echo "  --arch ARCH       Target architecture for --target-arch (default: native)"
    echo "  --jobs N          Number of parallel make jobs (default: $(nproc 2>/dev/null || echo 4))"
    echo "  --tag TAG         SPDK git tag to clone (default: v25.09)"
    echo "  --src-dir PATH    Directory to clone SPDK source into (default: /tmp/spdk-src)"
    echo "  -h, --help        Show this help message"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --rdma)
            ENABLE_RDMA=1
            shift
            ;;
        --arch)
            TARGET_ARCH="$2"
            shift 2
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --tag)
            SPDK_TAG="$2"
            shift 2
            ;;
        --src-dir)
            SPDK_SRC_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

print_info "SPDK build configuration:"
print_info "  Tag:          ${SPDK_TAG}"
print_info "  Prefix:       ${PREFIX}"
print_info "  Architecture: ${TARGET_ARCH}"
print_info "  Jobs:         ${JOBS}"
print_info "  RDMA:         $([ $ENABLE_RDMA -eq 1 ] && echo 'enabled' || echo 'disabled')"
print_info "  Source dir:   ${SPDK_SRC_DIR}"

# --- Step 1: Install build dependencies ---
print_info "Installing build dependencies..."

if command -v dnf &>/dev/null; then
    dnf install -y \
        gcc gcc-c++ make pkg-config \
        libaio-devel libuuid-devel \
        openssl-devel numactl-devel \
        python3 python3-pip \
        git wget curl-minimal jq \
        libffi-devel \
        && dnf clean all
    if [ $ENABLE_RDMA -eq 1 ]; then
        print_info "Installing RDMA dependencies..."
        dnf install -y \
            rdma-core-devel libibverbs-devel librdmacm-devel \
            && dnf clean all
    fi
elif command -v apt-get &>/dev/null; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update && apt-get install -y \
        build-essential pkg-config \
        libaio-dev libuuid-dev \
        libssl-dev libnuma-dev \
        python3 python3-pip \
        git wget curl jq \
        libffi-dev \
        && apt-get clean
    if [ $ENABLE_RDMA -eq 1 ]; then
        print_info "Installing RDMA dependencies..."
        apt-get update && apt-get install -y \
            libibverbs-dev librdmacm-dev \
            && apt-get clean
    fi
elif command -v yum &>/dev/null; then
    yum install -y \
        gcc gcc-c++ make pkgconfig \
        libaio-devel libuuid-devel \
        openssl-devel numactl-devel \
        python3 python3-pip \
        git wget curl jq \
        libffi-devel \
        && yum clean all
    if [ $ENABLE_RDMA -eq 1 ]; then
        print_info "Installing RDMA dependencies..."
        yum install -y \
            rdma-core-devel libibverbs-devel librdmacm-devel \
            && yum clean all
    fi
else
    print_error "Unsupported package manager. Please install SPDK dependencies manually."
    exit 1
fi

# --- Step 2: Clone SPDK source ---
if [ -d "${SPDK_SRC_DIR}/.git" ]; then
    print_info "SPDK source already cloned at ${SPDK_SRC_DIR}, updating..."
    cd "${SPDK_SRC_DIR}"
    git fetch --tags origin "${SPDK_TAG}"
    git checkout "${SPDK_TAG}"
else
    print_info "Cloning SPDK ${SPDK_TAG} from GitHub..."
    rm -rf "${SPDK_SRC_DIR}"
    git clone --branch "${SPDK_TAG}" --depth 1 https://github.com/spdk/spdk "${SPDK_SRC_DIR}"
    cd "${SPDK_SRC_DIR}"
fi

# --- Step 3: Initialize submodules ---
print_info "Initializing SPDK submodules..."
git submodule update --init

# --- Step 4: Install SPDK-specific dependencies ---
print_info "Running SPDK pkgdep.sh..."
if [ $ENABLE_RDMA -eq 1 ]; then
    scripts/pkgdep.sh --rdma
else
    scripts/pkgdep.sh
fi

# --- Step 4.1: Build isa-l from source (required by SPDK, not available in Rocky 9 aarch64 repos) ---
if ! pkg-config --exists libisal 2>/dev/null; then
    print_info "Building isa-l from source..."
    dnf install -y nasm autoconf automake libtool
    cd /tmp
    git clone --depth 1 --branch v2.31.1 https://github.com/intel/isa-l.git isa-l-src
    cd isa-l-src
    ./autogen.sh
    ./configure
    make -j "${JOBS}"
    make install
    ldconfig
    cd /
    rm -rf /tmp/isa-l-src
    dnf remove -y nasm autoconf automake libtool
    dnf clean all
    print_success "isa-l built and installed"
else
    print_info "isa-l already installed, skipping"
fi

# --- Step 5: Install Python dependencies for RPC tools ---
print_info "Installing Python dependencies for SPDK RPC tools..."
pip3 install --break-system-packages grpcio-tools==1.51.3 protobuf==4.22.1 2>/dev/null || \
    pip3 install grpcio-tools==1.51.3 protobuf==4.22.1 2>/dev/null || \
    print_info "pip install for grpcio-tools skipped (may already be installed)"

# --- Step 6: Configure SPDK ---
print_info "Configuring SPDK..."
CONFIGURE_CMD="./configure --disable-tests --target-arch=${TARGET_ARCH}"

if [ $ENABLE_RDMA -eq 1 ]; then
    CONFIGURE_CMD="${CONFIGURE_CMD} --with-rdma"
fi

print_info "Configure command: ${CONFIGURE_CMD}"
eval "${CONFIGURE_CMD}"

# --- Step 7: Build SPDK ---
print_info "Building SPDK with ${JOBS} parallel jobs..."
make -j "${JOBS}"

# --- Step 8: Install to prefix ---
print_info "Installing SPDK to ${PREFIX}..."
mkdir -p "${PREFIX}"
cp -r "${SPDK_SRC_DIR}"/* "${PREFIX}/"

# Verify build
if [ -f "${PREFIX}/build/bin/nvmf_tgt" ]; then
    print_success "SPDK ${SPDK_TAG} built and installed successfully to ${PREFIX}"
    print_info "nvmf_tgt: ${PREFIX}/build/bin/nvmf_tgt"
    print_info "rpc.py:   ${PREFIX}/scripts/rpc.py"
    print_info "setup.sh: ${PREFIX}/scripts/setup.sh"
else
    print_error "SPDK build failed — nvmf_tgt not found at ${PREFIX}/build/bin/nvmf_tgt"
    exit 1
fi
