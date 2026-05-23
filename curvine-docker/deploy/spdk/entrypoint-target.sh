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

# entrypoint-target.sh — SPDK NVMe-oF target
#
# Reads its own identity from curvine-cluster.toml by matching $(hostname -s)
# against the traddr field in [[worker.spdk_disk.targets]].
# Then starts nvmf_tgt with --wait-for-rpc, runs setup via RPC, and starts I/O.
# On SIGTERM, cleans up subsystem and kills nvmf_tgt gracefully.
#
# Environment variables (machine-specific, not in TOML):
#   NVME_PCI_ADDR   — PCI address of NVMe device (optional — creates malloc bdev if empty).
#                     Host must bind the device to vfio-pci before starting the container.
#                     Example: sudo driverctl set-override <ADDR> vfio-pci
#   REACTOR_MASK    — CPU core mask (default: 0x3)
#   MEM_SIZE        — Memory size in MB (default: 1024)

set -euo pipefail

# ============================================================
# Defaults
# ============================================================
SPDK_DIR="${SPDK_DIR:-/opt/spdk}"
NVME_PCI_ADDR="${NVME_PCI_ADDR:-}"
REACTOR_MASK="${REACTOR_MASK:-0x3}"
MEM_SIZE="${MEM_SIZE:-1024}"
NR_HUGE_PAGES="${NR_HUGE_PAGES:-1024}"
SERIAL="${SERIAL:-SPDK0001}"

RPC="${SPDK_DIR}/scripts/rpc.py"
NVMF_TGT="${SPDK_DIR}/build/bin/nvmf_tgt"

print_info()    { echo -e "\033[34m[TARGET]\033[0m $1"; }
print_success() { echo -e "\033[32m[TARGET]\033[0m $1"; }
print_error()   { echo -e "\033[31m[TARGET]\033[0m $1"; }

# ============================================================
# Read target identity from TOML, matched by container hostname
# ============================================================
CURVINE_CONF_FILE="${CURVINE_CONF_FILE:-}"
MY_HOSTNAME="$(hostname -s)"
SUBNQN=""
TARGET_PORT=""
TRTYPE=""

if [ -n "$CURVINE_CONF_FILE" ] && [ -f "$CURVINE_CONF_FILE" ]; then
    print_info "Looking up target config for hostname '$MY_HOSTNAME' in $CURVINE_CONF_FILE"
    TARGET_BLOCK=$(awk -v RS= -v host="$MY_HOSTNAME" '
        /spdk_disk\.targets/ && index($0, "traddr = \"" host "\"") { print; exit }
    ' "$CURVINE_CONF_FILE")
    if [ -n "$TARGET_BLOCK" ]; then
        SUBNQN=$(echo "$TARGET_BLOCK" | grep subnqn | head -1 | sed 's/.*= *"\(.*\)"/\1/')
        TARGET_PORT=$(echo "$TARGET_BLOCK" | grep trsvcid | head -1 | sed 's/.*= *\([0-9]*\)/\1/')
        TRTYPE=$(echo "$TARGET_BLOCK" | grep trtype | head -1 | sed 's/.*= *"\(.*\)"/\1/')
        print_info "Matched: SUBNQN=$SUBNQN TRTYPE=$TRTYPE PORT=$TARGET_PORT"
    else
        print_info "No TOML block for hostname '$MY_HOSTNAME' — using defaults"
    fi
fi

SUBNQN="${SUBNQN:-nqn.2026-05.curvine:${MY_HOSTNAME}}"
TARGET_PORT="${TARGET_PORT:-4420}"
TRTYPE="${TRTYPE:-tcp}"
TARGET_IP="${TARGET_IP:-0.0.0.0}"

# ============================================================
# Hugepages (host-side, needs privileged or IPC_LOCK)
# ============================================================
print_info "Configuring hugepages (${NR_HUGE_PAGES} pages)..."
sysctl -w vm.nr_hugepages="$NR_HUGE_PAGES" || {
    print_error "Failed to set hugepages. Container must run privileged."
    exit 1
}

# ============================================================
# Start nvmf_tgt with --wait-for-rpc
# ============================================================
print_info "Starting nvmf_tgt (reactor_mask=$REACTOR_MASK, mem=${MEM_SIZE}MB)..."
"$NVMF_TGT" \
    -m "$REACTOR_MASK" \
    -s "$MEM_SIZE" \
    --wait-for-rpc \
    -r /var/tmp/spdk.sock &
NVMF_PID=$!
print_info "nvmf_tgt PID: $NVMF_PID"

# Wait for RPC socket (up to 15s)
for i in $(seq 1 30); do
    [ -S /var/tmp/spdk.sock ] && break
    sleep 0.5
done
if [ ! -S /var/tmp/spdk.sock ]; then
    print_error "nvmf_tgt RPC socket not ready after 15s"
    exit 1
fi
print_success "nvmf_tgt RPC socket ready"

# ============================================================
# Setup via RPC
# ============================================================

# Attach NVMe controller or create malloc bdev
if [ -n "$NVME_PCI_ADDR" ]; then
    print_info "Attaching NVMe controller at $NVME_PCI_ADDR..."
    "$RPC" bdev_nvme_attach_controller -b Nvme0 -t PCIe -a "$NVME_PCI_ADDR"
    print_success "NVMe controller attached as Nvme0"
else
    print_info "No NVME_PCI_ADDR — creating malloc bdev (128 MB)..."
    "$RPC" bdev_malloc_create -b Malloc0 128 4096
    print_success "Malloc bdev created as Malloc0"
fi

# Create transport matching TRTYPE
case "$TRTYPE" in
    tcp)
        print_info "Creating TCP transport..."
        "$RPC" nvmf_create_transport -t TCP -u 16384 -m 8 -c 8192
        ;;
    rdma)
        print_info "Creating RDMA transport..."
        "$RPC" nvmf_create_transport -t RDMA -u 16384 -m 8 -c 8192
        ;;
esac

# Create subsystem
print_info "Creating subsystem $SUBNQN..."
"$RPC" nvmf_create_subsystem "$SUBNQN" -a -s "$SERIAL"
print_success "Subsystem created"

# Add namespace
if [ -n "$NVME_PCI_ADDR" ]; then
    print_info "Adding Nvme0n1 to subsystem..."
    "$RPC" nvmf_subsystem_add_ns "$SUBNQN" Nvme0n1
else
    print_info "Adding Malloc0 to subsystem..."
    "$RPC" nvmf_subsystem_add_ns "$SUBNQN" Malloc0
fi
print_success "Namespace added"

# Add listener matching TRTYPE
case "$TRTYPE" in
    tcp)
        print_info "Adding TCP listener on $TARGET_IP:$TARGET_PORT..."
        "$RPC" nvmf_subsystem_add_listener "$SUBNQN" -t TCP -a "$TARGET_IP" -s "$TARGET_PORT"
        ;;
    rdma)
        print_info "Adding RDMA listener on $TARGET_IP:$TARGET_PORT..."
        "$RPC" nvmf_subsystem_add_listener "$SUBNQN" -t RDMA -a "$TARGET_IP" -s "$TARGET_PORT"
        ;;
esac

# Start I/O processing
print_info "Starting I/O processing..."
"$RPC" framework_start_init
print_success "framework_start_init complete"

# ============================================================
# Graceful shutdown handler
# ============================================================
cleanup() {
    print_info "Shutting down..."
    "$RPC" nvmf_delete_subsystem "$SUBNQN" 2>/dev/null || true
    kill -TERM "$NVMF_PID" 2>/dev/null || true
    wait "$NVMF_PID" 2>/dev/null || true
    print_success "Clean shutdown complete"
}
trap cleanup TERM INT

# ============================================================
# Print summary
# ============================================================
print_success "SPDK NVMe-oF Target running"
print_info "  Subsystem: $SUBNQN"
print_info "  Listener:  $TARGET_IP:$TARGET_PORT"
print_info "  NVMe:      ${NVME_PCI_ADDR:-Malloc0 (test)}"

# ============================================================
# Wait for nvmf_tgt to exit
# ============================================================
wait "$NVMF_PID"
