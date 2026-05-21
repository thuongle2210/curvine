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

# entrypoint-target.sh — SPDK NVMe-oF target container startup
#
# Environment variables:
#   TARGET_IP       — IP address to bind listeners (default: 127.0.0.1)
#   TARGET_PORT     — NVMe-oF port (default: 4420)
#   NVME_PCI_ADDR   — PCI address of NVMe device to export (default: 0000:00:0e.0 — VirtualBox)
#   SUBNQN          — Subsystem NQN (default: nqn.2025-03.io.curvine:cnode1)
#   SERIAL          — Serial number (default: SPDK0001)
#   TRANSPORT_MODE  — tcp, rdma, or both (default: tcp)
#   REACTOR_MASK    — CPU core mask for SPDK reactors (default: 0x3)
#   SHM_ID          — Shared memory ID (default: -1)
#   MEM_SIZE        — Memory size in MB (default: 1024)
#   NR_HUGE_PAGES   — Number of huge pages (default: 1024)

set -euo pipefail

SPDK_DIR="${SPDK_DIR:-/opt/spdk}"
TARGET_IP="${TARGET_IP:-127.0.0.1}"
TARGET_PORT="${TARGET_PORT:-4420}"
NVME_PCI_ADDR="${NVME_PCI_ADDR:-0000:00:0e.0}"
SUBNQN="${SUBNQN:-nqn.2025-03.io.curvine:cnode1}"
SERIAL="${SERIAL:-SPDK0001}"
TRANSPORT_MODE="${TRANSPORT_MODE:-tcp}"
REACTOR_MASK="${REACTOR_MASK:-0x3}"
SHM_ID="${SHM_ID:--1}"
MEM_SIZE="${MEM_SIZE:-1024}"
NR_HUGE_PAGES="${NR_HUGE_PAGES:-1024}"

RPC="${SPDK_DIR}/scripts/rpc.py"
NVMF_TGT="${SPDK_DIR}/build/bin/nvmf_tgt"

print_info() {
    echo -e "\033[34m[TARGET]\033[0m $1"
}

print_success() {
    echo -e "\033[32m[TARGET]\033[0m $1"
}

print_error() {
    echo -e "\033[31m[TARGET]\033[0m $1"
}

# --- Step 1: Configure hugepages ---
print_info "Configuring hugepages (${NR_HUGE_PAGES} pages)..."
sysctl -w vm.nr_hugepages="${NR_HUGE_PAGES}" || {
    print_error "Failed to set hugepages. Container must be privileged."
    exit 1
}

# --- Step 2: Bind NVMe device to userspace driver ---
print_info "Binding NVMe device ${NVME_PCI_ADDR} to userspace driver..."
"${SPDK_DIR}/scripts/setup.sh" || {
    print_error "Failed to bind NVMe device. Check PCI address: ${NVME_PCI_ADDR}"
    exit 1
}

# --- Step 3: Start nvmf_tgt ---
print_info "Starting nvmf_tgt with reactor_mask=${REACTOR_MASK}, mem=${MEM_SIZE}MB, shm_id=${SHM_ID}..."
"${NVMF_TGT}" \
    -m "${REACTOR_MASK}" \
    -s "${MEM_SIZE}" \
    --shm-id "${SHM_ID}" \
    -r "/var/tmp/spdk.sock" \
    > /var/log/spdk/nvmf_tgt.log 2>&1 &

NVMF_PID=$!
print_info "nvmf_tgt started with PID ${NVMF_PID}"

# Wait for nvmf_tgt to initialize
sleep 3

if ! kill -0 "${NVMF_PID}" 2>/dev/null; then
    print_error "nvmf_tgt failed to start. Check /var/log/spdk/nvmf_tgt.log"
    cat /var/log/spdk/nvmf_tgt.log
    exit 1
fi

# --- Step 4: Create transports ---
if [ "${TRANSPORT_MODE}" = "tcp" ] || [ "${TRANSPORT_MODE}" = "both" ]; then
    print_info "Creating TCP transport..."
    "${RPC}" nvmf_create_transport -t TCP -u 16384 -m 8 -c 8192
    print_success "TCP transport created"
fi

if [ "${TRANSPORT_MODE}" = "rdma" ] || [ "${TRANSPORT_MODE}" = "both" ]; then
    print_info "Creating RDMA transport..."
    "${RPC}" nvmf_create_transport -t RDMA -u 16384 -m 8 -c 8192
    print_success "RDMA transport created"
fi

# --- Step 5: Attach NVMe controller ---
print_info "Attaching NVMe controller at ${NVME_PCI_ADDR}..."
"${RPC}" bdev_nvme_attach_controller -b Nvme0 -t PCIe -a "${NVME_PCI_ADDR}"
print_success "NVMe controller attached as Nvme0"

# --- Step 6: Create subsystem ---
print_info "Creating subsystem ${SUBNQN}..."
"${RPC}" nvmf_create_subsystem "${SUBNQN}" -a -s "${SERIAL}"
print_success "Subsystem created"

# --- Step 7: Add namespace ---
print_info "Adding namespace Nvme0n1 to subsystem..."
"${RPC}" nvmf_subsystem_add_ns "${SUBNQN}" Nvme0n1
print_success "Namespace added"

# --- Step 8: Add listeners ---
if [ "${TRANSPORT_MODE}" = "tcp" ] || [ "${TRANSPORT_MODE}" = "both" ]; then
    print_info "Adding TCP listener on ${TARGET_IP}:${TARGET_PORT}..."
    "${RPC}" nvmf_subsystem_add_listener "${SUBNQN}" -t TCP -a "${TARGET_IP}" -s "${TARGET_PORT}"
    print_success "TCP listener added"
fi

if [ "${TRANSPORT_MODE}" = "rdma" ] || [ "${TRANSPORT_MODE}" = "both" ]; then
    print_info "Adding RDMA listener on ${TARGET_IP}:${TARGET_PORT}..."
    "${RPC}" nvmf_subsystem_add_listener "${SUBNQN}" -t RDMA -a "${TARGET_IP}" -s "${TARGET_PORT}"
    print_success "RDMA listener added"
fi

# --- Step 9: Print configuration summary ---
print_success "SPDK NVMe-oF Target is running"
print_info "  Subsystem NQN: ${SUBNQN}"
print_info "  Target IP:     ${TARGET_IP}"
print_info "  Target Port:   ${TARGET_PORT}"
print_info "  Transport(s):  ${TRANSPORT_MODE}"
print_info "  NVMe Device:   ${NVME_PCI_ADDR}"
print_info "  Serial:        ${SERIAL}"

# --- Step 10: Keep container alive ---
print_info "Tailing nvmf_tgt log to keep container alive..."
tail -f /var/log/spdk/nvmf_tgt.log
