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

# entrypoint-initiator.sh — SPDK NVMe-oF initiator container startup
#
# Discovers and connects to a remote NVMe-oF target, then starts the
# curvine worker process.
#
# Environment variables:
#   TARGET_IP       — NVMe-oF target IP (default: auto-detect first routable IP)
#   TARGET_PORT     — NVMe-oF target port (default: 4420)
#   SUBNQN          — Subsystem NQN to connect (default: nqn.2025-03.io.curvine:cnode1)
#   TRANSPORT_TYPE  — tcp or rdma (default: tcp)
#   CURVINE_CONF_FILE — Path to curvine config (default: /app/curvine/conf/curvine-cluster.toml)
#
# Usage:
#   /entrypoint-initiator.sh [worker|all] [start|stop|restart]

set -euo pipefail

TARGET_IP="${TARGET_IP:-$(hostname -I | awk '{print $1}')}"
TARGET_PORT="${TARGET_PORT:-4420}"
SUBNQN="${SUBNQN:-nqn.2025-03.io.curvine:cnode1}"
TRANSPORT_TYPE="${TRANSPORT_TYPE:-tcp}"
CURVINE_CONF_FILE="${CURVINE_CONF_FILE:-/app/curvine/conf/curvine-cluster.toml}"
SPDK_DIR="${SPDK_DIR:-/opt/spdk}"

# Service type: worker (default)
SERVER_TYPE="${1:-worker}"
ACTION_TYPE="${2:-start}"

print_info() {
    echo -e "\033[34m[INITIATOR]\033[0m $1"
}

print_success() {
    echo -e "\033[32m[INITIATOR]\033[0m $1"
}

print_error() {
    echo -e "\033[31m[INITIATOR]\033[0m $1"
}

# --- Discover NVMe-oF target ---
discover_target() {
    print_info "Discovering NVMe-oF target at ${TARGET_IP}:${TARGET_PORT}..."

    if [ ! -e /dev/nvme-fabrics ]; then
        print_info "/dev/nvme-fabrics not found — kernel NVMe-oF initiator unavailable"
        print_info "SPDK initiator will connect at the application level via curvine-server"
        return 0
    fi

    if command -v nvme &>/dev/null; then
        nvme discover -t "${TRANSPORT_TYPE}" -a "${TARGET_IP}" -s "${TARGET_PORT}" 2>/dev/null || {
            print_error "Discovery failed. Is the target running at ${TARGET_IP}:${TARGET_PORT}?"
            return 1
        }
    else
        print_info "nvme-cli not found, skipping discovery"
    fi
}

# --- Connect to NVMe-oF target ---
connect_target() {
    print_info "Connecting to NVMe-oF target ${SUBNQN} at ${TARGET_IP}:${TARGET_PORT}..."

    if [ ! -e /dev/nvme-fabrics ]; then
        print_info "/dev/nvme-fabrics not found — kernel NVMe-oF initiator unavailable"
        print_info "SPDK initiator will connect at the application level via curvine-server"
        return 0
    fi

    if command -v nvme &>/dev/null; then
        nvme connect -t "${TRANSPORT_TYPE}" -n "${SUBNQN}" -a "${TARGET_IP}" -s "${TARGET_PORT}" || {
            print_error "Connection failed. Check target availability and NQN: ${SUBNQN}"
            return 1
        }
        print_success "Connected to ${SUBNQN}"

        # Verify connection
        nvme list 2>/dev/null || true
    else
        print_info "nvme-cli not found, skipping kernel-level connection"
        print_info "SPDK initiator will connect at the application level via curvine-server"
    fi
}

# --- Disconnect from NVMe-oF target ---
disconnect_target() {
    print_info "Disconnecting from NVMe-oF target ${SUBNQN}..."

    if command -v nvme &>/dev/null; then
        nvme disconnect -n "${SUBNQN}" 2>/dev/null || {
            print_info "No active connection to disconnect"
        }
        print_success "Disconnected from ${SUBNQN}"
    fi
}

# --- Start curvine service ---
start_service() {
    local SERVICE_NAME=$1

    print_info "Starting ${SERVICE_NAME} service..."

    local LOG_DIR=${CURVINE_HOME}/logs
    local OUT_FILE=${LOG_DIR}/${SERVICE_NAME}.out
    mkdir -p "${LOG_DIR}"

    cd "${CURVINE_HOME}"

    # Start the service with tee to output to both stdout and file
    "${CURVINE_HOME}/lib/curvine-server" \
        --service "${SERVICE_NAME}" \
        --conf "${CURVINE_CONF_FILE}" \
        2>&1 | tee "${OUT_FILE}" &

    local TEE_PID=$!
    sleep 3

    # Check if tee process is still running
    if ! kill -0 "${TEE_PID}" 2>/dev/null; then
        wait "${TEE_PID}" 2>/dev/null
        local EXIT_CODE=$?
        print_error "${SERVICE_NAME} start fail - process exited during startup with code ${EXIT_CODE}"
        exit 1
    fi

    # Verify curvine-server process is running
    local ACTUAL_PID
    ACTUAL_PID=$(pgrep -n -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
    if [[ -n "${ACTUAL_PID}" ]] && kill -0 "${ACTUAL_PID}" 2>/dev/null; then
        print_success "${SERVICE_NAME} start success, pid=${ACTUAL_PID}"

        # Wait for tee process to keep container alive
        wait "${TEE_PID}"
        local EXIT_CODE=$?
        print_info "${SERVICE_NAME} process exited with code ${EXIT_CODE}"
        exit "${EXIT_CODE}"
    else
        print_error "${SERVICE_NAME} start fail - process not found after startup check"
        kill "${TEE_PID}" 2>/dev/null || true
        exit 1
    fi
}

# --- Stop curvine service ---
stop_service() {
    local SERVICE_NAME=$1

    print_info "Stopping ${SERVICE_NAME} service gracefully..."

    local PIDS
    PIDS=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)

    if [[ -z "${PIDS}" ]]; then
        print_info "No running ${SERVICE_NAME} service to stop"
        return 0
    fi

    print_info "Found ${SERVICE_NAME} PIDs: ${PIDS}"

    read -r -a PID_ARRAY <<< "${PIDS}"
    if [[ ${#PID_ARRAY[@]} -gt 0 ]]; then
        kill "${PID_ARRAY[@]}" 2>/dev/null || true
    fi

    local TIMEOUT=15
    local INTERVAL=3
    local ELAPSED=0

    while [[ ${ELAPSED} -lt ${TIMEOUT} ]]; do
        sleep "${INTERVAL}"
        ELAPSED=$((ELAPSED + INTERVAL))

        local ALIVE
        ALIVE=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
        if [[ -z "${ALIVE}" ]]; then
            print_success "${SERVICE_NAME} stopped gracefully"
            return 0
        fi

        print_info "Waiting for ${SERVICE_NAME} to stop gracefully (elapsed=${ELAPSED}s)"
    done

    local STILL_ALIVE
    STILL_ALIVE=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
    if [[ -n "${STILL_ALIVE}" ]]; then
        print_error "${SERVICE_NAME} did not stop gracefully after ${TIMEOUT}s, killing with SIGKILL"
        read -r -a STILL_ARRAY <<< "${STILL_ALIVE}"
        kill -9 "${STILL_ARRAY[@]}" 2>/dev/null || true
    fi
}

# --- Main ---
print_info "SPDK NVMe-oF Initiator starting"
print_info "  Target IP:     ${TARGET_IP}"
print_info "  Target Port:   ${TARGET_PORT}"
print_info "  Subsystem NQN: ${SUBNQN}"
print_info "  Transport:     ${TRANSPORT_TYPE}"

case "$SERVER_TYPE" in
    worker)
        case "$ACTION_TYPE" in
            start)
                # Discover and connect to target first
                discover_target || true
                connect_target || true
                # Start curvine worker
                start_service "worker"
                ;;
            stop)
                stop_service "worker"
                disconnect_target
                ;;
            restart)
                stop_service "worker"
                disconnect_target
                sleep 2
                discover_target || true
                connect_target || true
                start_service "worker"
                ;;
            *)
                print_error "Unsupported action: $ACTION_TYPE (expected: start|stop|restart)"
                exit 1
                ;;
        esac
        ;;
    *)
        exec "$@"
        ;;
esac
