#!/bin/bash

# entrypoint-initiator.sh — Curvine worker/master startup
#
# All cluster config comes from curvine-cluster.toml (mounted).
# Starts curvine-server in the specified service role.
#
# Usage:
#   /entrypoint-initiator.sh [worker|master] [start|stop|restart]

set -euo pipefail

CURVINE_CONF_FILE="${CURVINE_CONF_FILE:-/app/curvine/conf/curvine-cluster.toml}"
CURVINE_HOME="${CURVINE_HOME:-/app/curvine}"

SERVER_TYPE="${1:-worker}"
ACTION_TYPE="${2:-start}"

print_info() {
    echo -e "\033[34m[CURVINE]\033[0m $1"
}

print_success() {
    echo -e "\033[32m[CURVINE]\033[0m $1"
}

print_error() {
    echo -e "\033[31m[CURVINE]\033[0m $1"
}

# --- Validate TOML config exists ---
validate_config() {
    if [ ! -f "$CURVINE_CONF_FILE" ]; then
        print_error "Config not found: $CURVINE_CONF_FILE"
        print_error "Mount your curvine-cluster.toml or set CURVINE_CONF_FILE"
        exit 1
    fi
    print_info "Using config: $CURVINE_CONF_FILE"
}

# --- Start curvine service ---
start_service() {
    local SERVICE_NAME=$1

    print_info "Starting $SERVICE_NAME service..."

    local LOG_DIR=${CURVINE_HOME}/logs
    local OUT_FILE=${LOG_DIR}/${SERVICE_NAME}.out
    mkdir -p "$LOG_DIR"

    cd "$CURVINE_HOME"

    "${CURVINE_HOME}/lib/curvine-server" \
        --service "$SERVICE_NAME" \
        --conf "$CURVINE_CONF_FILE" \
        2>&1 | tee "$OUT_FILE" &

    local TEE_PID=$!
    sleep 3

    if ! kill -0 "$TEE_PID" 2>/dev/null; then
        wait "$TEE_PID" 2>/dev/null
        local EXIT_CODE=$?
        print_error "$SERVICE_NAME start fail — exited during startup with code $EXIT_CODE"
        exit 1
    fi

    local ACTUAL_PID
    ACTUAL_PID=$(pgrep -n -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
    if [[ -n "$ACTUAL_PID" ]] && kill -0 "$ACTUAL_PID" 2>/dev/null; then
        print_success "$SERVICE_NAME start success, pid=$ACTUAL_PID"

        wait "$TEE_PID"
        local EXIT_CODE=$?
        print_info "$SERVICE_NAME process exited with code $EXIT_CODE"
        exit "$EXIT_CODE"
    else
        print_error "$SERVICE_NAME start fail — process not found after startup"
        kill "$TEE_PID" 2>/dev/null || true
        exit 1
    fi
}

# --- Stop curvine service ---
stop_service() {
    local SERVICE_NAME=$1

    print_info "Stopping $SERVICE_NAME service gracefully..."

    local PIDS
    PIDS=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)

    if [[ -z "$PIDS" ]]; then
        print_info "No running $SERVICE_NAME service to stop"
        return 0
    fi

    read -r -a PID_ARRAY <<< "$PIDS"
    if [[ ${#PID_ARRAY[@]} -gt 0 ]]; then
        kill "${PID_ARRAY[@]}" 2>/dev/null || true
    fi

    local TIMEOUT=15
    local INTERVAL=3
    local ELAPSED=0

    while [[ $ELAPSED -lt $TIMEOUT ]]; do
        sleep "$INTERVAL"
        ELAPSED=$((ELAPSED + INTERVAL))

        local ALIVE
        ALIVE=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
        if [[ -z "$ALIVE" ]]; then
            print_success "$SERVICE_NAME stopped gracefully"
            return 0
        fi
    done

    local STILL_ALIVE
    STILL_ALIVE=$(pgrep -f "${CURVINE_HOME}/lib/curvine-server[[:space:]].*--service[[:space:]]${SERVICE_NAME}" || true)
    if [[ -n "$STILL_ALIVE" ]]; then
        print_error "$SERVICE_NAME did not stop in ${TIMEOUT}s, sending SIGKILL"
        read -r -a STILL_ARRAY <<< "$STILL_ALIVE"
        kill -9 "${STILL_ARRAY[@]}" 2>/dev/null || true
    fi
}

# --- Main ---
validate_config

print_info "Starting Curvine"
print_info "Config: $CURVINE_CONF_FILE"
print_info "Type:   $SERVER_TYPE"

case "$SERVER_TYPE" in
    worker|master)
        case "$ACTION_TYPE" in
            start)
                start_service "$SERVER_TYPE"
                ;;
            stop)
                stop_service "$SERVER_TYPE"
                ;;
            restart)
                stop_service "$SERVER_TYPE"
                sleep 2
                start_service "$SERVER_TYPE"
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
