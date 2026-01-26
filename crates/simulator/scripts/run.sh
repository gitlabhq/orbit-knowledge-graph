#!/usr/bin/env bash
set -euo pipefail

# Simulator run script - starts ClickHouse via Colima/Docker and populates with fake data

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$CRATE_DIR/../.." && pwd)"

# Configuration
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_NATIVE_PORT="${CLICKHOUSE_NATIVE_PORT:-9000}"
CLICKHOUSE_CONTAINER_NAME="${CLICKHOUSE_CONTAINER_NAME:-gkg-clickhouse}"
CLICKHOUSE_IMAGE="${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server:24.8}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# Check if a command exists
command_exists() {
    command -v "$1" &> /dev/null
}

# Ensure Colima is running
ensure_colima() {
    if ! command_exists colima; then
        log_error "Colima is not installed. Install with: brew install colima"
        exit 1
    fi

    if ! colima status &> /dev/null; then
        log_info "Starting Colima..."
        # Start with existing settings (don't override disk/memory/cpu)
        colima start
    else
        log_info "Colima is already running"
    fi
}

# Ensure Docker CLI is available
ensure_docker() {
    if ! command_exists docker; then
        log_error "Docker CLI is not installed. Install with: brew install docker"
        exit 1
    fi

    # Verify Docker is connected to Colima
    if ! docker info &> /dev/null; then
        log_error "Docker cannot connect. Make sure Colima is running: colima start"
        exit 1
    fi
}

# Start ClickHouse container
start_clickhouse() {
    # Check if container already exists
    if docker ps -a --format '{{.Names}}' | grep -q "^${CLICKHOUSE_CONTAINER_NAME}$"; then
        # Container exists, check if running
        if docker ps --format '{{.Names}}' | grep -q "^${CLICKHOUSE_CONTAINER_NAME}$"; then
            log_info "ClickHouse container is already running"
        else
            log_info "Starting existing ClickHouse container..."
            docker start "$CLICKHOUSE_CONTAINER_NAME"
        fi
    else
        log_info "Creating and starting ClickHouse container..."
        docker run -d \
            --name "$CLICKHOUSE_CONTAINER_NAME" \
            -p "${CLICKHOUSE_PORT}:8123" \
            -p "${CLICKHOUSE_NATIVE_PORT}:9000" \
            --ulimit nofile=262144:262144 \
            -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
            -e CLICKHOUSE_PASSWORD="" \
            "$CLICKHOUSE_IMAGE"
    fi
}

# Wait for ClickHouse to be ready
wait_for_clickhouse() {
    log_info "Waiting for ClickHouse to be ready..."
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -s "http://localhost:${CLICKHOUSE_PORT}/ping" | grep -q "Ok"; then
            log_info "ClickHouse is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    log_error "ClickHouse failed to start within ${max_attempts} seconds"
    exit 1
}

# Stop ClickHouse container
stop_clickhouse() {
    if docker ps --format '{{.Names}}' | grep -q "^${CLICKHOUSE_CONTAINER_NAME}$"; then
        log_info "Stopping ClickHouse container..."
        docker stop "$CLICKHOUSE_CONTAINER_NAME"
    else
        log_warn "ClickHouse container is not running"
    fi
}

# Remove ClickHouse container and data
clean_clickhouse() {
    if docker ps -a --format '{{.Names}}' | grep -q "^${CLICKHOUSE_CONTAINER_NAME}$"; then
        log_info "Removing ClickHouse container..."
        docker rm -f "$CLICKHOUSE_CONTAINER_NAME"
    else
        log_warn "ClickHouse container does not exist"
    fi
}

# Run the data population
run_populate() {
    log_info "Building and running data population..."
    cd "$REPO_ROOT"
    
    cargo run --bin simulate -- \
        --ontology-path fixtures/ontology \
        --clickhouse-url "http://localhost:${CLICKHOUSE_PORT}" \
        "$@"
}

# Show ClickHouse status
show_status() {
    echo ""
    echo "ClickHouse Status:"
    echo "=================="
    
    if docker ps --format '{{.Names}}' | grep -q "^${CLICKHOUSE_CONTAINER_NAME}$"; then
        echo -e "Container: ${GREEN}running${NC}"
        echo "HTTP API:  http://localhost:${CLICKHOUSE_PORT}"
        echo "Native:    localhost:${CLICKHOUSE_NATIVE_PORT}"
        
        # Show database stats if available
        if curl -s "http://localhost:${CLICKHOUSE_PORT}/ping" | grep -q "Ok"; then
            echo ""
            echo "Tables:"
            curl -s "http://localhost:${CLICKHOUSE_PORT}" \
                --data "SELECT name, total_rows, formatReadableSize(total_bytes) as size FROM system.tables WHERE database = 'default' AND name LIKE 'kg_%' FORMAT Pretty" \
                2>/dev/null || true
        fi
    else
        echo -e "Container: ${RED}not running${NC}"
    fi
}

# Print usage
usage() {
    cat << EOF
Usage: $(basename "$0") [command] [options]

Commands:
    start       Start ClickHouse (default if no command given)
    stop        Stop ClickHouse container
    clean       Remove ClickHouse container (deletes all data)
    restart     Restart ClickHouse container
    status      Show ClickHouse status and table statistics
    populate    Start ClickHouse and populate with fake data
    help        Show this help message

Populate Options (passed to the simulator binary):
    --tenants N              Number of tenants (default: 2)
    --nodes-per-type N       Default nodes per type (default: 100)
    --node-count TYPE=N      Override count for specific type (repeatable)
                             Example: --node-count User=500 --node-count Project=200
    --edges-per-source N     Edges per source node (default: 3)
    --batch-size N           Batch size for inserts (default: 10000)
    --dry-run                Print plan without executing

Environment Variables:
    CLICKHOUSE_PORT             HTTP port (default: 8123)
    CLICKHOUSE_NATIVE_PORT      Native port (default: 9000)
    CLICKHOUSE_CONTAINER_NAME   Container name (default: gkg-clickhouse)
    CLICKHOUSE_IMAGE            Docker image (default: clickhouse/clickhouse-server:24.8)

Examples:
    $(basename "$0")                              # Start ClickHouse
    $(basename "$0") populate                     # Populate with default settings
    $(basename "$0") populate --tenants 5         # 5 tenants
    $(basename "$0") populate --nodes-per-type 500 --node-count User=1000
    $(basename "$0") populate --dry-run           # Preview without executing
    $(basename "$0") status                       # Check status and row counts
    $(basename "$0") clean                        # Remove container and data
EOF
}

# Main
main() {
    local command="${1:-start}"
    shift || true

    case "$command" in
        start)
            ensure_colima
            ensure_docker
            start_clickhouse
            wait_for_clickhouse
            show_status
            ;;
        stop)
            ensure_docker
            stop_clickhouse
            ;;
        clean)
            ensure_docker
            clean_clickhouse
            ;;
        restart)
            ensure_docker
            stop_clickhouse
            start_clickhouse
            wait_for_clickhouse
            show_status
            ;;
        status)
            ensure_docker
            show_status
            ;;
        populate)
            ensure_colima
            ensure_docker
            start_clickhouse
            wait_for_clickhouse
            run_populate "$@"
            show_status
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            log_error "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

main "$@"
