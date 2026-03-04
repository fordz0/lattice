#!/usr/bin/env bash
set -euo pipefail

CMD="${1:-start}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

ROOT_DIR="${LATTICE_LOCALNET_DIR:-/tmp/lattice-localnet}"
BASE_PORT="${LATTICE_LOCALNET_BASE_PORT:-19000}"
NODE_COUNT="${LATTICE_LOCALNET_NODES:-3}"
RUST_LOG_LEVEL="${RUST_LOG:-info}"

DAEMON_BIN="${LATTICE_DAEMON_BIN:-$REPO_ROOT/target/release/lattice-daemon}"
CLI_BIN="${LATTICE_CLI_BIN:-$REPO_ROOT/target/release/lattice-cli}"

if ! [[ "$NODE_COUNT" =~ ^[0-9]+$ ]] || (( NODE_COUNT < 1 )); then
  echo "LATTICE_LOCALNET_NODES must be a positive integer"
  exit 1
fi

node_p2p_port() {
  local idx="$1"
  echo $((BASE_PORT + (idx - 1) * 10))
}

node_rpc_port() {
  local idx="$1"
  echo $((BASE_PORT + (idx - 1) * 10 + 1))
}

node_http_port() {
  local idx="$1"
  echo $((BASE_PORT + (idx - 1) * 10 + 2))
}

node_dir() {
  local idx="$1"
  echo "$ROOT_DIR/node$idx"
}

node_log() {
  local idx="$1"
  echo "$ROOT_DIR/node$idx.log"
}

node_pidfile() {
  local idx="$1"
  echo "$ROOT_DIR/node$idx.pid"
}

ensure_bins() {
  if [[ ! -x "$DAEMON_BIN" || ! -x "$CLI_BIN" ]]; then
    echo "Building lattice-daemon and lattice-cli..."
    (cd "$REPO_ROOT" && cargo build --release -p lattice-daemon -p lattice-cli)
  fi
}

is_pid_running() {
  local pid="$1"
  kill -0 "$pid" 2>/dev/null
}

wait_for_rpc() {
  local rpc_port="$1"
  local tries=120
  local i
  for ((i = 0; i < tries; i++)); do
    if "$CLI_BIN" --rpc-port "$rpc_port" status >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

peer_id_for_rpc() {
  local rpc_port="$1"
  "$CLI_BIN" --rpc-port "$rpc_port" status | awk '/Peer ID:/ {print $3; exit}'
}

write_config() {
  local idx="$1"
  local bootstrap_entry="$2"
  local p2p_port
  local rpc_port
  local http_port
  local dir

  p2p_port="$(node_p2p_port "$idx")"
  rpc_port="$(node_rpc_port "$idx")"
  http_port="$(node_http_port "$idx")"
  dir="$(node_dir "$idx")"

  mkdir -p "$dir"
  cat >"$dir/config.toml" <<EOF
listen_port = $p2p_port
rpc_port = $rpc_port
http_port = $http_port
listen_address = "127.0.0.1"
data_dir = "$dir"
bootstrap_peers = ["$bootstrap_entry"]
EOF
}

start_node() {
  local idx="$1"
  local pidfile
  local dir
  local log_file
  local p2p_port
  local rpc_port
  local http_port

  pidfile="$(node_pidfile "$idx")"
  if [[ -f "$pidfile" ]]; then
    local existing_pid
    existing_pid="$(cat "$pidfile")"
    if [[ -n "$existing_pid" ]] && is_pid_running "$existing_pid"; then
      echo "node$idx already running (pid $existing_pid)"
      return 0
    fi
    rm -f "$pidfile"
  fi

  dir="$(node_dir "$idx")"
  log_file="$(node_log "$idx")"
  p2p_port="$(node_p2p_port "$idx")"
  rpc_port="$(node_rpc_port "$idx")"
  http_port="$(node_http_port "$idx")"

  mkdir -p "$dir"
  LATTICE_DATA_DIR="$dir" \
    LATTICE_PORT="$p2p_port" \
    LATTICE_RPC_PORT="$rpc_port" \
    LATTICE_HTTP_PORT="$http_port" \
    RUST_LOG="$RUST_LOG_LEVEL" \
    "$DAEMON_BIN" >"$log_file" 2>&1 &

  local pid="$!"
  echo "$pid" >"$pidfile"
  echo "started node$idx (pid $pid) p2p=$p2p_port rpc=$rpc_port http=$http_port"
}

stop_node() {
  local idx="$1"
  local pidfile
  pidfile="$(node_pidfile "$idx")"
  if [[ ! -f "$pidfile" ]]; then
    return 0
  fi

  local pid
  pid="$(cat "$pidfile")"
  if [[ -n "$pid" ]] && is_pid_running "$pid"; then
    kill "$pid" 2>/dev/null || true
    sleep 0.2
    if is_pid_running "$pid"; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    echo "stopped node$idx (pid $pid)"
  fi
  rm -f "$pidfile"
}

print_status() {
  local idx
  for ((idx = 1; idx <= NODE_COUNT; idx++)); do
    local rpc_port
    rpc_port="$(node_rpc_port "$idx")"
    echo "---- node$idx (rpc:$rpc_port) ----"
    if "$CLI_BIN" --rpc-port "$rpc_port" status 2>/dev/null; then
      :
    else
      echo "not running"
    fi
  done
}

start_all() {
  ensure_bins
  mkdir -p "$ROOT_DIR"

  start_node 1
  if ! wait_for_rpc "$(node_rpc_port 1)"; then
    echo "node1 RPC did not come up. Check $(node_log 1)"
    exit 1
  fi

  local node1_peer_id
  node1_peer_id="$(peer_id_for_rpc "$(node_rpc_port 1)")"
  if [[ -z "$node1_peer_id" ]]; then
    echo "failed to read node1 peer id"
    exit 1
  fi
  local bootstrap_entry="/ip4/127.0.0.1/tcp/$(node_p2p_port 1)/p2p/$node1_peer_id"

  write_config 1 "$bootstrap_entry"
  stop_node 1
  start_node 1
  if ! wait_for_rpc "$(node_rpc_port 1)"; then
    echo "node1 RPC did not come back after local bootstrap config. Check $(node_log 1)"
    exit 1
  fi

  local idx
  for ((idx = 2; idx <= NODE_COUNT; idx++)); do
    write_config "$idx" "$bootstrap_entry"
  done

  for ((idx = 2; idx <= NODE_COUNT; idx++)); do
    start_node "$idx"
  done

  for ((idx = 2; idx <= NODE_COUNT; idx++)); do
    wait_for_rpc "$(node_rpc_port "$idx")" || true
  done

  echo
  echo "local net root: $ROOT_DIR"
  echo "nodes: $NODE_COUNT"
  echo "bootstrap: $bootstrap_entry"
  echo "logs:"
  for ((idx = 1; idx <= NODE_COUNT; idx++)); do
    echo "  $(node_log "$idx")"
  done
  echo
  print_status
}

stop_all() {
  if [[ ! -d "$ROOT_DIR" ]]; then
    return 0
  fi

  local pidfile
  for pidfile in "$ROOT_DIR"/node*.pid; do
    [[ -e "$pidfile" ]] || continue
    local idx
    idx="$(basename "$pidfile" .pid | sed 's/^node//')"
    if [[ "$idx" =~ ^[0-9]+$ ]]; then
      stop_node "$idx"
    fi
  done
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [start|stop|status|restart]

Env vars:
  LATTICE_LOCALNET_DIR        Base dir for node data/logs (default: /tmp/lattice-localnet)
  LATTICE_LOCALNET_BASE_PORT  First node P2P port (default: 19000)
  LATTICE_LOCALNET_NODES      Number of nodes to run (default: 3)
  RUST_LOG                    Daemon log level (default: info)
EOF
}

case "$CMD" in
  start)
    start_all
    ;;
  stop)
    stop_all
    ;;
  status)
    print_status
    ;;
  restart)
    stop_all
    start_all
    ;;
  *)
    usage
    exit 1
    ;;
esac
