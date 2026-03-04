#!/usr/bin/env bash
set -euo pipefail

CMD="${1:-run}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
LOCALNET_SCRIPT="$REPO_ROOT/scripts/localnet.sh"
CLI_BIN="${LATTICE_CLI_BIN:-$REPO_ROOT/target/release/lattice-cli}"

NODES="${LATTICE_STRESS_NODES:-12}"
ROUNDS="${LATTICE_STRESS_ROUNDS:-3}"
SITE_NAME="${LATTICE_STRESS_NAME:-stress}"
STRESS_DIR="${LATTICE_STRESS_DIR:-/tmp/lattice-stress}"
KEEP_RUNNING="${LATTICE_STRESS_KEEP_RUNNING:-0}"
ASSET_FILES="${LATTICE_STRESS_ASSET_FILES:-30}"
ASSET_BYTES="${LATTICE_STRESS_ASSET_BYTES:-8192}"
SETTLE_SECS="${LATTICE_STRESS_SETTLE_SECS:-0}"
BASE_PORT="${LATTICE_LOCALNET_BASE_PORT:-19000}"

if ! [[ "$NODES" =~ ^[0-9]+$ ]] || (( NODES < 2 )); then
  echo "LATTICE_STRESS_NODES must be an integer >= 2"
  exit 1
fi
if ! [[ "$ROUNDS" =~ ^[0-9]+$ ]] || (( ROUNDS < 1 )); then
  echo "LATTICE_STRESS_ROUNDS must be an integer >= 1"
  exit 1
fi
if ! [[ "$ASSET_FILES" =~ ^[0-9]+$ ]] || (( ASSET_FILES < 1 )); then
  echo "LATTICE_STRESS_ASSET_FILES must be an integer >= 1"
  exit 1
fi
if ! [[ "$ASSET_BYTES" =~ ^[0-9]+$ ]] || (( ASSET_BYTES < 1 )); then
  echo "LATTICE_STRESS_ASSET_BYTES must be an integer >= 1"
  exit 1
fi
if ! [[ "$SETTLE_SECS" =~ ^[0-9]+$ ]]; then
  echo "LATTICE_STRESS_SETTLE_SECS must be an integer >= 0"
  exit 1
fi

node_rpc_port() {
  local idx="$1"
  echo $((BASE_PORT + (idx - 1) * 10 + 1))
}

node_http_port() {
  local idx="$1"
  echo $((BASE_PORT + (idx - 1) * 10 + 2))
}

create_site_round() {
  local round="$1"
  local site_dir="$STRESS_DIR/site-round-$round"

  rm -rf "$site_dir"
  mkdir -p "$site_dir/assets"

  cat >"$site_dir/index.html" <<EOF
<!doctype html>
<html>
<head><meta charset="utf-8"><title>stress round $round</title></head>
<body>
  <h1>stress round $round</h1>
  <p>nodes=$NODES rounds=$ROUNDS</p>
</body>
</html>
EOF

  local i
  for ((i = 1; i <= ASSET_FILES; i++)); do
    # Add enough content to exercise multi-block replication paths.
    head -c "$ASSET_BYTES" /dev/urandom | base64 >"$site_dir/assets/blob-$i.txt"
  done

  echo "$site_dir"
}

run_round() {
  local round="$1"
  local node_idx
  local fail_count=0
  local ok_count=0

  local site_dir
  site_dir="$(create_site_round "$round")"

  echo "=== round $round: publish from node1 ==="
  "$CLI_BIN" --rpc-port "$(node_rpc_port 1)" publish --dir "$site_dir" --name "$SITE_NAME"
  if (( SETTLE_SECS > 0 )); then
    echo "=== round $round: settling for ${SETTLE_SECS}s before fetch ==="
    sleep "$SETTLE_SECS"
  fi

  echo "=== round $round: fetch from nodes 2..$NODES ==="
  for ((node_idx = 2; node_idx <= NODES; node_idx++)); do
    local out_dir="$STRESS_DIR/fetch-round-$round-node-$node_idx"
    rm -rf "$out_dir"
    if "$CLI_BIN" --rpc-port "$(node_rpc_port "$node_idx")" fetch "$SITE_NAME" --out "$out_dir" >/tmp/lattice-stress-fetch-$node_idx.log 2>&1; then
      ok_count=$((ok_count + 1))
    else
      fail_count=$((fail_count + 1))
      echo "fetch failed on node$node_idx (see /tmp/lattice-stress-fetch-$node_idx.log)"
    fi
  done

  echo "=== round $round: HTTP checks ==="
  local check_nodes=(2 "$NODES")
  if (( NODES >= 6 )); then
    check_nodes+=(6)
  fi
  local check_idx
  for check_idx in "${check_nodes[@]}"; do
    local http_port
    http_port="$(node_http_port "$check_idx")"
    if curl -fsS -H "Host: ${SITE_NAME}.lat" "http://127.0.0.1:$http_port/" >/tmp/lattice-stress-http-$check_idx.html; then
      :
    else
      echo "http check failed on node$check_idx port $http_port"
      fail_count=$((fail_count + 1))
    fi
  done

  echo "round $round summary: fetch_ok=$ok_count fetch_fail=$fail_count"
  if (( fail_count > 0 )); then
    return 1
  fi
}

run_stress() {
  mkdir -p "$STRESS_DIR"
  export LATTICE_LOCALNET_NODES="$NODES"
  "$LOCALNET_SCRIPT" restart

  local round
  for ((round = 1; round <= ROUNDS; round++)); do
    run_round "$round"
  done
}

teardown() {
  export LATTICE_LOCALNET_NODES="$NODES"
  "$LOCALNET_SCRIPT" stop
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [run|stop]

Env vars:
  LATTICE_STRESS_NODES         Number of local nodes (default: 12)
  LATTICE_STRESS_ROUNDS        Number of publish/fetch rounds (default: 3)
  LATTICE_STRESS_NAME          Site name to publish/fetch (default: stress)
  LATTICE_STRESS_DIR           Working directory (default: /tmp/lattice-stress)
  LATTICE_STRESS_KEEP_RUNNING  1 to keep localnet running after run (default: 0)
  LATTICE_STRESS_ASSET_FILES   Number of generated asset files per round (default: 30)
  LATTICE_STRESS_ASSET_BYTES   Random bytes per asset before base64 (default: 8192)
  LATTICE_STRESS_SETTLE_SECS   Delay after publish before fetch (default: 0)
  LATTICE_LOCALNET_BASE_PORT   Base port for localnet (default: 19000)
EOF
}

case "$CMD" in
  run)
    if run_stress; then
      echo "stress test completed successfully"
      if [[ "$KEEP_RUNNING" != "1" ]]; then
        teardown
      fi
    else
      echo "stress test failed"
      if [[ "$KEEP_RUNNING" != "1" ]]; then
        teardown
      fi
      exit 1
    fi
    ;;
  stop)
    teardown
    ;;
  *)
    usage
    exit 1
    ;;
esac
