#!/usr/bin/env bash
set -euo pipefail

CMD="${1:-run}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
LOCALNET_SCRIPT="$REPO_ROOT/scripts/localnet.sh"
CLI_BIN="${LATTICE_CLI_BIN:-$REPO_ROOT/target/release/lattice-cli}"

NODES="${LATTICE_STRESS_NODES:-12}"
ROUNDS="${LATTICE_STRESS_ROUNDS:-3}"
SITE_NAME="${LATTICE_STRESS_NAME:-stress-${RANDOM}-$$}"
STRESS_DIR="${LATTICE_STRESS_DIR:-/tmp/lattice-stress}"
KEEP_RUNNING="${LATTICE_STRESS_KEEP_RUNNING:-0}"
RESET_LOCALNET="${LATTICE_STRESS_RESET_LOCALNET:-1}"
ASSET_FILES="${LATTICE_STRESS_ASSET_FILES:-30}"
ASSET_BYTES="${LATTICE_STRESS_ASSET_BYTES:-8192}"
VIDEO_BYTES="${LATTICE_STRESS_VIDEO_BYTES:-1048576}"
SETTLE_SECS="${LATTICE_STRESS_SETTLE_SECS:-0}"
RANGE_CHECK="${LATTICE_STRESS_RANGE_CHECK:-1}"
HTTPS_PROXY_CHECK="${LATTICE_STRESS_HTTPS_PROXY_CHECK:-1}"
DIRECT_HTTPS_CHECK="${LATTICE_STRESS_DIRECT_HTTPS_CHECK:-1}"
RESTART_CHECK="${LATTICE_STRESS_RESTART_CHECK:-1}"
RESTART_SETTLE_SECS="${LATTICE_STRESS_RESTART_SETTLE_SECS:-5}"
FETCH_RETRIES="${LATTICE_STRESS_FETCH_RETRIES:-10}"
FETCH_RETRY_SECS="${LATTICE_STRESS_FETCH_RETRY_SECS:-1}"
DEFAULT_BASE_PORT=$((20000 + (RANDOM % 20000)))
BASE_PORT="${LATTICE_LOCALNET_BASE_PORT:-$DEFAULT_BASE_PORT}"
LOCALNET_ROOT="${LATTICE_LOCALNET_DIR:-/tmp/lattice-localnet-${SITE_NAME}}"
LAST_SITE_DIR=""

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
if ! [[ "$VIDEO_BYTES" =~ ^[0-9]+$ ]] || (( VIDEO_BYTES < 1 )); then
  echo "LATTICE_STRESS_VIDEO_BYTES must be an integer >= 1"
  exit 1
fi
if ! [[ "$SETTLE_SECS" =~ ^[0-9]+$ ]]; then
  echo "LATTICE_STRESS_SETTLE_SECS must be an integer >= 0"
  exit 1
fi
if ! [[ "$RESTART_SETTLE_SECS" =~ ^[0-9]+$ ]]; then
  echo "LATTICE_STRESS_RESTART_SETTLE_SECS must be an integer >= 0"
  exit 1
fi
if ! [[ "$FETCH_RETRIES" =~ ^[0-9]+$ ]] || (( FETCH_RETRIES < 1 )); then
  echo "LATTICE_STRESS_FETCH_RETRIES must be an integer >= 1"
  exit 1
fi
if ! [[ "$FETCH_RETRY_SECS" =~ ^[0-9]+$ ]]; then
  echo "LATTICE_STRESS_FETCH_RETRY_SECS must be an integer >= 0"
  exit 1
fi
if [[ "$RANGE_CHECK" != "0" && "$RANGE_CHECK" != "1" ]]; then
  echo "LATTICE_STRESS_RANGE_CHECK must be 0 or 1"
  exit 1
fi
if [[ "$HTTPS_PROXY_CHECK" != "0" && "$HTTPS_PROXY_CHECK" != "1" ]]; then
  echo "LATTICE_STRESS_HTTPS_PROXY_CHECK must be 0 or 1"
  exit 1
fi
if [[ "$DIRECT_HTTPS_CHECK" != "0" && "$DIRECT_HTTPS_CHECK" != "1" ]]; then
  echo "LATTICE_STRESS_DIRECT_HTTPS_CHECK must be 0 or 1"
  exit 1
fi
if [[ "$RESTART_CHECK" != "0" && "$RESTART_CHECK" != "1" ]]; then
  echo "LATTICE_STRESS_RESTART_CHECK must be 0 or 1"
  exit 1
fi
if [[ "$RESET_LOCALNET" != "0" && "$RESET_LOCALNET" != "1" ]]; then
  echo "LATTICE_STRESS_RESET_LOCALNET must be 0 or 1"
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

node_proxy_port() {
  local idx="$1"
  echo $((BASE_PORT + (idx - 1) * 10 + 4))
}

node_https_port() {
  local idx="$1"
  echo $((BASE_PORT + (idx - 1) * 10 + 3))
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
    head -c "$ASSET_BYTES" /dev/urandom | base64 >"$site_dir/assets/blob-$i.txt"
  done
  head -c "$VIDEO_BYTES" /dev/urandom >"$site_dir/assets/video.bin"

  echo "$site_dir"
}

assert_range_bytes() {
  local site_dir="$1"
  local node_idx="$2"
  local range_header="$3"
  local expected_skip="$4"
  local expected_count="$5"
  local label="$6"
  local http_port
  http_port="$(node_http_port "$node_idx")"

  local expected_file="$STRESS_DIR/expected-${label}-node${node_idx}.bin"
  local actual_file="$STRESS_DIR/actual-${label}-node${node_idx}.bin"
  local header_file="$STRESS_DIR/headers-${label}-node${node_idx}.txt"
  dd if="$site_dir/assets/video.bin" of="$expected_file" bs=1 skip="$expected_skip" count="$expected_count" status=none
  if ! curl -fsS \
    -D "$header_file" \
    -H "Host: ${SITE_NAME}.loom" \
    -H "Range: $range_header" \
    "http://127.0.0.1:$http_port/assets/video.bin" \
    -o "$actual_file"; then
    echo "range check $label on node$node_idx request failed" >&2
    return 1
  fi

  if ! grep -q "206" "$header_file"; then
    echo "range check $label on node$node_idx did not return HTTP 206" >&2
    return 1
  fi
  if ! cmp -s "$expected_file" "$actual_file"; then
    echo "range check $label on node$node_idx returned unexpected bytes" >&2
    return 1
  fi
}

run_range_checks() {
  local site_dir="$1"
  local fail_count="$2"
  local check_nodes=(2 "$NODES")
  local check_idx
  for check_idx in "${check_nodes[@]}"; do
    local first_count=128
    if (( VIDEO_BYTES < first_count )); then
      first_count="$VIDEO_BYTES"
    fi
    local mid_start=4096
    if (( VIDEO_BYTES <= mid_start )); then
      mid_start=$(( VIDEO_BYTES / 3 ))
    fi
    local mid_end=8191
    if (( VIDEO_BYTES <= mid_end )); then
      mid_end=$(( VIDEO_BYTES - 1 ))
    fi
    local mid_count=$((mid_end - mid_start + 1))
    local suffix_count=512
    if (( VIDEO_BYTES < suffix_count )); then
      suffix_count="$VIDEO_BYTES"
    fi

    if ! assert_range_bytes "$site_dir" "$check_idx" "bytes=0-$((first_count - 1))" 0 "$first_count" "first"; then
      fail_count=$((fail_count + 1))
    fi
    if ! assert_range_bytes "$site_dir" "$check_idx" "bytes=${mid_start}-${mid_end}" "$mid_start" "$mid_count" "middle"; then
      fail_count=$((fail_count + 1))
    fi
    if ! assert_range_bytes "$site_dir" "$check_idx" "bytes=-${suffix_count}" "$((VIDEO_BYTES - suffix_count))" "$suffix_count" "suffix"; then
      fail_count=$((fail_count + 1))
    fi
  done

  echo "$fail_count"
}

run_round() {
  local round="$1"
  local node_idx
  local fail_count=0
  local ok_count=0

  local site_dir
  site_dir="$(create_site_round "$round")"
  LAST_SITE_DIR="$site_dir"

  echo "=== round $round: publish from node1 ==="
  if ! "$CLI_BIN" --rpc-port "$(node_rpc_port 1)" publish --dir "$site_dir" --name "$SITE_NAME"; then
    echo "publish failed for ${SITE_NAME}.loom in round $round" >&2
    return 1
  fi
  if (( SETTLE_SECS > 0 )); then
    echo "=== round $round: settling for ${SETTLE_SECS}s before fetch ==="
    sleep "$SETTLE_SECS"
  fi

  echo "=== round $round: fetch from nodes 2..$NODES ==="
  for ((node_idx = 2; node_idx <= NODES; node_idx++)); do
    local out_dir="$STRESS_DIR/fetch-round-$round-node-$node_idx"
    rm -rf "$out_dir"
    local attempt
    local fetched=0
    for ((attempt = 1; attempt <= FETCH_RETRIES; attempt++)); do
      if "$CLI_BIN" --rpc-port "$(node_rpc_port "$node_idx")" fetch "$SITE_NAME" --out "$out_dir" >/tmp/lattice-stress-fetch-$node_idx.log 2>&1; then
        fetched=1
        break
      fi
      rm -rf "$out_dir"
      if (( attempt < FETCH_RETRIES )) && (( FETCH_RETRY_SECS > 0 )); then
        sleep "$FETCH_RETRY_SECS"
      fi
    done
    if (( fetched == 1 )); then
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
    if curl -fsS -H "Host: ${SITE_NAME}.loom" "http://127.0.0.1:$http_port/" >/tmp/lattice-stress-http-$check_idx.html; then
      :
    else
      echo "http check failed on node$check_idx port $http_port"
      fail_count=$((fail_count + 1))
    fi
  done

  if [[ "$RANGE_CHECK" == "1" ]]; then
    echo "=== round $round: HTTP range checks ==="
    fail_count="$(run_range_checks "$site_dir" "$fail_count")"
  fi

  if [[ "$HTTPS_PROXY_CHECK" == "1" ]]; then
    echo "=== round $round: HTTPS proxy checks ==="
    for check_idx in "${check_nodes[@]}"; do
      local proxy_port
      local ca_path
      proxy_port="$(node_proxy_port "$check_idx")"
      ca_path="${LOCALNET_ROOT}/node${check_idx}/tls/lattice-local-ca.pem"
      if ! curl -fsS \
        --proxy "http://127.0.0.1:$proxy_port" \
        --cacert "$ca_path" \
        "https://${SITE_NAME}.loom/" >/tmp/lattice-stress-https-$check_idx.html; then
        echo "https proxy check failed on node$check_idx proxy $proxy_port"
        fail_count=$((fail_count + 1))
      fi
    done
  fi

  if [[ "$DIRECT_HTTPS_CHECK" == "1" ]]; then
    echo "=== round $round: direct HTTPS checks ==="
    for check_idx in "${check_nodes[@]}"; do
      local https_port
      local ca_path
      https_port="$(node_https_port "$check_idx")"
      ca_path="${LOCALNET_ROOT}/node${check_idx}/tls/lattice-local-ca.pem"
      if ! curl -fsS \
        --resolve "${SITE_NAME}.loom.lattice.localhost:${https_port}:127.0.0.1" \
        --cacert "$ca_path" \
        "https://${SITE_NAME}.loom.lattice.localhost:${https_port}/" \
        >/tmp/lattice-stress-direct-https-$check_idx.html; then
        echo "direct https check failed on node$check_idx https $https_port"
        fail_count=$((fail_count + 1))
      fi
    done
  fi

  echo "round $round summary: fetch_ok=$ok_count fetch_fail=$fail_count"
  if (( fail_count > 0 )); then
    return 1
  fi
}

run_stress() {
  export LATTICE_LOCALNET_BASE_PORT="$BASE_PORT"
  export LATTICE_LOCALNET_DIR="$LOCALNET_ROOT"
  if [[ "$RESET_LOCALNET" == "1" ]]; then
    export LATTICE_LOCALNET_NODES="$NODES"
    "$LOCALNET_SCRIPT" stop || true
    rm -rf "$LOCALNET_ROOT" "$STRESS_DIR"
  fi
  mkdir -p "$STRESS_DIR"
  export LATTICE_LOCALNET_NODES="$NODES"
  "$LOCALNET_SCRIPT" restart

  local round
  for ((round = 1; round <= ROUNDS; round++)); do
    if ! run_round "$round"; then
      return 1
    fi
  done

  if [[ "$RESTART_CHECK" == "1" ]]; then
    if ! run_restart_check; then
      return 1
    fi
  fi
}

run_restart_check() {
  export LATTICE_LOCALNET_BASE_PORT="$BASE_PORT"
  export LATTICE_LOCALNET_DIR="$LOCALNET_ROOT"
  if [[ -z "$LAST_SITE_DIR" || ! -d "$LAST_SITE_DIR" ]]; then
    echo "restart check skipped: no prior site directory available"
    return 0
  fi

  echo "=== restart check: restarting localnet ==="
  export LATTICE_LOCALNET_NODES="$NODES"
  "$LOCALNET_SCRIPT" restart
  if (( RESTART_SETTLE_SECS > 0 )); then
    sleep "$RESTART_SETTLE_SECS"
  fi

  echo "=== restart check: fetch from node2 ==="
  local restart_out="$STRESS_DIR/restart-fetch-node2"
  rm -rf "$restart_out"
  local attempt
  for ((attempt = 1; attempt <= FETCH_RETRIES; attempt++)); do
    if "$CLI_BIN" --rpc-port "$(node_rpc_port 2)" fetch "$SITE_NAME" --out "$restart_out"; then
      break
    fi
    rm -rf "$restart_out"
    if (( attempt == FETCH_RETRIES )); then
      return 1
    fi
    if (( FETCH_RETRY_SECS > 0 )); then
      sleep "$FETCH_RETRY_SECS"
    fi
  done

  local http_port
  http_port="$(node_http_port 2)"
  curl -fsS -H "Host: ${SITE_NAME}.loom" "http://127.0.0.1:$http_port/" >/tmp/lattice-stress-restart-http.html

  if [[ "$RANGE_CHECK" == "1" ]]; then
    echo "=== restart check: HTTP range ==="
    local restart_first=128
    if (( VIDEO_BYTES < restart_first )); then
      restart_first="$VIDEO_BYTES"
    fi
    local restart_suffix=512
    if (( VIDEO_BYTES < restart_suffix )); then
      restart_suffix="$VIDEO_BYTES"
    fi
    assert_range_bytes "$LAST_SITE_DIR" 2 "bytes=0-$((restart_first - 1))" 0 "$restart_first" "restart-first"
    assert_range_bytes "$LAST_SITE_DIR" 2 "bytes=-${restart_suffix}" "$((VIDEO_BYTES - restart_suffix))" "$restart_suffix" "restart-suffix"
  fi

  if [[ "$HTTPS_PROXY_CHECK" == "1" ]]; then
    local proxy_port
    local ca_path
    proxy_port="$(node_proxy_port 2)"
    ca_path="${LOCALNET_ROOT}/node2/tls/lattice-local-ca.pem"
    curl -fsS \
      --proxy "http://127.0.0.1:$proxy_port" \
      --cacert "$ca_path" \
      "https://${SITE_NAME}.loom/" >/tmp/lattice-stress-restart-https.html
  fi

  if [[ "$DIRECT_HTTPS_CHECK" == "1" ]]; then
    local https_port
    local ca_path
    https_port="$(node_https_port 2)"
    ca_path="${LOCALNET_ROOT}/node2/tls/lattice-local-ca.pem"
    curl -fsS \
      --resolve "${SITE_NAME}.loom.lattice.localhost:${https_port}:127.0.0.1" \
      --cacert "$ca_path" \
      "https://${SITE_NAME}.loom.lattice.localhost:${https_port}/" \
      >/tmp/lattice-stress-restart-direct-https.html
  fi
}

teardown() {
  export LATTICE_LOCALNET_BASE_PORT="$BASE_PORT"
  export LATTICE_LOCALNET_DIR="$LOCALNET_ROOT"
  export LATTICE_LOCALNET_NODES="$NODES"
  "$LOCALNET_SCRIPT" stop
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [run|stop]

Env vars:
  LATTICE_STRESS_NODES         Number of local nodes (default: 12)
  LATTICE_STRESS_ROUNDS        Number of publish/fetch rounds (default: 3)
  LATTICE_STRESS_NAME          Site name to publish/fetch (default: stress-<random>-<pid>)
  LATTICE_STRESS_DIR           Working directory (default: /tmp/lattice-stress)
  LATTICE_STRESS_KEEP_RUNNING  1 to keep localnet running after run (default: 0)
  LATTICE_STRESS_RESET_LOCALNET 1 to wipe localnet/state before run (default: 1)
  LATTICE_STRESS_ASSET_FILES   Number of generated asset files per round (default: 30)
  LATTICE_STRESS_ASSET_BYTES   Random bytes per asset before base64 (default: 8192)
  LATTICE_STRESS_VIDEO_BYTES   Binary test payload size in bytes (default: 1048576)
  LATTICE_STRESS_SETTLE_SECS   Delay after publish before fetch (default: 0)
  LATTICE_STRESS_RANGE_CHECK   1 to validate HTTP Range bytes (default: 1)
  LATTICE_STRESS_HTTPS_PROXY_CHECK 1 to validate HTTPS via local proxy (default: 1)
  LATTICE_STRESS_DIRECT_HTTPS_CHECK 1 to validate direct HTTPS on :7443 (default: 1)
  LATTICE_STRESS_RESTART_CHECK 1 to restart nodes and verify persistence (default: 1)
  LATTICE_STRESS_RESTART_SETTLE_SECS Delay after restart before checks (default: 5)
  LATTICE_STRESS_FETCH_RETRIES Number of fetch attempts before failing (default: 10)
  LATTICE_STRESS_FETCH_RETRY_SECS Delay between fetch attempts (default: 1)
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
