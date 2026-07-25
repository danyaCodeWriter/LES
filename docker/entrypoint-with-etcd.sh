#!/bin/sh
# Start a single-node etcd, then coordination-server (same container, for local/dev).
set -eu

ETCD_BIN="${ETCD_BIN:-/usr/local/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/usr/local/bin/etcdctl}"
ETCD_DATA_DIR="${ETCD_DATA_DIR:-/var/lib/etcd}"
ETCD_CLIENT_URL="${ETCD_CLIENT_URL:-http://127.0.0.1:2379}"
ETCD_PEER_URL="${ETCD_PEER_URL:-http://127.0.0.1:2380}"

mkdir -p "$ETCD_DATA_DIR"

echo "[entrypoint] starting embedded etcd data-dir=$ETCD_DATA_DIR client=$ETCD_CLIENT_URL"
"$ETCD_BIN" \
  --name etcd0 \
  --data-dir "$ETCD_DATA_DIR" \
  --advertise-client-urls "$ETCD_CLIENT_URL" \
  --listen-client-urls http://0.0.0.0:2379 \
  --initial-advertise-peer-urls "$ETCD_PEER_URL" \
  --listen-peer-urls "$ETCD_PEER_URL" \
  --initial-cluster "etcd0=${ETCD_PEER_URL}" \
  --initial-cluster-state new \
  --initial-cluster-token coord-embedded \
  --auto-compaction-retention=1 \
  --quota-backend-bytes=2147483648 \
  &
ETCD_PID=$!

cleanup() {
  echo "[entrypoint] stopping etcd pid=$ETCD_PID"
  kill "$ETCD_PID" 2>/dev/null || true
  wait "$ETCD_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

export ETCDCTL_API=3
i=0
while [ "$i" -lt 60 ]; do
  if "$ETCDCTL_BIN" --endpoints="$ETCD_CLIENT_URL" endpoint health >/dev/null 2>&1; then
    echo "[entrypoint] etcd is healthy"
    break
  fi
  i=$((i + 1))
  sleep 0.5
done
if [ "$i" -ge 60 ]; then
  echo "[entrypoint] etcd failed to become healthy" >&2
  exit 1
fi

# App talks to etcd on localhost inside this container.
export COORDINATION_ETCD_ENDPOINTS_0="${COORDINATION_ETCD_ENDPOINTS_0:-http://127.0.0.1:2379}"

echo "[entrypoint] starting coordination-server (etcd=$COORDINATION_ETCD_ENDPOINTS_0)"
exec java \
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -jar /app/app.jar
