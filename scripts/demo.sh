#!/usr/bin/env bash
# =====================================================================
#  One-command demo (Linux / macOS / Git-Bash).
#  Builds + starts the stack, waits for the dashboard, tails engine logs.
#  Usage:   ./scripts/demo.sh        Stop: docker compose down [-v]
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Building and starting the stack (first run pulls images)..."
docker compose up -d --build

echo "==> Waiting for the dashboard at http://localhost:8501 ..."
ready=0
for _ in $(seq 1 60); do
  if curl -sf http://localhost:8501 >/dev/null 2>&1; then ready=1; break; fi
  sleep 3
done
[ "$ready" = "1" ] && echo "==> Dashboard is up." || echo "!! Dashboard not reachable yet; check 'docker compose ps'."

cat <<'EOF'

Open these in your browser:
   Dashboard (metrics) : http://localhost:8501
   Spark UI            : http://localhost:4040
   HDFS NameNode UI    : http://localhost:9870

Tip: every ~60s the workload's hot set shifts — watch the cache-hit rate dip
     then recover as the engine re-learns. That is the adaptation story.

==> Tailing placement-engine logs (Ctrl+C to stop log view; stack keeps running)...
EOF
docker compose logs -f engine
