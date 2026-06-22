#!/usr/bin/env bash
# =====================================================================
#  OPTIONAL manual HDFS check / seed.
#  The placement engine already self-seeds the cloud tier on startup, so you
#  normally do NOT need this. Use it to independently verify HDFS works or to
#  inspect what the engine wrote.
#
#  Usage:  ./scripts/bootstrap_hdfs.sh
# =====================================================================
set -euo pipefail

echo "==> HDFS report:"
docker compose exec namenode hdfs dfsadmin -report || true

echo "==> Listing /datasets in HDFS (written by the engine when backend=hdfs):"
docker compose exec namenode hdfs dfs -ls /datasets 2>/dev/null | head -n 20 || \
  echo "   (no /datasets yet — engine may be using the simulated fallback)"

echo "==> Done. NameNode web UI: http://localhost:9870"
