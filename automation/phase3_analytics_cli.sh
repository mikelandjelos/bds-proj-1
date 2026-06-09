#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

echo "[1/4] Rebuilding HDFS + processed baseline via phase 2 checkpoint"
"${REPO_ROOT}/automation/phase2_preprocessing.sh"

echo "[2/4] Running analytics count example"
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 12

echo "[3/4] Running analytics show example"
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --year 2018 \
  --month 7 \
  --min-speed 15 \
  --sort-by timestamp \
  --sort-desc \
  --limit 5

echo "[4/4] Running analytics stats example"
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  stats \
  --year 2018 \
  --group-by month \
  --metrics speed \
  --order-by records \
  --order-desc \
  --limit 12

echo
echo "Phase 3 analytics CLI checkpoint completed."
