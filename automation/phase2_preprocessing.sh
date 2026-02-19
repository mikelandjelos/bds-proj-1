#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

MAX_WAIT_SEC="${MAX_WAIT_SEC:-120}"
SLEEP_SEC=3

cd "${REPO_ROOT}"

echo "[1/5] Preparing raw HDFS input via phase 1 checkpoint"
"${REPO_ROOT}/automation/phase1_hdfs_upload.sh"

echo "[2/5] Starting Spark + App layer services"
docker compose up -d spark-master spark-worker-1 spark-worker-2 app

echo "[3/5] Waiting for Spark master to report alive workers"
elapsed=0
while true; do
  master_json="$(docker compose exec spark-master sh -lc "wget -qO- http://localhost:8080/json/" 2>/dev/null || true)"
  if [[ "${master_json}" =~ \"aliveworkers\"[[:space:]]*:[[:space:]]*[1-9] ]]; then
    break
  fi

  if (( elapsed >= MAX_WAIT_SEC )); then
    echo "Spark master did not report live workers within ${MAX_WAIT_SEC}s." >&2
    exit 1
  fi

  sleep "${SLEEP_SEC}"
  elapsed=$((elapsed + SLEEP_SEC))
done

echo "[4/5] Running preprocessing job from app layer"
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/preprocess_ais_hdfs.py \
  --input 'hdfs://namenode:9000/bds/proj1/raw/*.csv' \
  --output-base hdfs://namenode:9000/bds/proj1/processed \
  --exclude-file-regex '.*sample.*' \
  --mode overwrite

echo "[5/5] Verifying processed outputs in HDFS"
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/processed
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/clean
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/quarantine
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/quality_report

echo
echo "Phase 2 preprocessing checkpoint completed."
