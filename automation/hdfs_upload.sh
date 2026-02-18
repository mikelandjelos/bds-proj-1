#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

MAX_WAIT_SEC="${MAX_WAIT_SEC:-120}"
SLEEP_SEC=3

JUL_SRC="/input/unipi_ais_dynamic_2018/unipi_ais_dynamic_jul2018.csv"
DEC_SRC="/input/unipi_ais_dynamic_2018/unipi_ais_dynamic_dec2018.csv"

cd "${REPO_ROOT}"

echo "[1/6] Resetting previous HDFS services (clean start)"
docker compose down >/dev/null 2>&1 || true

echo "[2/6] Starting HDFS services"
docker compose up -d namenode datanode

echo "[3/6] Waiting for HDFS readiness"
elapsed=0
while true; do
  report="$(docker compose exec namenode hdfs dfsadmin -report 2>&1 || true)"
  if [[ "${report}" =~ Live[[:space:]]datanodes[[:space:]]\(([1-9][0-9]*)\) ]]; then
    break
  fi

  if (( elapsed >= MAX_WAIT_SEC )); then
    echo "HDFS not ready within ${MAX_WAIT_SEC}s (no live datanodes)." >&2
    echo "${report}" >&2
    exit 1
  fi

  sleep "${SLEEP_SEC}"
  elapsed=$((elapsed + SLEEP_SEC))
done

echo "[4/6] Verifying read-only input files are visible in namenode"
docker compose exec namenode ls -lh "${JUL_SRC}" "${DEC_SRC}"

echo "[5/6] Creating HDFS target folders and uploading files"
docker compose exec namenode hdfs dfs -mkdir -p /bds/proj1/raw /bds/proj1/processed
docker compose exec namenode hdfs dfsadmin -safemode wait
docker compose exec namenode hdfs dfs -put -f "${JUL_SRC}" /bds/proj1/raw/jul2018.csv
docker compose exec namenode hdfs dfs -put -f "${DEC_SRC}" /bds/proj1/raw/dec2018.csv

echo "[6/6] Verifying uploaded data"
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/raw
docker compose exec namenode hdfs dfs -count -h /bds/proj1/raw

echo
echo "Phase 1 upload completed."
