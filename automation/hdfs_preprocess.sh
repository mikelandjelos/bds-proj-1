#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

echo "[1/4] Ensuring HDFS services are running"
docker compose up -d namenode datanode

echo "[2/4] Running PySpark preprocessing service"
docker compose run --rm spark-preprocess "$@"

echo "[3/4] Verifying processed outputs in HDFS"
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/processed
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/processed/clean
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/processed/quarantine
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/processed/quality_report

echo "[4/4] Counting records/files"
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/clean
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/quarantine
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/quality_report

echo
echo "Phase 2 preprocessing completed."
