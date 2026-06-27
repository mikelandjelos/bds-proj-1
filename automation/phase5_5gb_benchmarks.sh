#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

MAX_WAIT_SEC="${MAX_WAIT_SEC:-180}"
SLEEP_SEC=3
ITERATIONS="${ITERATIONS:-5}"
KEEP_5GB_HDFS="${KEEP_5GB_HDFS:-0}"

RAW_SOURCES=(
  "jan2018:/input/unipi_ais_dynamic_2018/unipi_ais_dynamic_jan2018.csv"
  "feb2018:/input/unipi_ais_dynamic_2018/unipi_ais_dynamic_feb2018.csv"
  "apr2018:/input/unipi_ais_dynamic_2018/unipi_ais_dynamic_apr2018.csv"
  "jul2018:/input/unipi_ais_dynamic_2018/unipi_ais_dynamic_jul2018.csv"
  "aug2018:/input/unipi_ais_dynamic_2018/unipi_ais_dynamic_aug2018.csv"
  "dec2018:/input/unipi_ais_dynamic_2018/unipi_ais_dynamic_dec2018.csv"
)

EXPERIMENT_ROOT="${EXPERIMENT_ROOT:-/bds/proj1_5gb}"
RAW_INPUT="${RAW_INPUT:-${EXPERIMENT_ROOT}/raw}"
PROCESSED_ROOT="${PROCESSED_ROOT:-hdfs://namenode:9000${EXPERIMENT_ROOT}/processed}"
PROCESSED_INPUT="${PROCESSED_INPUT:-${PROCESSED_ROOT}/clean}"

cd "${REPO_ROOT}"

cleanup() {
  if [[ "${KEEP_5GB_HDFS}" == "1" ]]; then
    echo "[cleanup] KEEP_5GB_HDFS=1, leaving ${EXPERIMENT_ROOT} in HDFS"
    return
  fi

  echo "[cleanup] Removing temporary 5GB HDFS experiment data: ${EXPERIMENT_ROOT}"
  docker compose exec namenode hdfs dfs -rm -r -f "${EXPERIMENT_ROOT}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_hdfs() {
  echo "[wait] Waiting for HDFS readiness"
  local elapsed=0
  local report

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
}

wait_for_spark() {
  echo "[wait] Waiting for Spark master to report alive workers"
  local elapsed=0
  local master_json

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
}

run_campaign() {
  local benchmark_name="$1"
  local query_override="$2"
  local suffix="$3"
  local campaign_id
  local runs_dir
  local reports_dir

  campaign_id="$(date -u +%Y%m%dT%H%M%SZ)_5gb_${suffix}"
  runs_dir="runs/campaign_${campaign_id}/runs"
  reports_dir="runs/campaign_${campaign_id}/reports"
  mkdir -p "${runs_dir}" "${reports_dir}"

  echo
  echo "[campaign:${suffix}] benchmark=${benchmark_name}"
  echo "[campaign:${suffix}] runs_dir=${runs_dir}"
  echo "[campaign:${suffix}] reports_dir=${reports_dir}"

  for ((i = 1; i <= ITERATIONS; i++)); do
    echo "[campaign:${suffix}] iteration=${i}/${ITERATIONS}"

    python3 scripts/benchmark_ais_analytics.py \
      --benchmark-name "${benchmark_name}" \
      --base-dir "${runs_dir}" \
      --modes both \
      --repeats 1 \
      --query-override "${query_override}" \
      --raw-input "${RAW_INPUT}" \
      --input "${PROCESSED_INPUT}"

    python3 scripts/aggregate_benchmark_runs.py \
      --runs-dir "${runs_dir}" \
      --output-root "${reports_dir}" \
      --latest-only
  done

  echo "[campaign:${suffix}] final aggregate"
  python3 scripts/aggregate_benchmark_runs.py \
    --runs-dir "${runs_dir}" \
    --output-root "${reports_dir}"

  echo "[campaign:${suffix}] reports"
  ls -1 "${reports_dir}"
}

echo "[1/8] Resetting stack for a clean 5GB benchmark run"
docker compose down >/dev/null 2>&1 || true

echo "[2/8] Starting HDFS, Spark, and app services"
docker compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2 app
wait_for_hdfs
wait_for_spark

echo "[3/8] Verifying read-only input files are visible in namenode"
for source_entry in "${RAW_SOURCES[@]}"; do
  source_path="${source_entry#*:}"
  docker compose exec namenode ls -lh "${source_path}"
done

echo "[4/8] Creating approximately 5GB raw input in HDFS from real monthly files"
docker compose exec namenode hdfs dfsadmin -safemode wait
docker compose exec namenode hdfs dfs -rm -r -f "${EXPERIMENT_ROOT}" >/dev/null 2>&1 || true
docker compose exec namenode hdfs dfs -mkdir -p "${RAW_INPUT}" "${EXPERIMENT_ROOT}/processed"

for source_entry in "${RAW_SOURCES[@]}"; do
  source_name="${source_entry%%:*}"
  source_path="${source_entry#*:}"
  echo "[raw:${source_name}] uploading ${source_path}"
  docker compose exec namenode hdfs dfs -put -f \
    "${source_path}" \
    "${RAW_INPUT}/${source_name}.csv"
done

echo "[5/8] Verifying temporary raw input size"
docker compose exec namenode hdfs dfs -ls -h "${RAW_INPUT}"
docker compose exec namenode hdfs dfs -count -h "${RAW_INPUT}"

echo "[6/8] Running preprocessing over temporary 5GB raw input"
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/preprocess_ais_hdfs.py \
  --input "hdfs://namenode:9000${RAW_INPUT}/*.csv" \
  --output-base "${PROCESSED_ROOT}" \
  --exclude-file-regex '.*sample.*' \
  --mode overwrite

echo "[7/8] Verifying processed outputs"
docker compose exec namenode hdfs dfs -ls -h "${EXPERIMENT_ROOT}/processed"
docker compose exec namenode hdfs dfs -count -h "${EXPERIMENT_ROOT}/processed/clean"
docker compose exec namenode hdfs dfs -count -h "${EXPERIMENT_ROOT}/processed/quarantine"
docker compose exec namenode hdfs dfs -count -h "${EXPERIMENT_ROOT}/processed/quality_report"

echo "[8/8] Running 5GB benchmark campaigns"
run_campaign \
  "ais_stats_monthly_speed_5gb" \
  "stats --year 2018 --group-by month --metrics speed --order-by month --limit 12" \
  "stats_speed"

run_campaign \
  "ais_stats_monthly_heading_course_5gb" \
  "stats --year 2018 --group-by month --metrics heading,course --order-by month --limit 12" \
  "stats_heading_course"

run_campaign \
  "ais_count_dec2018_5gb" \
  "count --year 2018 --month 12" \
  "count"

run_campaign \
  "ais_show_fast_jul2018_5gb" \
  "show --year 2018 --month 7 --min-speed 15 --sort-by timestamp --sort-desc --limit 20" \
  "show"

echo
echo "Phase 5 5GB benchmark checkpoint completed."
echo "Reports are preserved under runs/campaign_*_5gb_*/reports."
echo "Temporary HDFS data will be removed unless KEEP_5GB_HDFS=1."
