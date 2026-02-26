#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

ITERATIONS="${ITERATIONS:-5}"
RAW_INPUT="${RAW_INPUT:-/bds/proj1/raw}"
PROCESSED_INPUT="${PROCESSED_INPUT:-hdfs://namenode:9000/bds/proj1/processed/clean}"

cd "${REPO_ROOT}"

run_campaign() {
  local benchmark_name="$1"
  local query_override="$2"
  local suffix="$3"
  local campaign_id
  local runs_dir
  local reports_dir

  campaign_id="$(date -u +%Y%m%dT%H%M%SZ)_${suffix}"
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

echo "[1/5] Rebuilding storage + compute + processed baseline"
"${REPO_ROOT}/automation/phase2_preprocessing.sh"

echo "[2/5] Running benchmark campaign: stats speed"
run_campaign \
  "ais_stats_monthly_speed_1gb" \
  "stats --year 2018 --group-by month --metrics speed --order-by month --limit 12" \
  "stats_speed"

echo "[3/5] Running benchmark campaign: stats heading/course"
run_campaign \
  "ais_stats_monthly_heading_course_1gb" \
  "stats --year 2018 --group-by month --metrics heading,course --order-by month --limit 12" \
  "stats_heading_course"

echo "[4/5] Running benchmark campaign: count"
run_campaign \
  "ais_count_dec2018_1gb" \
  "count --year 2018 --month 12" \
  "count"

echo "[5/5] Running benchmark campaign: show"
run_campaign \
  "ais_show_fast_jul2018_1gb" \
  "show --year 2018 --month 7 --min-speed 15 --sort-by timestamp --sort-desc --limit 20" \
  "show"

echo
echo "Phase 5 full benchmark checkpoint completed."
