# Phase 2: Preprocessing and reformatting

This phase converts raw AIS CSV from HDFS into three outputs on HDFS:

- `hdfs://namenode:9000/bds/proj1/processed/clean`
- `hdfs://namenode:9000/bds/proj1/processed/quarantine`
- `hdfs://namenode:9000/bds/proj1/processed/quality_report`

## Architecture placement
Preprocessing is an **App-layer job**.

Execution intent:
1. app triggers preprocess job
2. Spark executes transformations
3. HDFS is the only data source/sink

## Script used by app layer

- `scripts/preprocess_ais_hdfs.py`

This script is called from the app container with `/spark/bin/spark-submit`.

Checkpoint script:

- `automation/phase2_preprocessing.sh`

## Transformations

### 1. Schema enforcement
- `timestamp` (long), `vessel_id` (string), `lon/lat/heading/speed/course` (double).

### 2. Quality flags
- `missing_required`: missing `timestamp` or `vessel_id` or `lon` or `lat`.
- `invalid_geo`: out-of-range `lon/lat`.
- `invalid_speed`: `speed < 0`.
- `invalid_heading`: heading outside `[0, 360]`.
- `invalid_course`: course outside `[0, 360]`.
- `duplicate_row`: duplicate full-record signature.

### 3. Binning policy
- `clean`: records without hard failures.
- `quarantine`: records with hard failures (`missing_required`, `invalid_geo`, `duplicate_row`).
- soft-invalid movement fields (`speed/heading/course`) are set to `null` in `clean` and tracked with flags.

### 4. Reformat
- output format: Parquet.
- `clean` is partitioned by `year`, `month`.
- `quarantine` is partitioned by primary quarantine reason.

### 5. Quality report
- summary table (`total`, `clean`, `quarantine`, issue counters).
- reason counts table for quarantine reasons.

## Output verification

```bash
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/processed
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/clean
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/quarantine
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/quality_report
```
