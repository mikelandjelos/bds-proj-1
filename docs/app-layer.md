# App layer (job submission)

This document defines the app layer as the control plane that submits Spark jobs.
For infra structure and top-level workflow visuals, use `docs/architecture.md` and `docs/strategy.md`.

## Service

- Compose service: `app` (`bds-app`)
- Image: `bde2020/spark-master:3.3.0-hadoop3.3`
- Purpose: submit jobs using `spark-submit` to cluster master

Environment in container:

- `SPARK_MASTER_URL=spark://spark-master:7077`
- `HDFS_URL=hdfs://namenode:9000`

## Start full stack

```bash
docker compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2 app
```

## Submit preprocessing job from app layer

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/preprocess_ais_hdfs.py \
  --input 'hdfs://namenode:9000/bds/proj1/raw/*.csv' \
  --output-base hdfs://namenode:9000/bds/proj1/processed \
  --exclude-file-regex '.*sample.*'
```

## Submit analytics CLI job (Phase 4)

Count example:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 12
```

Show/filter/sort example:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --year 2018 \
  --month 7 \
  --min-speed 15 \
  --sort-by timestamp \
  --sort-desc \
  --limit 10
```

Grouped stats example:

```bash
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
```

## Verify output

```bash
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/processed
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/clean
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/quarantine
docker compose exec namenode hdfs dfs -count -h /bds/proj1/processed/quality_report
```

## Notes

- Business jobs belong to app layer; Spark layer is execution only.
- App layer should be extended later with CLI/API interfaces.
- Detailed CLI reference: `docs/analytics-cli.md`.
- Benchmark runner reference: `docs/benchmarking.md`.
