# Spark layer (infrastructure)

This document covers only Spark cluster infrastructure (no app logic).
For infra structure and top-level workflow visuals, use `docs/architecture.md` and `docs/strategy.md`.

## Services

- `spark-master` (`bds-spark-master`)
- `spark-worker-1` (`bds-spark-worker-1`)
- `spark-worker-2` (`bds-spark-worker-2`)

## Start only Spark layer

```bash
docker compose up -d spark-master spark-worker-1 spark-worker-2
```

## Start full storage + compute stack

```bash
docker compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2
```

## Verify

```bash
docker compose ps
```

Spark Master UI:

- `http://localhost:8080`

Expected:

- master is running on `spark://spark-master:7077`
- both workers are registered in the master UI

## Stop Spark layer

```bash
docker compose stop spark-worker-1 spark-worker-2 spark-master
```

## Notes

- Spark layer is compute-only; data remains in HDFS.
- App layer will later submit jobs to `spark://spark-master:7077`.
