# bds-proj-1

## What this repo is
Course project for Big Data Systems using the Piraeus AIS dataset.

- Dataset: [The Piraeus AIS Dataset for Large-scale Maritime Data Analytics](https://zenodo.org/records/5792100#.Yd63IFko9PY)
- Paper: [The Piraeus AIS dataset for large-scale maritime data analytics](https://www.sciencedirect.com/science/article/pii/S2352340921010568)

## Architecture direction
Target architecture is a 3-layer model:

1. HDFS layer (storage/persistence)
2. Spark layer (cluster compute)
3. App layer (scripts/CLI/API orchestration)

Preprocessing is defined as an App-layer job submitted to Spark and reading/writing HDFS.

## Current status
- Active step: Spark layer infrastructure bring-up (master + workers).
- Spark analytics/API phase is not started yet.

## Documentation

- Architecture: `docs/architecture.md`
- HDFS workflow: `docs/hdfs.md`
- Spark layer workflow: `docs/spark-layer.md`
- Preprocessing workflow: `docs/preprocessing.md`
- Atomic execution plan: `docs/strategy.md`
