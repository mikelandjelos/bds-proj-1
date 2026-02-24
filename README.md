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

- Phase 1 completed: HDFS upload flow is verified.
- Phase 2 completed: preprocessing writes Parquet outputs to HDFS via Spark.
- Phase 3 completed: layered HDFS + Spark + App submission flow is verified.
- Phase 4 step 1 completed: analytics CLI foundation (`count`, `show`, filter + sort).
- Phase 4 step 2 pending: grouped statistics (`min/max/mean/stddev by group`).

## Documentation

- Architecture: `docs/architecture.md`
- HDFS workflow: `docs/hdfs.md`
- Spark layer workflow: `docs/spark-layer.md`
- App layer workflow: `docs/app-layer.md`
- Preprocessing workflow: `docs/preprocessing.md`
- Analytics CLI workflow: `docs/analytics-cli.md`
- Atomic execution plan: `docs/strategy.md`

## Reproducibility checkpoints

After each completed and approved phase, a checkpoint script is added under `automation/` as:

- `phase<N>_<semantic_name>.sh` (example: `phase1_hdfs_upload.sh`)
- current checkpoints: `phase1_hdfs_upload.sh`, `phase2_preprocessing.sh`

These scripts are created only after the phase is verified and docs are updated.
