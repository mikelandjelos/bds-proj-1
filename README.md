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

- HDFS upload flow is verified.
- Preprocessing writes Parquet outputs to HDFS via Spark.
- Layered HDFS + Spark + App submission flow is verified.
- Analytics CLI supports `count`, `show`, and grouped statistics (`min/max/avg/stddev` by group).
- Benchmark runner stores timestamped runs for standalone and cluster modes.
- Benchmark aggregation/reporting supports campaign-style run summaries.

## Documentation

- Architecture: `docs/architecture.md`
- HDFS workflow: `docs/hdfs.md`
- Spark layer workflow: `docs/spark-layer.md`
- App layer workflow: `docs/app-layer.md`
- Preprocessing workflow: `docs/preprocessing.md`
- Analytics CLI workflow: `docs/analytics-cli.md`
- Benchmark workflow: `docs/benchmarking.md`
- Atomic execution plan: `docs/strategy.md`

## Reproducibility checkpoints

After each completed and approved phase, a checkpoint script is added under `automation/` as:

- `phase<N>_<semantic_name>.sh` (example: `phase1_hdfs_upload.sh`)
- current checkpoints: `phase1_hdfs_upload.sh`, `phase2_preprocessing.sh`, `phase4_analytics_cli.sh`, `phase5_full_benchmarks.sh`

These scripts are created only after the phase is verified and docs are updated.

## Presentation generation

Marp source:

- `docs/presentation.md`

Generate outputs:

```bash
marp docs/presentation.md --pptx -o presentation.pptx
marp docs/presentation.md --pdf -o presentation.pdf
```
