# Phase 5 (step 1): Benchmark runner

This step introduces one benchmark program with timestamped, append-only run artifacts.

Script:

- `scripts/benchmark_ais_analytics.py`

Benchmark program executed in this step:

- `ais_analytics.py stats --year 2018 --group-by month --metrics speed --order-by month --limit 12`

Execution modes:

- standalone: `--master local[*]`
- cluster: `--master spark://spark-master:7077`

## Run benchmark

From repo root:

```bash
python3 scripts/benchmark_ais_analytics.py --modes both --repeats 3
```

Quick smoke run:

```bash
python3 scripts/benchmark_ais_analytics.py --modes both --repeats 1
```

Dry run (no execution):

```bash
python3 scripts/benchmark_ais_analytics.py --modes both --repeats 1 --dry-run
```

## Aggregate benchmark history

Generate a timestamped aggregation report from all saved runs:

```bash
python3 scripts/aggregate_benchmark_runs.py
```

Latest-only report (one row per mode):

```bash
python3 scripts/aggregate_benchmark_runs.py --latest-only
```

Aggregation outputs are written to:

- `runs/reports/<timestamp>/aggregate.json`
- `runs/reports/<timestamp>/aggregate.csv`
- `runs/reports/<timestamp>/aggregate.md`

## Output layout

Runs are stored under:

- `runs/standalone/<timestamp>/`
- `runs/cluster/<timestamp>/`

Timestamps are UTC and never overwritten.

Each run directory contains:

- `meta.json` (benchmark/query metadata)
- `results.json` (per-iteration status and file references)
- `summary.json` (min/max/avg duration on successful iterations)
- `iter_XX.stdout.log` (spark-submit output)
- `iter_XX.stderr.log` (errors/warnings)

## Notes

- Runs are append-only and intended for later aggregation/report generation.
- `runs/` is git-ignored to keep repository history clean.
