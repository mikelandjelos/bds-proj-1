# Benchmarking

This document describes how to run benchmark workloads, store timestamped run artifacts,
and generate summary reports.

Scripts:

- `scripts/benchmark_ais_analytics.py`
- `scripts/aggregate_benchmark_runs.py`
- `automation/phase4_full_benchmarks.sh`

## What the runner records

Each run captures:

- benchmark name (`--benchmark-name`)
- full `spark-submit` command
- query parameters
- raw input size snapshot (`--raw-input`, HDFS `dfs -count`)
- processed input size snapshot (`--input`, HDFS `dfs -count`)
- per-iteration duration and status

## Run one benchmark batch

Default benchmark query is `stats --year 2018 --group-by month --metrics speed --order-by month --limit 12`.

```bash
python3 scripts/benchmark_ais_analytics.py \
  --benchmark-name ais_stats_monthly_speed_1gb \
  --modes both \
  --repeats 5 \
  --raw-input /bds/proj1/raw \
  --input hdfs://namenode:9000/bds/proj1/processed/clean
```

Use a different analytics command with `--query-override`:

```bash
python3 scripts/benchmark_ais_analytics.py \
  --benchmark-name ais_count_dec2018_1gb \
  --modes both \
  --repeats 5 \
  --query-override "count --year 2018 --month 12"
```

```bash
python3 scripts/benchmark_ais_analytics.py \
  --benchmark-name ais_show_fast_jul2018_1gb \
  --modes both \
  --repeats 5 \
  --query-override "show --year 2018 --month 7 --min-speed 15 --sort-by timestamp --sort-desc --limit 20"
```

## Run artifact layout

Run directories:

- `runs/<campaign_or_default>/runs/standalone/<timestamp>/`
- `runs/<campaign_or_default>/runs/cluster/<timestamp>/`

Each run directory contains:

- `meta.json`
- `results.json`
- `summary.json`
- `run_report.md`
- `iter_XX.stdout.log`
- `iter_XX.stderr.log`

## Aggregate reports

Generate report from all runs in a directory:

```bash
python3 scripts/aggregate_benchmark_runs.py \
  --runs-dir runs/<campaign>/runs \
  --output-root runs/<campaign>/reports
```

Generate latest-only view (one row per mode):

```bash
python3 scripts/aggregate_benchmark_runs.py \
  --runs-dir runs/<campaign>/runs \
  --output-root runs/<campaign>/reports \
  --latest-only
```

Aggregate outputs:

- `aggregate.json`
- `aggregate.csv`
- `aggregate.md`

`aggregate.md` structure:

- included runs
- included run details
- summary section at the end (grouped by benchmark and mode)

## Campaign pattern (5 reports + final summary)

Recommended reproducible pattern:

1. run benchmark with `--repeats 1` for iteration 1, then generate `--latest-only` report
2. repeat for iterations 2..5
3. run one final aggregate (without `--latest-only`) for 5x2 summary

This keeps one report per iteration plus one final report for reasoning.

## Full benchmark checkpoint

Run full storage+compute+app rebuild and all 4 benchmark campaigns
(`stats speed`, `stats heading/course`, `count`, `show`):

```bash
automation/phase4_full_benchmarks.sh
```

Default campaign iterations: `5` (env override: `ITERATIONS=<n>`).

## Presentation generation (LaTeX Beamer)

Canonical source:

- `docs/presentation.tex`

Generate PDF in the repository root:

```bash
lualatex -interaction=nonstopmode -halt-on-error -output-directory . docs/presentation.tex
lualatex -interaction=nonstopmode -halt-on-error -output-directory . docs/presentation.tex
```
