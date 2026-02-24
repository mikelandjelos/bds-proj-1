# Phase 4 (step 1): Analytics CLI reference

This document is the full reference for the current analytics CLI.

Script:

- `scripts/ais_analytics.py`

Default input:

- `hdfs://namenode:9000/bds/proj1/processed/clean`

Current scope:

- `count` command
- `show` command
- filtering support for both commands
- optional sorting and projection for `show`

Not in this step:

- grouped statistics (`min/max/mean/stddev by group`)

## Prerequisites

Stack running:

```bash
docker compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2 app
```

Base invocation pattern:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py <GLOBAL_ARGS> <COMMAND> <COMMAND_ARGS>
```

## Command model

CLI structure:

```text
ais_analytics.py [global options] {count|show} [command options]
```

Commands:

- `count`: prints one integer (row count after filters)
- `show`: displays rows after filters

## Global options

| Option | Type | Default | Meaning |
|---|---|---|---|
| `--app-name` | string | `bds-proj1-analytics-step1` | Spark application name in Spark UI |
| `--input` | string | `hdfs://namenode:9000/bds/proj1/processed/clean` | Input parquet path |
| `--master` | string | unset | Optional Spark master override (normally use `spark-submit --master`) |
| `--shuffle-partitions` | int | `200` | Spark SQL shuffle partitions |

## Shared filter options (`count` and `show`)

| Option | Type | Meaning |
|---|---|---|
| `--vessel-id` | string | exact match on `vessel_id` |
| `--from-ts` | int | `timestamp >= from-ts` (epoch milliseconds) |
| `--to-ts` | int | `timestamp <= to-ts` (epoch milliseconds) |
| `--year` | int | match partition/value `year` |
| `--month` | int | match partition/value `month` |
| `--min-speed` | float | `speed >= min-speed` |
| `--max-speed` | float | `speed <= max-speed` |
| `--where` | string (repeatable) | extra Spark SQL expression filter |

Notes:

- multiple `--where` flags are combined with logical `AND` (because they are applied sequentially)
- all active filters are combined with logical `AND`

## `count` command

Syntax:

```text
ais_analytics.py [global options] count [shared filter options]
```

Output:

- single integer line (count after all filters)

Example:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 12
```

## `show` command

Syntax:

```text
ais_analytics.py [global options] show [shared filter options] [show options]
```

Show-specific options:

| Option | Type | Default | Meaning |
|---|---|---|---|
| `--select` | CSV string or `*` | `timestamp,vessel_id,lon,lat,speed,year,month` | output columns |
| `--sort-by` | string | unset | sort column |
| `--sort-desc` | flag | false | descending sort |
| `--limit` | int | `20` | number of rows to display |

Behavior:

- sorting is applied before projection (`--select`), so sorting can use columns not shown in output
- output is printed with `truncate=False`

Example:

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

## Copy-paste query examples

Count all rows:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count
```

Count December 2018:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 12
```

Show July rows with speed threshold:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --year 2018 \
  --month 7 \
  --min-speed 15 \
  --limit 10
```

Show selected columns, sorted by timestamp descending:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --where "lon > 23.6" \
  --where "lat > 37.7" \
  --sort-by timestamp \
  --sort-desc \
  --select "vessel_id,speed,lon,lat" \
  --limit 10
```

## Query cookbook (assignment-style examples)

All commands below are copy-paste ready.

1. Count all July 2018 records:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 7
```

2. Show latest high-speed records in July (speed >= 20):

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --year 2018 \
  --month 7 \
  --min-speed 20 \
  --sort-by timestamp \
  --sort-desc \
  --limit 20
```

3. Count records in a geographic window:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --where "lon BETWEEN 23.6 AND 23.9" \
  --where "lat BETWEEN 37.7 AND 37.95"
```

4. Show records for one vessel in December:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --year 2018 \
  --month 12 \
  --vessel-id 1d2a32243707b28cb49a7c7806585b884d7f84b30c1a248eda6636f67780f0a4 \
  --sort-by timestamp \
  --limit 20
```

5. Show only selected columns for map-like inspection:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --year 2018 \
  --month 7 \
  --select "timestamp,vessel_id,lon,lat,speed" \
  --limit 15
```

6. Count low-speed traffic (possible anchoring/slow movement):

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 12 \
  --max-speed 1
```

7. Count medium-speed traffic band:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 7 \
  --min-speed 5 \
  --max-speed 15
```

8. Show records between two timestamps (epoch ms):

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --from-ts 1530403200000 \
  --to-ts 1530489599000 \
  --sort-by timestamp \
  --limit 20
```

9. Show a custom condition using SQL expressions:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --where "speed >= 12" \
  --where "course >= 45 AND course <= 135" \
  --select "timestamp,vessel_id,speed,course,lon,lat" \
  --limit 20
```

10. Explore newest events while displaying fewer columns:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --sort-by timestamp \
  --sort-desc \
  --select "timestamp,vessel_id,speed" \
  --limit 25
```

## Query cookbook (report-oriented set)

Use these labels directly in your report text.

Query A. Baseline volume on processed dataset
Purpose: establish total record count on clean data.

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count
```

Query B. Monthly traffic slice (December 2018)
Purpose: compare seasonal/period traffic volume.

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 12
```

Query C. High-speed events in a month
Purpose: estimate aggressive/fast movement activity.

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 7 \
  --min-speed 20
```

Query D. Low-speed events in a month
Purpose: approximate slow/anchoring-like behavior.

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --year 2018 \
  --month 12 \
  --max-speed 1
```

Query E. Geographic window traffic
Purpose: inspect activity around a bounded lon/lat zone.

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  count \
  --where "lon BETWEEN 23.6 AND 23.9" \
  --where "lat BETWEEN 37.7 AND 37.95"
```

Query F. Vessel track sample
Purpose: inspect one vessel's records over a period.

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --year 2018 \
  --month 12 \
  --vessel-id 1d2a32243707b28cb49a7c7806585b884d7f84b30c1a248eda6636f67780f0a4 \
  --sort-by timestamp \
  --limit 30
```

Query G. Time-window event extraction
Purpose: isolate records in a strict timestamp interval.

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --from-ts 1530403200000 \
  --to-ts 1530489599000 \
  --sort-by timestamp \
  --limit 25
```

Query H. Reproducible sample table for appendix
Purpose: produce a compact, human-readable sample output table.

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py \
  show \
  --year 2018 \
  --month 7 \
  --sort-by timestamp \
  --sort-desc \
  --select "timestamp,vessel_id,speed,lon,lat" \
  --limit 20
```

## Error behavior and troubleshooting

Unknown or missing columns:

- the script raises `ValueError: Missing columns in input dataset: ...`
- fix by correcting `--select`, `--sort-by`, `--where`, or by pointing `--input` to the expected dataset

No rows returned:

- command succeeds; output count is `0` or `show` prints only header

HDFS path issues:

- verify input exists:

```bash
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/processed/clean
```

Spark submit issues:

- verify app can submit and cluster is up:

```bash
docker compose ps
```

## Help commands

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py --help
```

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py count --help
```

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/ais_analytics.py show --help
```
