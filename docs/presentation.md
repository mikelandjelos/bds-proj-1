---
marp: true
theme: default
paginate: true
size: 16:9
title: BDS Projekat 1
---

# BDS Projekat 1
## AIS analitika nad HDFS + Spark

- Kurs: Big Data Systems
- Dataset: Piraeus AIS (Zenodo 5792100)
- Autor: Mihajlo (projekat)

---

# Zadatak

Implementirati end-to-end tok:

1. priprema podataka i smeštaj na HDFS
2. Spark aplikacija sa CLI upitima (`count`, `show`, `stats`)
3. benchmark poređenje režima izvršavanja

Traženi artefakti:

- reproducibilna infrastruktura (Docker Compose)
- merenja performansi
- izveštaji sa rezultatima

---

# Obim i podaci

Korišćen podskup:

- raw ulaz: `~971.54 MiB` (jul+dec 2018 CSV)
- preprocesirano: `~191.60 MiB` (Parquet)

Relevantne HDFS putanje:

```bash
/bds/proj1/raw
/bds/proj1/processed/clean
/bds/proj1/processed/quarantine
/bds/proj1/processed/quality_report
```

---

# Arhitektura

3-slojni model:

1. HDFS sloj: skladište (`namenode`, `datanode`)
2. Spark sloj: obrada (`spark-master`, `spark-worker-*`)
3. App sloj: submission i CLI (`app`)

Tok:

```text
Raw CSV -> HDFS (/raw) -> preprocess job -> HDFS (/processed/*)
                                    |
                                    +-> analytics CLI (count/show/stats)
                                    +-> benchmark kampanje i izveštaji
```

---

# Docker Compose sloj

Servisi:

```yaml
namenode, datanode
spark-master, spark-worker-1, spark-worker-2
app
```

Princip:

- isti HDFS izvor za oba benchmark moda
- `app` container submituje iste komande za `local[*]` i `spark://...`

---

# Ingest na HDFS

Reproducibilni checkpoint:

```bash
automation/phase1_hdfs_upload.sh
```

Ključne komande:

```bash
docker compose exec namenode hdfs dfs -mkdir -p /bds/proj1/raw
docker compose exec namenode hdfs dfs -put -f /input/.../jul2018.csv /bds/proj1/raw/jul2018.csv
docker compose exec namenode hdfs dfs -put -f /input/.../dec2018.csv /bds/proj1/raw/dec2018.csv
docker compose exec namenode hdfs dfs -count -h /bds/proj1/raw
```

---

# Preprocessing (Spark job)

Reproducibilni checkpoint:

```bash
automation/phase2_preprocessing.sh
```

Job:

```bash
docker compose exec app /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/scripts/preprocess_ais_hdfs.py \
  --input 'hdfs://namenode:9000/bds/proj1/raw/*.csv' \
  --output-base hdfs://namenode:9000/bds/proj1/processed
```

---

# Preprocessing logika

Ključne transformacije:

```python
# quality flags
missing_required = timestamp/vessel_id/lon/lat missing
invalid_geo = lon/lat out of range
invalid_speed = speed < 0
invalid_heading = heading not in [0, 360]
invalid_course = course not in [0, 360]

# routing
clean       <- no hard_fail
quarantine  <- hard_fail rows

# output
clean:       parquet partitioned by year, month
quarantine:  parquet partitioned by reason
quality_report: summary + reason_counts
```

---

# Analytics CLI

Komande:

- `count`
- `show`
- `stats` (min/max/avg/stddev + count)

Primeri:

```bash
# count
ais_analytics.py count --year 2018 --month 12

# show
ais_analytics.py show --year 2018 --month 7 --min-speed 15 \
  --sort-by timestamp --sort-desc --limit 20

# stats
ais_analytics.py stats --year 2018 --group-by month --metrics speed
```

---

# Benchmark metodologija

Pravila za fer poređenje:

- isti dataset (`/processed/clean`)
- ista komanda
- oba moda:
  - `--master local[*]`
  - `--master spark://spark-master:7077`

Kampanja:

- 5 iteracija
- svaka iteracija: 1 run po modu + poseban report
- zatim finalni agregat (5x2)

---

# Benchmark alati i artefakti

Runner:

```bash
python3 scripts/benchmark_ais_analytics.py \
  --benchmark-name <name> \
  --modes both \
  --repeats 1 \
  --query-override "count ... | show ... | stats ..."
```

Aggregator:

```bash
python3 scripts/aggregate_benchmark_runs.py \
  --runs-dir runs/campaign_<id>/runs \
  --output-root runs/campaign_<id>/reports
```

---

# Full checkpoint (jedna komanda)

Finalni reproducibilni tok:

```bash
automation/phase5_full_benchmarks.sh
```

Šta radi:

1. podiže storage + compute + app sloj
2. puni HDFS i izvršava preprocessing
3. pokreće 4 benchmark kampanje (`stats speed`, `stats heading/course`, `count`, `show`)
4. za svaku kampanju pravi 5 iteracionih reporta + 1 finalni agregat

---

# Struktura rezultata

```text
runs/campaign_<id>/
  runs/
    standalone/<timestamp>/
      meta.json
      results.json
      summary.json
      run_report.md
    cluster/<timestamp>/
      ...
  reports/
    <timestamp>/aggregate.md
    <timestamp>/aggregate.csv
    <timestamp>/aggregate.json
```

`meta.json` čuva i raw i processed size snapshot.

---

# Rezultati: Stats (speed)

Kampanja: `campaign_20260226T101745Z_stats_speed`

Upit:

```text
stats --year 2018 --group-by month --metrics speed --order-by month --limit 12
```

| mode | n | mean s | stddev s | min s | max s |
|---|---:|---:|---:|---:|---:|
| standalone | 5 | 11.049 | 0.576 | 10.350 | 11.781 |
| cluster | 5 | 15.444 | 0.458 | 15.023 | 16.202 |

Raw: `971.54 MiB`, Processed: `191.60 MiB`

---

# Rezultati: Stats (heading,course)

Kampanja: `campaign_20260226T102011Z_stats_heading_course`

Upit:

```text
stats --year 2018 --group-by month --metrics heading,course --order-by month --limit 12
```

| mode | n | mean s | stddev s | min s | max s |
|---|---:|---:|---:|---:|---:|
| standalone | 5 | 11.132 | 0.498 | 10.760 | 11.825 |
| cluster | 5 | 16.069 | 0.611 | 15.484 | 16.734 |

Raw: `971.54 MiB`, Processed: `191.60 MiB`

---

# Rezultati: Count

Kampanja: `campaign_20260226T102241Z_count`

Upit:

```text
count --year 2018 --month 12
```

| mode | n | mean s | stddev s | min s | max s |
|---|---:|---:|---:|---:|---:|
| standalone | 5 | 9.486 | 0.018 | 9.465 | 9.508 |
| cluster | 5 | 14.065 | 0.231 | 13.657 | 14.188 |

---

# Rezultati: Show

Kampanja: `campaign_20260226T102453Z_show`

Upit:

```text
show --year 2018 --month 7 --min-speed 15 --sort-by timestamp --sort-desc --limit 20
```

| mode | n | mean s | stddev s | min s | max s |
|---|---:|---:|---:|---:|---:|
| standalone | 5 | 10.490 | 0.031 | 10.445 | 10.513 |
| cluster | 5 | 14.309 | 0.220 | 14.167 | 14.697 |

---

# Poređenje cluster vs standalone

Cluster/standalone odnos (mean):

| benchmark | ratio | delta (s) |
|---|---:|---:|
| stats speed | 1.397x | +4.395 |
| stats heading,course | 1.443x | +4.937 |
| count dec2018 | 1.483x | +4.579 |
| show fast jul2018 | 1.364x | +3.819 |

---

# Analiza rezultata

Zaključci za trenutni obim (~1GB raw, ~192MB parquet):

- `local[*]` je brži u svim testovima.
- Cluster overhead (driver/executor scheduling + network + serialization) dominira nad dobitkom paralelizacije.
- Stabilnost merenja zavisi od upita: `count` i `show` su stabilniji, dok `stats` ima veću varijansu.

Implikacija:

- Za dokaz da cluster postaje bolji, potrebno je povećati ulaz i/ili težinu transformacija.

---

# Ograničenja i sledeći koraci

Ograničenja trenutnog eksperimenta:

- jedan data scale (~1GB raw)
- jedan host i ograničeni worker resursi
- fokus na latency, ne na throughput pod konkurentnim opterećenjem

Sledeće:

1. benchmark na većim scale-ovima (npr. 5GB+ raw)
2. tuning Spark parametara (shuffle partitions, executor memory/cores)
3. finalni uporedni grafici iz `aggregate.csv`

---

# Reproducibilnost

Reference komande:

```bash
# HDFS + preprocess baseline
bash automation/phase2_preprocessing.sh

# jedna benchmark kampanja
python3 scripts/benchmark_ais_analytics.py ...
python3 scripts/aggregate_benchmark_runs.py ...
```

Sve kampanje i izveštaji su timestamped i append-only.

---

# Pitanja?

## Kraj
