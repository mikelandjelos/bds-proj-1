# Phase 1: HDFS only (atomic step)

This step brings up only HDFS (NameNode + DataNode), then verifies upload/query.
Current prototype runs without Docker volumes, so HDFS data is ephemeral.
Dataset input is mounted into `namenode` as read-only bind mount.

## Fast path (script)

```bash
automation/hdfs_upload.sh
```

This does: start HDFS -> wait for readiness -> upload Jul/Dec 2018 from read-only mount -> verify.
It performs a clean restart first (`docker compose down`).

## 1) Start HDFS

```bash
docker compose up -d namenode datanode
```

Check container status:

```bash
docker compose ps
```

NameNode UI (HDFS web):

- `http://localhost:9870`

## 2) Verify HDFS is reachable

```bash
docker compose exec namenode hdfs dfsadmin -report
```

## 3) Create project folders in HDFS

```bash
docker compose exec namenode hdfs dfs -mkdir -p /bds/proj1/raw
docker compose exec namenode hdfs dfs -mkdir -p /bds/proj1/processed
```

## 4) Upload a small sample first (smoke test)

Create a local sample:

```bash
head -n 10000 ../dataset/5792100/unipi_ais_dynamic_2018/unipi_ais_dynamic_jul2018.csv > /tmp/jul2018_sample.csv
```

Copy into NameNode container and upload to HDFS:

```bash
docker cp /tmp/jul2018_sample.csv bds-namenode:/tmp/jul2018_sample.csv
docker compose exec namenode hdfs dfs -put -f /tmp/jul2018_sample.csv /bds/proj1/raw/jul2018_sample.csv
```

## 5) Query data in HDFS

```bash
docker compose exec namenode hdfs dfs -ls -h /bds/proj1/raw
docker compose exec namenode hdfs dfs -cat /bds/proj1/raw/jul2018_sample.csv | sed -n '1,5p'
docker compose exec namenode hdfs dfs -count -h /bds/proj1/raw
```

## 6) Upload full ~1GB subset

Suggested subset:

- `../dataset/5792100/unipi_ais_dynamic_2018/unipi_ais_dynamic_jul2018.csv` (~423MB)
- `../dataset/5792100/unipi_ais_dynamic_2018/unipi_ais_dynamic_dec2018.csv` (~549MB)

Upload commands:

```bash
docker compose exec namenode hdfs dfs -put -f /input/unipi_ais_dynamic_2018/unipi_ais_dynamic_jul2018.csv /bds/proj1/raw/jul2018.csv
docker compose exec namenode hdfs dfs -put -f /input/unipi_ais_dynamic_2018/unipi_ais_dynamic_dec2018.csv /bds/proj1/raw/dec2018.csv

docker compose exec namenode hdfs dfs -ls -h /bds/proj1/raw
docker compose exec namenode hdfs dfs -count -h /bds/proj1/raw
```

## 7) Stop HDFS

```bash
docker compose down
```

Start again and re-check:

```bash
docker compose up -d namenode datanode
docker compose exec namenode hdfs dfs -ls -R /bds/proj1
```

If `/bds/proj1` is missing after restart, that is expected in this phase.
