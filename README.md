# bds-proj-1

## Skup podataka

- Izabrani dataset: [The Piraeus AIS Dataset for Large-scale Maritime Data Analytics](https://zenodo.org/records/5792100#.Yd63IFko9PY)
- Naučni rad: [The Piraeus AIS dataset for large-scale maritime data analytics](https://www.sciencedirect.com/science/article/pii/S2352340921010568)

## Strategija rada

Detaljan plan je u `docs/strategy.md`.

Prva iteracija pokriva:

- preprocesiranje podataka u partitioned Parquet,
- Spark batch aplikaciju sa:
  - filtriranjem/sortiranjem/prebrojavanjem/prikazom,
  - grupisanim statistikama (min/max/avg/stddev).

## Struktura

- `scripts/preprocess_ais.py` - preprocess CSV -> Parquet
- `scripts/spark_ais_batch.py` - Spark batch analytics CLI

## Instalacija

```bash
poetry config virtualenvs.in-project true
poetry env use python3
poetry install
```

## Formatiranje koda

Podesavanja su u `pyproject.toml`.

```bash
poetry run black .
poetry run isort .
poetry run ruff check . --fix
```

## Preprocesiranje (~1GB ulaz)

Predlog ulaznog skupa (oko 972MB):

- `../dataset/5792100/unipi_ais_dynamic_2018/unipi_ais_dynamic_jul2018.csv`
- `../dataset/5792100/unipi_ais_dynamic_2018/unipi_ais_dynamic_dec2018.csv`

Komanda:

```bash
poetry run python scripts/preprocess_ais.py \
  --input \
    ../dataset/5792100/unipi_ais_dynamic_2018/unipi_ais_dynamic_jul2018.csv \
    ../dataset/5792100/unipi_ais_dynamic_2018/unipi_ais_dynamic_dec2018.csv \
  --static-path ../dataset/5792100/ais_static/ais_static/unipi_ais_static.csv \
  --output data/processed/ais_2018_jul_dec \
  --drop-null-geo \
  --partitions 32
```

## Upload na HDFS

Primer (prilagodi `HDFS_URI` i putanje):

```bash
hdfs dfs -mkdir -p /projects/bds/proj1/ais_2018_jul_dec
hdfs dfs -put -f data/processed/ais_2018_jul_dec/* /projects/bds/proj1/ais_2018_jul_dec/
```

## Spark aplikacija

### 1) Filtriranje/sortiranje/prebrojavanje/prikaz

Primer brojanja svih zapisa u periodu:

```bash
poetry run python scripts/spark_ais_batch.py query \
  --input data/processed/ais_2018_jul_dec \
  --input-format parquet \
  --from-ts 1530403200000 \
  --to-ts 1546214399000 \
  --count-only
```

Primer prikaza najbrzih zapisa:

```bash
poetry run python scripts/spark_ais_batch.py query \
  --input data/processed/ais_2018_jul_dec \
  --input-format parquet \
  --min-speed 10 \
  --select vessel_id,event_ts,lon,lat,speed,course,country,shiptype \
  --sort-by speed \
  --sort-desc \
  --limit 20
```

### 2) Grupisane statistike

Primer statistike brzine po tipu broda i mesecu:

```bash
poetry run python scripts/spark_ais_batch.py stats \
  --input data/processed/ais_2018_jul_dec \
  --input-format parquet \
  --group-by shiptype,year,month \
  --metrics speed \
  --order-by records \
  --order-desc \
  --limit 50
```

## Napomena za cluster pokretanje

Skripte su kompatibilne sa `spark-submit`. Primer:

```bash
spark-submit --master spark://<spark-master>:7077 scripts/spark_ais_batch.py query --input hdfs:///projects/bds/proj1/ais_2018_jul_dec --input-format parquet --count-only
```

## Sledece iteracije

- standalone Python aplikacija (bez Spark-a),
- benchmark i analiza performansi standalone vs Spark lokalno vs Spark cluster,
- priprema finalne prezentacije.
