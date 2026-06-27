# bds-proj-1

- Student: `Mihajlo Madic 2119`
- Prezentacija:
  - PDF - [presentation.pdf](./docs/presentation/presentation.pdf);
  - Izvorni kod prezentacije (TeX) - [presentation.tex](./docs/presentation/presentation.tex).

Sadržaj projekta:

- Docker-first 3-slojna arhitektura: HDFS + Spark + App.
- Upload sirovih AIS podataka na HDFS i verifikacija.
- Preprocesiranje u Parquet (`clean`, `quarantine`, `quality_report`) na HDFS.
- Spark analytics CLI (`count`, `show`, `stats` sa `min/max/avg/stddev`).
- Benchmark kampanje za `local[*]` i cluster mode + agregacija rezultata.
- Reproducibility checkpoint skripte: `phase1_hdfs_upload.sh`, `phase2_preprocessing.sh`, `phase3_analytics_cli.sh`, `phase4_full_benchmarks.sh`.
