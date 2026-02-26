# bds-proj-1

Autor: `Mihajlo Madic 2119`

Uradjeno:

- Docker-first 3-slojna arhitektura: HDFS + Spark + App.
- Upload sirovih AIS podataka na HDFS i verifikacija.
- Preprocesiranje u Parquet (`clean`, `quarantine`, `quality_report`) na HDFS.
- Spark analytics CLI (`count`, `show`, `stats` sa `min/max/avg/stddev`).
- Benchmark kampanje za `local[*]` i cluster mode + agregacija rezultata.
- Reproducibility checkpoint skripte: `phase1_hdfs_upload.sh`, `phase2_preprocessing.sh`, `phase4_analytics_cli.sh`, `phase5_full_benchmarks.sh`.

Prezentacija:

- [presentation.pptx](presentation.pptx)
- [docs/presentation.md](docs/presentation.md)

---

Completed:

- Docker-first 3-layer architecture: HDFS + Spark + App.
- Raw AIS upload to HDFS with verification.
- Preprocessing to Parquet (`clean`, `quarantine`, `quality_report`) on HDFS.
- Spark analytics CLI (`count`, `show`, `stats` with `min/max/avg/stddev`).
- Benchmark campaigns for `local[*]` and cluster mode + aggregated reporting.
- Reproducibility checkpoint scripts: `phase1_hdfs_upload.sh`, `phase2_preprocessing.sh`, `phase4_analytics_cli.sh`, `phase5_full_benchmarks.sh`.

Presentation:

- [presentation.pptx](presentation.pptx)
- [docs/presentation.md](docs/presentation.md)
