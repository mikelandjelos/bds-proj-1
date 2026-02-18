# Project strategy (atomic execution)

## Goal

Deliver the course project in strict, approved steps:

1. Prepare ~1GB dataset subset
2. Place data on HDFS
3. Build Spark CLI analytics app (argument-driven)
4. Compare execution modes on same HDFS source:
   - `--master local[*]`
   - `--master spark://...` (Docker cluster)

## Working agreement

- Do not continue to the next phase until approved.
- Prototype-first: infrastructure and data path must work before Spark logic.
- Current prototype phase uses no persistent volumes (read-only input bind mount is allowed).

## Phase plan

### Phase 1 (active): HDFS only

Scope:

- bring up HDFS (NameNode + DataNode) with Docker Compose
- verify health
- upload/query sample and target subset files
- verify restart behavior

Definition of done:

- `docker compose ps` healthy
- `hdfs dfsadmin -report` shows live DataNode(s)
- file upload works (`hdfs dfs -put`)
- file query works (`hdfs dfs -ls`, `hdfs dfs -cat`, `hdfs dfs -count`)

### Phase 2 (pending): Dataset preparation for target subset

Scope:

- select initial ~1GB subset (Jul+Dec 2018)
- perform required cleaning/reformatting
- upload final prepared subset to HDFS path used in later phases

### Phase 3 (pending): Spark application

Scope:

- CLI operations: filtering, sorting, counting, display
- grouped statistics: min/max/mean/stddev and similar
- input must be HDFS URI only

### Phase 4 (pending): Performance comparison

Scope:

- same app, same code, same HDFS input
- run A: local mode (`local[*]`)
- run B: Docker Spark cluster mode (`spark://...`)
- repeated runs + side-by-side metrics
