# AGENTS.md

## Scope

These rules apply only to this repository: `proj-1`.

## Project context

- Course: Big Data Systems project.
- Dataset: Piraeus AIS dataset (Zenodo 5792100).
- Current objective: build the project in atomic, approval-based phases.

## Working mode

- Work in small atomic steps.
- Do not continue to the next phase without explicit user approval.
- Prefer implementing and verifying one concrete deliverable per step.

## Infrastructure rules

- Infrastructure must be Docker-first.
- Prefer `docker compose` orchestration.
- Use `big-data-europe` stack components for Hadoop/Spark infrastructure.
- No persistent volumes unless explicitly approved.
- Read-only bind mounts for input data are allowed.

## Data flow rules

- Phase 1 flow: raw dataset -> HDFS upload -> verification.
- Canonical HDFS paths:
  - `/bds/proj1/raw`
  - `/bds/proj1/processed`
- For fair benchmarking later, both execution modes must read the same data source from HDFS.

## Spark phase rules (when approved)

- Spark app must support CLI-driven:
  - filtering/sorting/counting/display,
  - grouped statistics (min, max, mean, stddev, etc.).
- Keep Spark work separate from infrastructure-only steps.

## Performance comparison rules (when approved)

- Use the same app and same HDFS input for both modes.
- Compare:
  - `--master local[*]`
  - Dockerized cluster mode (`spark://...`).
- Report side-by-side results from repeated runs.

## Documentation rules

- Keep `README.md` short and high-level.
- Put operational details into focused docs under `docs/`.
- Update docs immediately when workflow changes.

## Safety and change control

- Avoid destructive commands unless explicitly requested.
- Do not revert user changes unless requested.
- If unexpected repo changes appear, stop and ask before proceeding.
