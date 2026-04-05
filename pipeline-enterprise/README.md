# Enterprise Reference Implementation

This folder shows how the LoL Meta Tracker pipeline would be built at production scale.

The portfolio implementation in `pipeline/` makes pragmatic trade-offs for a project running
on free-tier infrastructure. The files here demonstrate the equivalent enterprise architecture
— useful for interviews and for understanding the "why" behind each tool choice.

## File Map

| File | Enterprise tool | Replaces |
|---|---|---|
| `dbt/models/staging/stg_matches.sql` | dbt staging model | `transform.py` parse step |
| `dbt/models/marts/fct_champion_stats.sql` | dbt mart model | `transform.py` aggregation |
| `airflow/dags/meta_tracker_dag.py` | Airflow DAG | `main.py` + GitHub Actions |
| `docker-compose.yml` | Local Airflow + Postgres | — |

## Why These Tools at Scale

**dbt** — When data lives in a SQL warehouse (BigQuery, Snowflake, Redshift), dbt is the
standard transformation layer. It provides: version-controlled SQL models, lineage graphs,
built-in testing (`not_null`, `unique`, custom assertions), and documentation generation.
At millions of matches/day, an in-memory pandas approach would OOM; SQL push-down is essential.

**Airflow** — GitHub Actions is a CI/CD tool pressed into service as a scheduler. At scale
you need: dependency management between tasks, retries with backoff, SLA monitoring,
alerting to PagerDuty/Slack, and visibility into historical runs. Airflow (or Prefect/Dagster)
provides all of this. The DAG here mirrors the ingest → transform → output flow from `main.py`.

**Postgres (as Airflow metadata DB)** — Airflow stores its own state (DAG runs, task instances,
XCom values) in a relational database. The `docker-compose.yml` sets up a local Postgres
instance for development. In production this would be a managed RDS or Cloud SQL instance.
