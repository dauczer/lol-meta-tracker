"""
Airflow DAG: LoL Meta Tracker daily refresh.

This is the enterprise equivalent of:
  - pipeline/main.py  (orchestration logic)
  - .github/workflows/daily-refresh.yml  (scheduling)

The DAG mirrors the same three-stage flow (ingest → transform → output)
but adds proper dependency management, retries, and observability.

Requirements (not installed in portfolio env):
  apache-airflow>=2.8.0
  apache-airflow-providers-http
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default task arguments applied to all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def ingest(**context) -> None:
    """Stage 1: Fetch raw match data from Riot API."""
    from pipeline.ingest import run_ingestion

    raw_files = run_ingestion(dry_run=False)
    # Push file count to XCom so downstream tasks can log it
    context["ti"].xcom_push(key="raw_file_count", value=len(raw_files))


def transform(**context) -> None:
    """Stage 2: Parse, filter, and aggregate match data."""
    from pipeline import config
    from pipeline.transform import (
        aggregate_stats,
        current_patch,
        filter_valid,
        parse_matches,
        top_champions_per_role,
    )

    df = parse_matches(config.RAW_DIR)
    df = filter_valid(df)

    stats = aggregate_stats(df)
    patch = current_patch(stats)
    top = top_champions_per_role(stats, patch)

    # Pass results to the output task via XCom
    # (In production, persist to a staging table instead)
    context["ti"].xcom_push(key="current_patch", value=patch)
    context["ti"].xcom_push(key="total_matches", value=int(df["match_id"].nunique()))


def write_output(**context) -> None:
    """Stage 3: Write final JSON files to data/output/."""
    from pipeline import config
    from pipeline.output import (
        write_champions_by_role,
        write_meta_summary,
        write_top_champions,
    )
    from pipeline.transform import (
        aggregate_stats,
        current_patch,
        filter_valid,
        parse_matches,
        top_champions_per_role,
    )

    df = filter_valid(parse_matches(config.RAW_DIR))
    total_matches = int(df["match_id"].nunique())
    stats = aggregate_stats(df)
    patch = current_patch(stats)
    top = top_champions_per_role(stats, patch)

    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    write_meta_summary(stats, total_matches, patch, config.OUTPUT_DIR)
    write_top_champions(top, config.OUTPUT_DIR)
    write_champions_by_role(stats, patch, config.OUTPUT_DIR)


with DAG(
    dag_id="lol_meta_tracker",
    description="Daily LoL meta statistics refresh",
    schedule="0 6 * * *",   # 06:00 UTC daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["lol", "portfolio"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    output_task = PythonOperator(
        task_id="write_output",
        python_callable=write_output,
    )

    # Linear dependency chain: ingest → transform → output
    ingest_task >> transform_task >> output_task
