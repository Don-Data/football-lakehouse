import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

PROJECT_ROOT = "/usr/local/airflow/include/project"
DBT_PROJECT_PATH = f"{PROJECT_ROOT}/transform"
PROFILES_YML_PATH = "/usr/local/airflow/dbt_profiles/profiles.yml"
ENVIRONMENT = "prod"  # the scheduled pipeline runs against prod (Databricks);
                      # dev (DuckDB) is for manual/on-demand local testing only

profile_config = ProfileConfig(
    profile_name="transform",
    target_name=ENVIRONMENT,
    profiles_yml_filepath=PROFILES_YML_PATH,
)


def _load_ingest_module():
    """Shared bootstrap: make ingestion/ingest.py importable and load its .env."""
    os.chdir(PROJECT_ROOT)
    sys.path.insert(0, f"{PROJECT_ROOT}/ingestion")
    from dotenv import load_dotenv
    load_dotenv(f"{PROJECT_ROOT}/.env")
    import ingest
    return ingest


@dag(
    dag_id="football_lakehouse",
    # Deliberately NOT 2025-08-01 (the season start) - that historical range
    # was already loaded via ingest.load_matches() run manually for a wide
    # date range. This start_date only needs to cover when real @daily
    # scheduling began, so
    # catchup below only ever protects against a few days of Airflow
    # downtime, not an accidental ~365-day replay through the slow
    # per-day incremental path.
    start_date=datetime(2026, 7, 29),
    schedule="@daily",
    catchup=True,  # backfill any day missed while Airflow was down, rather
                   # than silently skipping straight to "now" - avoids
                   # holes in the timeline from downtime
    max_active_tasks=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)
def football_lakehouse():

    @task
    def ingest_competitions():
        ingest = _load_ingest_module()
        landing_file = ingest.extract_competitions(ENVIRONMENT)
        ingest.run_resource(ingest.load_competitions_bronze, landing_file, ENVIRONMENT)

    @task
    def ingest_matches(data_interval_start=None, data_interval_end=None):
        ingest = _load_ingest_module()

        if data_interval_start is None or data_interval_end is None:
            # Manual trigger via CLI/UI without an explicit run config doesn't
            # get a computed data_interval. Fall back to "yesterday" rather
            # than crash, since that's what an ad-hoc manual run most likely wants.
            yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
            date_from = yesterday.isoformat()
            date_to = (yesterday + timedelta(days=1)).isoformat()
            print(f"No data_interval on this run (manual trigger) - defaulting to yesterday: {date_from} to {date_to}")
        else:
            date_from = data_interval_start.date().isoformat()
            date_to = data_interval_end.date().isoformat()
            print(f"Using data_interval from this run: {date_from} to {date_to}")

        ingest.load_matches(date_from, date_to, ENVIRONMENT)

    transform_dbt = DbtTaskGroup(
        group_id="transform_dbt",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
    )

    [ingest_competitions(), ingest_matches()] >> transform_dbt


football_lakehouse()
