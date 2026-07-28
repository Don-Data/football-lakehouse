import os
import sys
from datetime import datetime

from airflow.decorators import dag, task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

PROJECT_ROOT = "/usr/local/airflow/include/project"
DBT_PROJECT_PATH = f"{PROJECT_ROOT}/transform"
PROFILES_YML_PATH = "/usr/local/airflow/dbt_profiles/profiles.yml"

profile_config = ProfileConfig(
    profile_name="transform",
    target_name="dev",
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
    start_date=datetime(2025, 8, 1),
    schedule="@daily",
    catchup=False,
    max_active_tasks=1,
)
def football_lakehouse():

    @task
    def ingest_competitions():
        ingest = _load_ingest_module()
        landing_file = ingest.extract_competitions()
        ingest.run_resource(ingest.load_competitions_bronze, landing_file)

    @task
    def ingest_matches(data_interval_start=None, data_interval_end=None):
        ingest = _load_ingest_module()
        landing_file = ingest.extract_matches(
            data_interval_start.date().isoformat(),
            data_interval_end.date().isoformat(),
        )
        ingest.run_resource(ingest.load_matches_bronze, landing_file)

    transform_dbt = DbtTaskGroup(
        group_id="transform_dbt",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
    )

    [ingest_competitions(), ingest_matches()] >> transform_dbt


football_lakehouse()
