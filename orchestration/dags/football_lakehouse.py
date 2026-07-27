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


@dag(
    dag_id="football_lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger only, for now
    catchup=False,
    max_active_tasks=1,
)
def football_lakehouse():

    @task
    def ingest_competitions():
        os.chdir(PROJECT_ROOT)
        sys.path.insert(0, f"{PROJECT_ROOT}/ingestion")
        from dotenv import load_dotenv
        load_dotenv(f"{PROJECT_ROOT}/.env")

        import ingest

        landing_file = ingest.extract_competitions()
        pipeline = ingest.dlt.pipeline(
            pipeline_name="football_lakehouse",
            destination="duckdb",
            dataset_name=f"{ingest.ENVIRONMENT}_bronze",
        )
        load_info = pipeline.run(ingest.load_competitions_bronze(landing_file))
        print(load_info)

    @task
    def ingest_matches():
        os.chdir(PROJECT_ROOT)
        sys.path.insert(0, f"{PROJECT_ROOT}/ingestion")
        from dotenv import load_dotenv
        load_dotenv(f"{PROJECT_ROOT}/.env")

        import ingest

        # Small recent window for now, not a full historical backfill.
        landing_file = ingest.extract_matches("2026-05-22", "2026-05-31")
        pipeline = ingest.dlt.pipeline(
            pipeline_name="football_lakehouse",
            destination="duckdb",
            dataset_name=f"{ingest.ENVIRONMENT}_bronze",
        )
        load_info = pipeline.run(ingest.load_matches_bronze(landing_file))
        print(load_info)

    transform_dbt = DbtTaskGroup(
        group_id="transform_dbt",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
    )

    [ingest_competitions(), ingest_matches()] >> transform_dbt


football_lakehouse()
