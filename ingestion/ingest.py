import argparse
import io
import json
import os
import sys
import time
from datetime import datetime, timezone, date, timedelta

import dlt
import requests
import yaml
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception, wait_exponential, stop_after_attempt

load_dotenv()

API_KEY = os.environ["FOOTBALL_DATA_API_KEY"]
BASE_URL = "https://api.football-data.org/v4"

with open(os.path.join(os.path.dirname(__file__), "tracked_competitions.yml")) as f:
    TRACKED_COMPETITION_IDS = yaml.safe_load(f)["competition_ids"]


def _get_databricks_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(
        host=f"https://{os.environ['DATABRICKS_WORKSPACE_URL']}",
        token=os.environ["DATABRICKS_TOKEN"],
    )


def _create_if_missing(create_fn) -> None:
    from databricks.sdk.errors import DatabricksError

    try:
        create_fn()
    except DatabricksError as e:
        # The SDK doesn't reliably map "already exists" to a specific
        # exception type (sometimes AlreadyExists, sometimes a generic
        # BadRequest) - check the message instead of the exception type.
        if "already exists" not in str(e).lower():
            raise


def _ensure_landing_volume_exists() -> None:
    from databricks.sdk.service.catalog import VolumeType

    client = _get_databricks_client()
    _create_if_missing(lambda: client.schemas.create(name="landing", catalog_name="prod"))
    _create_if_missing(lambda: client.volumes.create(
        catalog_name="prod",
        schema_name="landing",
        name="raw_files",
        volume_type=VolumeType.MANAGED,
    ))


def _get_pipeline(environment: str) -> dlt.Pipeline:
    if environment == "dev":
        return dlt.pipeline(
            pipeline_name="football_lakehouse",
            destination="duckdb",
            dataset_name="dev_bronze",
        )
    elif environment == "prod":
        databricks_destination = dlt.destinations.databricks(
            credentials={
                "server_hostname": os.environ["DATABRICKS_WORKSPACE_URL"],
                "http_path": os.environ["DATABRICKS_HTTP_PATH"],
                "access_token": os.environ["DATABRICKS_TOKEN"],
                "catalog": "prod",
            }
        )
        return dlt.pipeline(
            pipeline_name="football_lakehouse",
            destination=databricks_destination,
            dataset_name="bronze",
        )
    else:
        raise ValueError(f"Unknown environment: {environment}")


def run_resource(load_fn, landing_file: str | list[str], environment: str = "dev"):
    pipeline = _get_pipeline(environment)
    load_info = pipeline.run(load_fn(landing_file))
    print(load_info)
    return load_info


def _is_rate_limited(exception: BaseException) -> bool:
    return (
        isinstance(exception, requests.exceptions.HTTPError)
        and exception.response is not None
        and exception.response.status_code == 429
    )


@retry(
    retry=retry_if_exception(_is_rate_limited),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _get(path: str, params: dict | None = None) -> requests.Response:
    """GET against the football-data.org API. Retries with exponential backoff
    only on 429 (rate limited) - any other error (400, 404, ...) fails immediately,
    since retrying those would just repeat the same mistake."""
    response = requests.get(
        f"{BASE_URL}{path}",
        headers={"X-Auth-Token": API_KEY},
        params=params,
    )
    response.raise_for_status()
    return response


def _land_raw(resource_name: str, payload: dict, environment: str) -> str:
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    metadata = {
        "source": "football-data.org",
        "resource": resource_name,
        "ingested_at": now.isoformat(),
        "record_count": len(payload.get(resource_name, [])),
    }

    if environment == "dev":
        landing_dir = os.path.join("data", "raw", resource_name)
        os.makedirs(landing_dir, exist_ok=True)

        file_path = os.path.join(landing_dir, f"{timestamp}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

        metadata_path = os.path.join(landing_dir, f"{timestamp}.metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f)

        return file_path

    elif environment == "prod":
        _ensure_landing_volume_exists()
        client = _get_databricks_client()

        volume_path = f"/Volumes/prod/landing/raw_files/{resource_name}/{timestamp}.json"
        client.files.upload(volume_path, io.BytesIO(json.dumps(payload).encode("utf-8")), overwrite=True)

        metadata_path = f"/Volumes/prod/landing/raw_files/{resource_name}/{timestamp}.metadata.json"
        client.files.upload(metadata_path, io.BytesIO(json.dumps(metadata).encode("utf-8")), overwrite=True)

        return volume_path

    else:
        raise ValueError(f"Unknown environment: {environment}")


def _read_landed_file(file_path: str) -> dict:
    if file_path.startswith("/Volumes/"):
        client = _get_databricks_client()
        data = client.files.download(file_path).contents.read()
        return json.loads(data)
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)


def _fetch_competitions() -> dict:
    return _get("/competitions").json()


def check_for_new_competitions(competitions_payload: dict) -> None:
    """Fail loudly if the API has competitions we're not tracking yet."""
    live_ids = {c["id"] for c in competitions_payload["competitions"]}
    untracked = live_ids - set(TRACKED_COMPETITION_IDS)
    if untracked:
        raise ValueError(
            f"Found competitions not in TRACKED_COMPETITION_IDS: {untracked}. "
            "Update ingestion/tracked_competitions.py deliberately if you want to track them."
        )


def extract_competitions(environment: str = "dev") -> str:
    payload = _fetch_competitions()
    check_for_new_competitions(payload)
    return _land_raw("competitions", payload, environment)


@dlt.resource(name="competitions", write_disposition="replace")
def load_competitions_bronze(file_path: str):
    data = _read_landed_file(file_path)
    yield data["competitions"]


def extract_matches(date_from: str, date_to: str, environment: str = "dev") -> str:
    response = _get(
        "/matches",
        params={
            "dateFrom": date_from,
            "dateTo": date_to,
            "competitions": ",".join(str(c) for c in TRACKED_COMPETITION_IDS),
        },
    )
    return _land_raw("matches", response.json(), environment)


@dlt.resource(name="matches", write_disposition="merge", primary_key="id")
def load_matches_bronze(file_paths: str | list[str]):
    """Accepts a single landing file (normal case) or a list of them
    (backfill_matches, so many chunks load in one pipeline.run() call
    instead of paying per-load overhead once per chunk)."""
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    for file_path in file_paths:
        data = _read_landed_file(file_path)
        yield data["matches"]


def backfill_matches(date_from: str, date_to: str, environment: str = "dev") -> None:
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)

    # Extraction still has to be chunked (the API's own 10-day window limit)
    # and rate-limited, but loading is deliberately NOT done per-chunk - each
    # dlt load pays a large fixed overhead (schema checks, staging setup,
    # merge statements) regardless of how much data it carries, so loading
    # once at the end instead of once per chunk cuts a ~60-chunk backfill
    # from roughly an hour down to a couple of minutes.
    landing_files = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=9), end)
        api_date_to = chunk_end + timedelta(days=1)
        print(f"Fetching {chunk_start} to {chunk_end} (dateTo sent as {api_date_to})...")
        landing_files.append(extract_matches(chunk_start.isoformat(), api_date_to.isoformat(), environment))
        chunk_start = chunk_end + timedelta(days=1)
        if chunk_start <= end:
            time.sleep(7)  # stay under 10 req/min

    print(f"Landed {len(landing_files)} files, loading all at once...")
    run_resource(load_matches_bronze, landing_files, environment)


if __name__ == "__main__":
    environment = "dev"
    if "--env" in sys.argv:
        idx = sys.argv.index("--env")
        environment = sys.argv[idx + 1]
        del sys.argv[idx:idx + 2]

    if len(sys.argv) < 2 or sys.argv[1] not in ("competitions", "matches"):
        print("Usage:")
        print("  python ingest.py competitions [landing_file_to_replay] [--env dev|prod]")
        print("  python ingest.py matches <date_from> <date_to> [--env dev|prod]")
        print("  python ingest.py matches replay <landing_file_to_replay> [--env dev|prod]")
        print("  python ingest.py matches backfill <date_from> <date_to> [--env dev|prod]")
        sys.exit(1)

    resource_name = sys.argv[1]

    if resource_name == "competitions":
        load_fn = load_competitions_bronze
        if len(sys.argv) > 2:
            landing_file = sys.argv[2]
            print(f"Replaying from existing landing file: {landing_file}")
        else:
            landing_file = extract_competitions(environment)
            print(f"Landed raw response at {landing_file}")

    else:  # matches
        if sys.argv[2] == "replay":
            landing_file = sys.argv[3]
            print(f"Replaying from existing landing file: {landing_file}")
            load_fn = load_matches_bronze
        elif sys.argv[2] == "backfill":
            backfill_matches(sys.argv[3], sys.argv[4], environment)
            sys.exit(0)
        else:
            date_from, date_to = sys.argv[2], sys.argv[3]
            landing_file = extract_matches(date_from, date_to, environment)
            print(f"Landed raw response at {landing_file}")
            load_fn = load_matches_bronze

    run_resource(load_fn, landing_file, environment)
