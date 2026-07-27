import json
import os
import sys
import time
from datetime import datetime, timezone, date, timedelta
import yaml
import dlt
import requests
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.environ["FOOTBALL_DATA_API_KEY"]
BASE_URL = "https://api.football-data.org/v4"
ENVIRONMENT = "dev"

with open(os.path.join(os.path.dirname(__file__), "tracked_competitions.yml")) as f:
    TRACKED_COMPETITION_IDS = yaml.safe_load(f)["competition_ids"]

def backfill_matches(date_from: str, date_to: str) -> None:
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)

    pipeline = dlt.pipeline(
        pipeline_name="football_lakehouse",
        destination="duckdb",
        dataset_name=f"{ENVIRONMENT}_bronze",
    )

    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=9), end)  # last day we want included
        api_date_to = chunk_end + timedelta(days=1)  # API excludes this day, so shift by one
        print(f"Fetching {chunk_start} to {chunk_end} (dateTo sent as {api_date_to})...")
        landing_file = extract_matches(chunk_start.isoformat(), api_date_to.isoformat())
        load_info = pipeline.run(load_matches_bronze(landing_file))
        print(load_info)
        chunk_start = chunk_end + timedelta(days=1)
        if chunk_start <= end:
            time.sleep(7)  # stay under 10 req/min

def _land_raw(resource_name: str, payload: dict) -> str:
    landing_dir = os.path.join("data", "raw", resource_name)
    os.makedirs(landing_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    file_path = os.path.join(landing_dir, f"{timestamp}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    return file_path


def _fetch_competitions() -> dict:
    response = requests.get(
        f"{BASE_URL}/competitions",
        headers={"X-Auth-Token": API_KEY},
    )
    response.raise_for_status()
    return response.json()


def check_for_new_competitions(competitions_payload: dict) -> None:
    """Fail loudly if the API has competitions we're not tracking yet."""
    live_ids = {c["id"] for c in competitions_payload["competitions"]}
    untracked = live_ids - set(TRACKED_COMPETITION_IDS)
    if untracked:
        raise ValueError(
            f"Found competitions not in TRACKED_COMPETITION_IDS: {untracked}. "
            "Update ingestion/tracked_competitions.py deliberately if you want to track them."
        )


def extract_competitions() -> str:
    payload = _fetch_competitions()
    check_for_new_competitions(payload)
    return _land_raw("competitions", payload)


@dlt.resource(name="competitions", write_disposition="replace")
def load_competitions_bronze(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    yield data["competitions"]


def extract_matches(date_from: str, date_to: str) -> str:
    response = requests.get(
        f"{BASE_URL}/matches",
        headers={"X-Auth-Token": API_KEY},
        params={
            "dateFrom": date_from,
            "dateTo": date_to,
            "competitions": ",".join(str(c) for c in TRACKED_COMPETITION_IDS),
        },
    )
    response.raise_for_status()
    return _land_raw("matches", response.json())


@dlt.resource(name="matches", write_disposition="merge", primary_key="id")
def load_matches_bronze(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    yield data["matches"]


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("competitions", "matches"):
        print("Usage:")
        print("  python ingest.py competitions [landing_file_to_replay]")
        print("  python ingest.py matches <date_from> <date_to>")
        print("  python ingest.py matches replay <landing_file_to_replay>")
        sys.exit(1)

    resource_name = sys.argv[1]

    if resource_name == "competitions":
        load_fn = load_competitions_bronze
        if len(sys.argv) > 2:
            landing_file = sys.argv[2]
            print(f"Replaying from existing landing file: {landing_file}")
        else:
            landing_file = extract_competitions()
            print(f"Landed raw response at {landing_file}")

    else:  # matches
        if sys.argv[2] == "replay":
            landing_file = sys.argv[3]
            print(f"Replaying from existing landing file: {landing_file}")
            load_fn = load_matches_bronze
        elif sys.argv[2] == "backfill":
            backfill_matches(sys.argv[3], sys.argv[4])
            sys.exit(0)
        else:
            date_from, date_to = sys.argv[2], sys.argv[3]
            landing_file = extract_matches(date_from, date_to)
            print(f"Landed raw response at {landing_file}")
            load_fn = load_matches_bronze

    pipeline = dlt.pipeline(
        pipeline_name="football_lakehouse",
        destination="duckdb",
        dataset_name=f"{ENVIRONMENT}_bronze",
    )
    load_info = pipeline.run(load_fn(landing_file))
    print(load_info)
