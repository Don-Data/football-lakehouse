import json
import os
import sys
from datetime import datetime, timezone

import dlt
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["FOOTBALL_DATA_API_KEY"]
BASE_URL = "https://api.football-data.org/v4"
PREMIER_LEAGUE_ID = 2021
ENVIRONMENT = "dev"

def _land_raw(resource_name: str, payload: dict) -> str:
    landing_dir = os.path.join("data", "raw", resource_name)
    os.makedirs(landing_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    file_path = os.path.join(landing_dir, f"{timestamp}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    return file_path


def extract_competitions() -> str:
    response = requests.get(
        f"{BASE_URL}/competitions",
        headers={"X-Auth-Token": API_KEY},
    )
    response.raise_for_status()
    return _land_raw("competitions", response.json())


@dlt.resource(name="competitions", write_disposition="replace")
def load_competitions_bronze(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    yield data["competitions"]


def extract_matches() -> str:
    response = requests.get(
        f"{BASE_URL}/competitions/{PREMIER_LEAGUE_ID}/matches",
        headers={"X-Auth-Token": API_KEY},
        params={"dateFrom": "2025-08-01", "dateTo": "2026-05-31"},
    )
    response.raise_for_status()
    return _land_raw("matches", response.json())



@dlt.resource(name="matches", write_disposition="replace")
def load_matches_bronze(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    yield data["matches"]


RESOURCES = {
    "competitions": (extract_competitions, load_competitions_bronze),
    "matches": (extract_matches, load_matches_bronze),
}


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in RESOURCES:
        print(f"Usage: python ingest.py <{'|'.join(RESOURCES)}> [landing_file_to_replay]")
        sys.exit(1)

    resource_name = sys.argv[1]
    extract_fn, load_fn = RESOURCES[resource_name]

    if len(sys.argv) > 2:
        landing_file = sys.argv[2]
        print(f"Replaying from existing landing file: {landing_file}")
    else:
        landing_file = extract_fn()
        print(f"Landed raw response at {landing_file}")

    pipeline = dlt.pipeline(
        pipeline_name="football_lakehouse",
        destination="duckdb",
        dataset_name=f"{ENVIRONMENT}_bronze",
    )
    load_info = pipeline.run(load_fn(landing_file))
    print(load_info)
