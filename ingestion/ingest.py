import json
import os
from datetime import datetime, timezone

import dlt
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["FOOTBALL_DATA_API_KEY"]
BASE_URL = "https://api.football-data.org/v4"


def extract_competitions() -> str:
    """Fetch from the API and land the raw response. Returns the landing file path."""
    response = requests.get(
        f"{BASE_URL}/competitions",
        headers={"X-Auth-Token": API_KEY},
    )
    response.raise_for_status()

    landing_dir = os.path.join("data", "raw", "competitions")
    os.makedirs(landing_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    file_path = os.path.join(landing_dir, f"{timestamp}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(response.json(), f)

    return file_path


@dlt.resource(name="competitions", write_disposition="replace")
def load_competitions_bronze(file_path: str):
    """Load a landed raw file into the bronze table. Never calls the API."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    yield data["competitions"]


if __name__ == "__main__":
    landing_file = extract_competitions()
    print(f"Landed raw response at {landing_file}")

    pipeline = dlt.pipeline(
        pipeline_name="football_lakehouse",
        destination="duckdb",
        dataset_name="bronze",
    )
    load_info = pipeline.run(load_competitions_bronze(landing_file))
    print(load_info)
