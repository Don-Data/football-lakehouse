# football-lakehouse

A private, from-scratch practice project: build a modern data lakehouse pipeline end-to-end using free football data, as a technical spike ahead of a planned video series on modernizing a legacy data stack (SQL Server/SSIS/SSAS/SSRS/Tableau) toward a modern one (dlt, dbt, Airflow, Databricks).

## Data source

[football-data.org](https://www.football-data.org/) — free tier, 12 competitions, fixtures/results/standings.

## Architecture

Medallion architecture, environment × layer separated by schema naming (`<environment>_<layer>`, e.g. `dev_bronze`, `dev_silver`, `dev_gold`):

- **Landing** (`data/raw/`) — untouched raw JSON responses from the API, one file per fetch. Enables replay without re-hitting the (rate-limited) API.
- **Bronze** (`ingestion/`) — [dlt](https://dlthub.com/) loads landed JSON into DuckDB, structurally flattened only (no business logic).
- **Silver** (`transform/models/staging/`, `transform/models/intermediate/`) — [dbt](https://www.getdbt.com/) models: staging (thin cleanup/renaming) and intermediate (joins, reshaping, derived business logic).
- **Gold** (`transform/models/marts/`) — dbt models: final, business-ready marts (e.g. league standings).

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a `.env` file in the repo root:
```
FOOTBALL_DATA_API_KEY=your_key_here
```

## Running the pipeline

```powershell
python ingestion/ingest.py competitions
python ingestion/ingest.py matches backfill 2025-08-01 2026-05-31

cd transform
dbt run
dbt test
```
