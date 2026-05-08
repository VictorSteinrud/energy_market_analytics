# energy_market_analytics (ema)
This is the repository for my hobby project where im building an analytics engine for energy markets

working in main until the basic scaffolding is setup and im actually working on features

## Current scope

The current pipeline syncs DK1 day-ahead electricity prices into a local DuckDB database.

It:

1. reads `ema.toml`
2. creates the DuckDB schema
3. generates expected 15-minute intervals
4. fetches missing DK1 day-ahead prices
5. updates the database
6. writes a data quality report

## Setup

Create and activate a virtual environment:

```powershell
python -m venv ema_env
.\ema_env\Scripts\Activate.ps1
```

Install the repo in editable mode:

```powershell
python -m pip install -e .
```

This makes the `sync-prices` CLI command available.

You normally do not need to reinstall after editing `.py` files. Reinstall only if `pyproject.toml` changes, for example if dependencies or CLI entry points are changed.

## Run the sync

From the repo root:

```powershell
sync-prices --config ema.toml
```

Example output:

```text
DK1 DayAheadPrices sync completed
--------------------------------
Status:        ok
Expected rows: 266
Observed rows: 266
Missing rows:  0
```

## Config

The sync is configured in `ema.toml`.

Example:

```toml
[database]
path = "data/warehouse/ema.duckdb"

[day_ahead_prices]
start_date_dk = "2026-05-05"
price_area = "DK1"
interval_minutes = 15
timezone = "Europe/Copenhagen"

[energi_data_service]
base_url = "https://api.energidataservice.dk/dataset"
dataset = "DayAheadPrices"
provider = "energidataservice"
timeout_seconds = 30
```


