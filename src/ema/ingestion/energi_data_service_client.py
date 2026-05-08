from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd
import requests

OUTPUT_COLUMNS = [
    "time_utc",
    "time_dk",
    "date_dk",
    "hour_dk",
    "minute_dk",
    "price_area",
    "day_ahead_price_eur",
    "day_ahead_price_dkk",
    "data_status",
    "source_dataset",
    "source",
]


class EnergiDataServiceError(RuntimeError):
    pass


def _format_api_time(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M")


def _build_dataset_url(base_url: str, dataset: str) -> str:
    return f"{base_url.rstrip('/')}/{dataset}"


def fetch_day_ahead_prices(
    start_time_dk: datetime,
    end_time_dk: datetime,
    price_area: str,
    base_url: str,
    dataset: str,
    provider: str,
    timeout_seconds: int,
) -> pd.DataFrame:
    if not price_area:
        raise ValueError("price_area must not be empty")

    if not base_url:
        raise ValueError("base_url must not be empty")

    if not dataset:
        raise ValueError("dataset must not be empty")

    if not provider:
        raise ValueError("provider must not be empty")

    if end_time_dk <= start_time_dk:
        raise ValueError("end_time_dk must be after start_time_dk")

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    url = _build_dataset_url(base_url, dataset)

    params: dict[str, Any] = {
        "start": _format_api_time(start_time_dk),
        "end": _format_api_time(end_time_dk),
        "filter": json.dumps({"PriceArea": [price_area]}),
        "columns": "TimeUTC,TimeDK,PriceArea,DayAheadPriceEUR,DayAheadPriceDKK",
        "sort": "TimeUTC",
        "limit": "0",
    }

    try:
        response = requests.get(url, params=params, timeout=timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise EnergiDataServiceError(f"API request failed: {exc}") from exc

    payload = response.json()
    records = payload.get("records", [])

    if not records:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(records)

    required_columns = {
        "TimeUTC",
        "TimeDK",
        "PriceArea",
        "DayAheadPriceEUR",
        "DayAheadPriceDKK",
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise EnergiDataServiceError(
            f"API response is missing expected columns: {sorted(missing_columns)}"
        )

    df = df.rename(
        columns={
            "TimeUTC": "time_utc",
            "TimeDK": "time_dk",
            "PriceArea": "price_area",
            "DayAheadPriceEUR": "day_ahead_price_eur",
            "DayAheadPriceDKK": "day_ahead_price_dkk",
        }
    )

    df["time_utc"] = pd.to_datetime(df["time_utc"])
    df["time_dk"] = pd.to_datetime(df["time_dk"])

    df["date_dk"] = df["time_dk"].dt.date
    df["hour_dk"] = df["time_dk"].dt.hour
    df["minute_dk"] = df["time_dk"].dt.minute

    df["data_status"] = "observed"
    df["source_dataset"] = dataset
    df["source"] = provider

    return df[OUTPUT_COLUMNS]
