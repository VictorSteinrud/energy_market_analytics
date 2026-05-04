from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


EDS_BASE_URL = "https://api.energidataservice.dk/dataset"


@dataclass(frozen=True)
class DatasetSpec:
    source_name: str
    dataset_name: str
    params: dict[str, Any]


def get_repo_root() -> Path:
    """
    Expected file location:
    repo_root/src/ema/ingestion/fetch_energy_market_raw.py

    parents[0] = ingestion
    parents[1] = ema
    parents[2] = src
    parents[3] = repo root
    """
    return Path(__file__).resolve().parents[3]


def build_url(dataset_name: str, params: dict[str, Any]) -> str:
    query = urlencode(params, quote_via=quote)
    return f"{EDS_BASE_URL}/{dataset_name}?{query}"


def fetch_json(url: str, timeout_seconds: int = 30) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "ema-energy-market-analytics/0.1",
        },
    )

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            return json.loads(body)

    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP error while fetching {url}\n"
            f"Status: {error.code}\n"
            f"Body: {error_body[:1000]}"
        ) from error

    except URLError as error:
        raise RuntimeError(f"Network error while fetching {url}: {error}") from error

    except json.JSONDecodeError as error:
        raise RuntimeError(f"Could not parse JSON response from {url}") from error


def save_raw_payload(
    raw_dir: Path,
    source_name: str,
    dataset_name: str,
    url: str,
    params: dict[str, Any],
    response_json: dict[str, Any],
) -> Path:
    fetched_at_utc = datetime.now(timezone.utc)
    timestamp = fetched_at_utc.strftime("%Y%m%dT%H%M%SZ")

    output_dir = raw_dir / "energidataservice" / source_name
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{timestamp}_{source_name}.json"

    payload = {
        "source": "Energi Data Service",
        "dataset_name": dataset_name,
        "source_name": source_name,
        "fetched_at_utc": fetched_at_utc.isoformat(),
        "request_url": url,
        "request_params": params,
        "response": response_json,
    }

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    return output_path


def make_dataset_specs(
    price_areas: list[str],
    price_days_back: int,
    price_days_forward: int,
    co2_days_back: int,
) -> list[DatasetSpec]:
    price_area_filter = json.dumps(
        {"PriceArea": price_areas},
        separators=(",", ":"),
    )

    return [
        DatasetSpec(
            source_name="day_ahead_prices",
            dataset_name="DayAheadPrices",
            params={
                "start": f"now-P{price_days_back}D",
                "end": f"now+P{price_days_forward}D",
                "filter": price_area_filter,
                "sort": "TimeUTC asc",
                "limit": 0,
            },
        ),
        DatasetSpec(
            source_name="co2_emissions",
            dataset_name="CO2Emis",
            params={
                "start": f"now-P{co2_days_back}D",
                "end": "now",
                "filter": price_area_filter,
                "sort": "Minutes5UTC asc",
                "limit": 0,
            },
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch raw Danish energy market data into data/raw."
    )

    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=get_repo_root() / "data" / "raw",
        help="Directory where raw payloads should be saved.",
    )

    parser.add_argument(
        "--areas",
        nargs="+",
        default=["DK1", "DK2"],
        help="Price areas to fetch. Default: DK1 DK2.",
    )

    parser.add_argument(
        "--price-days-back",
        type=int,
        default=7,
        help="How many days back to fetch day-ahead prices.",
    )

    parser.add_argument(
        "--price-days-forward",
        type=int,
        default=2,
        help="How many days forward to fetch day-ahead prices.",
    )

    parser.add_argument(
        "--co2-days-back",
        type=int,
        default=2,
        help="How many days back to fetch CO2 emissions.",
    )

    args = parser.parse_args()

    specs = make_dataset_specs(
        price_areas=args.areas,
        price_days_back=args.price_days_back,
        price_days_forward=args.price_days_forward,
        co2_days_back=args.co2_days_back,
    )

    print(f"Saving raw data to: {args.raw_dir}")

    for spec in specs:
        url = build_url(spec.dataset_name, spec.params)

        print(f"\nFetching {spec.source_name}")
        print(f"Dataset: {spec.dataset_name}")

        response_json = fetch_json(url)

        records = response_json.get("records", [])
        if isinstance(records, list):
            print(f"Records fetched: {len(records)}")
        else:
            print("Records fetched: unknown")

        output_path = save_raw_payload(
            raw_dir=args.raw_dir,
            source_name=spec.source_name,
            dataset_name=spec.dataset_name,
            url=url,
            params=spec.params,
            response_json=response_json,
        )

        print(f"Saved: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())