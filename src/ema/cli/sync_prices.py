from __future__ import annotations

import argparse
from pathlib import Path

from ema.settings import load_config
from ema.ingestion.sync_day_ahead_prices import (
    sync_day_ahead_prices,
    print_sync_result,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync DK day-ahead prices into DuckDB."
    )

    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Path to the EMA TOML config file.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    result = sync_day_ahead_prices(
        start_date_dk=config.day_ahead_prices.start_date_dk,
        db_path=config.database.path,
        price_area=config.day_ahead_prices.price_area,
        interval_minutes=config.day_ahead_prices.interval_minutes,
        timezone=config.day_ahead_prices.timezone,
        api_base_url=config.energi_data_service.base_url,
        dataset=config.energi_data_service.dataset,
        provider=config.energi_data_service.provider,
        timeout_seconds=config.energi_data_service.timeout_seconds,
    )

    print_sync_result(result)


if __name__ == "__main__":
    main()