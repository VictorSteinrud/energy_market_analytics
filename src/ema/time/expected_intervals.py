from __future__ import annotations

from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd


def floor_to_interval(dt: datetime, interval_minutes: int) -> datetime:
    """
    Floor a datetime down to the nearest interval boundary.

    Example:
        18:43 with 15-minute intervals becomes 18:30.
    """
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")

    minute = (dt.minute // interval_minutes) * interval_minutes

    return dt.replace(
        minute=minute,
        second=0,
        microsecond=0,
    )


def generate_expected_intervals(
    start_date_dk: date,
    timezone: str,
    interval_minutes: int,
    price_area: str,
    source_dataset: str,
    source: str,
    end_time_dk: datetime | None = None,
) -> pd.DataFrame:
    """
    Generate the expected time intervals for the day-ahead price table.

    The start is 00:00 Danish time on start_date_dk.
    The end is current Danish time floored to the nearest interval,
    unless end_time_dk is provided.

    Returns a DataFrame matching the time columns in fact_day_ahead_prices.
    """

    local_timezone = ZoneInfo(timezone)
    utc_timezone = ZoneInfo("UTC")

    if price_area not in {"DK1", "DK2"}:
        raise ValueError("price_area must be either 'DK1' or 'DK2'")

    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive")

    start_time_dk = datetime(
        year=start_date_dk.year,
        month=start_date_dk.month,
        day=start_date_dk.day,
        hour=0,
        minute=0,
        tzinfo=local_timezone,
    )

    if end_time_dk is None:
        end_time_dk = datetime.now(local_timezone)
    elif end_time_dk.tzinfo is None:
        end_time_dk = end_time_dk.replace(tzinfo=local_timezone)
    else:
        end_time_dk = end_time_dk.astimezone(local_timezone)

    end_time_dk = floor_to_interval(end_time_dk, interval_minutes)

    if end_time_dk < start_time_dk:
        raise ValueError("end_time_dk must be after start_date_dk")

    # Generate in UTC to avoid daylight saving time problems.
    start_time_utc = start_time_dk.astimezone(utc_timezone)
    end_time_utc = end_time_dk.astimezone(utc_timezone)

    rows = []
    current_utc = start_time_utc

    while current_utc <= end_time_utc:
        current_dk = current_utc.astimezone(local_timezone)

        rows.append(
            {
                "time_utc": current_utc.replace(tzinfo=None),
                "time_dk": current_dk.replace(tzinfo=None),
                "date_dk": current_dk.date(),
                "hour_dk": current_dk.hour,
                "minute_dk": current_dk.minute,
                "price_area": price_area,
                "day_ahead_price_eur": None,
                "day_ahead_price_dkk": None,
                "data_status": "missing_not_checked",
                "source_dataset": source_dataset,
                "source": source,
            }
        )

        current_utc += timedelta(minutes=interval_minutes)

    return pd.DataFrame(rows)