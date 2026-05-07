from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd

from ema.storage.schema import create_schema
from ema.time.expected_intervals import (
    generate_expected_intervals,
)
from ema.ingestion.energi_data_service_client import (
    EnergiDataServiceError,
    fetch_day_ahead_prices,
)


@dataclass
class SyncResult:
    run_id: str
    dataset: str
    price_area: str
    expected_rows: int
    observed_rows: int
    missing_rows: int
    first_missing_time_dk: datetime | None
    last_missing_time_dk: datetime | None
    status: str
    message: str


def _normalize_dataframe_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make timestamp columns compatible with DuckDB TIMESTAMP columns.

    DuckDB TIMESTAMP columns are timezone-naive in your current schema.
    So we store naive timestamps:
    - time_utc as UTC time without tzinfo
    - time_dk as Danish local time without tzinfo
    """
    if df.empty:
        return df

    df = df.copy()

    df["time_utc"] = pd.to_datetime(df["time_utc"]).dt.tz_localize(None)
    df["time_dk"] = pd.to_datetime(df["time_dk"]).dt.tz_localize(None)

    return df


def _insert_expected_placeholders(
    con: duckdb.DuckDBPyConnection,
    expected_df: pd.DataFrame,
) -> int:
    """
    Insert expected rows that do not already exist.

    These are placeholder rows with NULL prices and status missing_not_checked.
    """
    expected_df = _normalize_dataframe_timestamps(expected_df)

    con.register("expected_intervals_df", expected_df)

    result = con.execute(
        """
        INSERT INTO fact_day_ahead_prices (
            time_utc,
            time_dk,
            date_dk,
            hour_dk,
            minute_dk,
            price_area,
            day_ahead_price_eur,
            day_ahead_price_dkk,
            data_status,
            source_dataset,
            source,
            last_checked_at,
            last_updated_at
        )
        SELECT
            e.time_utc,
            e.time_dk,
            e.date_dk,
            e.hour_dk,
            e.minute_dk,
            e.price_area,
            e.day_ahead_price_eur,
            e.day_ahead_price_dkk,
            e.data_status,
            e.source_dataset,
            e.source,
            NULL,
            NULL
        FROM expected_intervals_df e
        WHERE NOT EXISTS (
            SELECT 1
            FROM fact_day_ahead_prices f
            WHERE f.time_utc = e.time_utc
              AND f.price_area = e.price_area
        );
        """
    ).fetchone()

    con.unregister("expected_intervals_df")

    # DuckDB may return None depending on version, so keep this safe.
    return result[0] if result and result[0] is not None else 0


def _get_missing_range(
    con: duckdb.DuckDBPyConnection,
    price_area: str,
) -> tuple[datetime | None, datetime | None, int]:
    """
    Find the range of rows that still need prices.
    """
    result = con.execute(
        """
        SELECT
            MIN(time_dk) AS first_missing_time_dk,
            MAX(time_dk) AS last_missing_time_dk,
            COUNT(*) AS missing_rows
        FROM fact_day_ahead_prices
        WHERE price_area = ?
          AND day_ahead_price_eur IS NULL;
        """,
        [price_area],
    ).fetchone()

    first_missing_time_dk, last_missing_time_dk, missing_rows = result

    return first_missing_time_dk, last_missing_time_dk, missing_rows


def _update_observed_prices(
    con: duckdb.DuckDBPyConnection,
    observed_df: pd.DataFrame,
) -> int:
    """
    Update existing placeholder rows with observed API prices.
    """
    if observed_df.empty:
        return 0

    observed_df = _normalize_dataframe_timestamps(observed_df)

    con.register("observed_prices_df", observed_df)

    result = con.execute(
        """
        UPDATE fact_day_ahead_prices AS f
        SET
            day_ahead_price_eur = o.day_ahead_price_eur,
            day_ahead_price_dkk = o.day_ahead_price_dkk,
            data_status = 'observed',
            source_dataset = o.source_dataset,
            source = o.source,
            last_checked_at = CURRENT_TIMESTAMP,
            last_updated_at = CURRENT_TIMESTAMP
        FROM observed_prices_df AS o
        WHERE f.time_utc = o.time_utc
          AND f.price_area = o.price_area;
        """
    ).fetchone()

    con.unregister("observed_prices_df")

    return result[0] if result and result[0] is not None else 0


def _mark_requested_but_missing_as_missing_api(
    con: duckdb.DuckDBPyConnection,
    price_area: str,
    requested_start_time_dk: datetime,
    requested_end_time_dk: datetime,
) -> int:
    """
    Any rows inside the requested API range that still have NULL prices
    are marked as missing_api.
    """
    result = con.execute(
        """
        UPDATE fact_day_ahead_prices
        SET
            data_status = 'missing_api',
            last_checked_at = CURRENT_TIMESTAMP
        WHERE price_area = ?
          AND time_dk >= ?
          AND time_dk < ?
          AND day_ahead_price_eur IS NULL;
        """,
        [price_area, requested_start_time_dk, requested_end_time_dk],
    ).fetchone()

    return result[0] if result and result[0] is not None else 0


def _write_ingestion_run_start(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    dataset: str,
    price_area: str,
    requested_start_time_dk: datetime | None,
    requested_end_time_dk: datetime | None,
) -> None:
    con.execute(
        """
        INSERT INTO ingestion_runs (
            run_id,
            dataset,
            price_area,
            requested_start_time_dk,
            requested_end_time_dk,
            status,
            message
        )
        VALUES (?, ?, ?, ?, ?, 'running', 'Sync started');
        """,
        [run_id, dataset, price_area, requested_start_time_dk, requested_end_time_dk],
    )


def _write_ingestion_run_end(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    inserted_rows: int,
    updated_rows: int,
    missing_rows: int,
    status: str,
    message: str,
) -> None:
    con.execute(
        """
        UPDATE ingestion_runs
        SET
            completed_at = CURRENT_TIMESTAMP,
            inserted_rows = ?,
            updated_rows = ?,
            missing_rows = ?,
            status = ?,
            message = ?
        WHERE run_id = ?;
        """,
        [inserted_rows, updated_rows, missing_rows, status, message, run_id],
    )


def _write_quality_report(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    dataset: str,
    price_area: str,
    interval_minutes: int,
) -> SyncResult:
    """
    Write one row to data_quality_day_ahead_prices and return summary.
    """
    result = con.execute(
        """
        SELECT
            MIN(time_dk) AS expected_start_time_dk,
            MAX(time_dk) AS expected_end_time_dk,
            COUNT(*) AS expected_rows,
            SUM(CASE WHEN data_status = 'observed' THEN 1 ELSE 0 END) AS observed_rows,
            SUM(CASE WHEN day_ahead_price_eur IS NULL THEN 1 ELSE 0 END) AS missing_rows,
            MIN(CASE WHEN day_ahead_price_eur IS NULL THEN time_dk ELSE NULL END) AS first_missing_time_dk,
            MAX(CASE WHEN day_ahead_price_eur IS NULL THEN time_dk ELSE NULL END) AS last_missing_time_dk
        FROM fact_day_ahead_prices
        WHERE price_area = ?;
        """,
        [price_area],
    ).fetchone()

    (
        expected_start_time_dk,
        expected_end_time_dk,
        expected_rows,
        observed_rows,
        missing_rows,
        first_missing_time_dk,
        last_missing_time_dk,
    ) = result

    observed_rows = int(observed_rows or 0)
    missing_rows = int(missing_rows or 0)
    expected_rows = int(expected_rows or 0)

    if missing_rows == 0:
        status = "ok"
        message = "All expected intervals have observed prices."
    else:
        status = "missing_data"
        message = f"{missing_rows} expected intervals are missing prices."

    con.execute(
        """
        INSERT INTO data_quality_day_ahead_prices (
            run_id,
            price_area,
            expected_start_time_dk,
            expected_end_time_dk,
            interval_minutes,
            expected_rows,
            observed_rows,
            missing_rows,
            first_missing_time_dk,
            last_missing_time_dk,
            status,
            message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [
            run_id,
            price_area,
            expected_start_time_dk,
            expected_end_time_dk,
            interval_minutes,
            expected_rows,
            observed_rows,
            missing_rows,
            first_missing_time_dk,
            last_missing_time_dk,
            status,
            message,
        ],
    )

    return SyncResult(
        run_id=run_id,
        dataset=dataset,
        price_area=price_area,
        expected_rows=expected_rows,
        observed_rows=observed_rows,
        missing_rows=missing_rows,
        first_missing_time_dk=first_missing_time_dk,
        last_missing_time_dk=last_missing_time_dk,
        status=status,
        message=message,
    )


def sync_day_ahead_prices(
    start_date_dk: date,
    db_path: Path,
    price_area: str,
    interval_minutes: int,
    timezone: str,
    api_base_url: str,
    dataset: str,
    provider: str,
    timeout_seconds: int,
    end_time_dk: datetime | None = None,
) -> SyncResult:
    """
    Main sync function.

    This keeps fact_day_ahead_prices complete from start_date_dk 00:00
    until end_time_dk/current time.

    It inserts placeholder rows first, then fills them with real API data.
    """
    if price_area not in {"DK1", "DK2"}:
        raise ValueError("price_area must be either 'DK1' or 'DK2'")

    run_id = str(uuid4())

    create_schema(db_path)

    expected_df = generate_expected_intervals(
        start_date_dk=start_date_dk,
        timezone=timezone,
        interval_minutes=interval_minutes,
        price_area=price_area,
        source_dataset=dataset,
        source=provider,
        end_time_dk=end_time_dk,
    )

    with duckdb.connect(str(db_path)) as con:
        inserted_rows = _insert_expected_placeholders(con, expected_df)

        first_missing_time_dk, last_missing_time_dk, missing_rows_before = (
            _get_missing_range(con, price_area)
        )

        if missing_rows_before == 0:
            _write_ingestion_run_start(
                con=con,
                run_id=run_id,
                dataset=dataset,
                price_area=price_area,
                requested_start_time_dk=None,
                requested_end_time_dk=None,
            )

            _write_ingestion_run_end(
                con=con,
                run_id=run_id,
                inserted_rows=inserted_rows,
                updated_rows=0,
                missing_rows=0,
                status="completed",
                message="No missing data. Nothing fetched.",
            )

            return _write_quality_report(
                con=con,
                run_id=run_id,
                dataset=dataset,
                price_area=price_area,
                interval_minutes=interval_minutes,
            )

        # API end is exclusive, so add one interval to include the last missing timestamp.
        requested_start_time_dk = first_missing_time_dk
        requested_end_time_dk = last_missing_time_dk + timedelta(
            minutes=interval_minutes
        )

        _write_ingestion_run_start(
            con=con,
            run_id=run_id,
            dataset=dataset,
            price_area=price_area,
            requested_start_time_dk=requested_start_time_dk,
            requested_end_time_dk=requested_end_time_dk,
        )

        try:
            observed_df = fetch_day_ahead_prices(
                start_time_dk=requested_start_time_dk,
                end_time_dk=requested_end_time_dk,
                price_area=price_area,
                base_url=api_base_url,
                dataset=dataset,
                provider=provider,
                timeout_seconds=timeout_seconds,
            )

            updated_rows = _update_observed_prices(con, observed_df)

            missing_api_rows = _mark_requested_but_missing_as_missing_api(
                con=con,
                price_area=price_area,
                requested_start_time_dk=requested_start_time_dk,
                requested_end_time_dk=requested_end_time_dk,
            )

            result = _write_quality_report(
                con=con,
                run_id=run_id,
                dataset=dataset,
                price_area=price_area,
                interval_minutes=interval_minutes,
            )

            ingestion_status = (
                "completed" if result.missing_rows == 0 else "completed_with_missing_data"
            )

            _write_ingestion_run_end(
                con=con,
                run_id=run_id,
                inserted_rows=inserted_rows,
                updated_rows=updated_rows,
                missing_rows=missing_api_rows,
                status=ingestion_status,
                message=result.message,
            )

            return result

        except EnergiDataServiceError as exc:
            con.execute(
                """
                UPDATE fact_day_ahead_prices
                SET
                    data_status = 'error',
                    last_checked_at = CURRENT_TIMESTAMP
                WHERE price_area = ?
                  AND time_dk >= ?
                  AND time_dk < ?
                  AND day_ahead_price_eur IS NULL;
                """,
                [price_area, requested_start_time_dk, requested_end_time_dk],
            )

            _write_ingestion_run_end(
                con=con,
                run_id=run_id,
                inserted_rows=inserted_rows,
                updated_rows=0,
                missing_rows=missing_rows_before,
                status="error",
                message=str(exc),
            )

            result = _write_quality_report(
                con=con,
                run_id=run_id,
                price_area=price_area,
                interval_minutes=interval_minutes,
                dataset=dataset,
            )

            return SyncResult(
                run_id=run_id,
                dataset=dataset,
                price_area=price_area,
                expected_rows=result.expected_rows,
                observed_rows=result.observed_rows,
                missing_rows=result.missing_rows,
                first_missing_time_dk=result.first_missing_time_dk,
                last_missing_time_dk=result.last_missing_time_dk,
                status="error",
                message=str(exc),
            )


def print_sync_result(result: SyncResult) -> None:
    print()
    print(f"{result.price_area} {result.dataset} sync completed")
    print("--------------------------------")
    print(f"Run ID:        {result.run_id}")
    print(f"Price area:    {result.price_area}")
    print(f"Status:        {result.status}")
    print(f"Expected rows: {result.expected_rows}")
    print(f"Observed rows: {result.observed_rows}")
    print(f"Missing rows:  {result.missing_rows}")

    if result.missing_rows > 0:
        print(f"First missing: {result.first_missing_time_dk}")
        print(f"Last missing:  {result.last_missing_time_dk}")

    print(f"Message:       {result.message}")