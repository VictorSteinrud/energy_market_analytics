from pathlib import Path

import duckdb


def ensure_database_directory(db_path: Path) -> None:
    """
    Make sure the DuckDB parent folder exists.
    Example: data/warehouse/
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)


def create_schema(db_path: Path) -> None:
    """
    Create the DuckDB database schema if it does not already exist.

    This does not fetch data.
    This only prepares the database tables.
    """
    ensure_database_directory(db_path)

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_day_ahead_prices (
                time_utc TIMESTAMP NOT NULL,
                time_dk TIMESTAMP NOT NULL,

                date_dk DATE NOT NULL,
                hour_dk INTEGER NOT NULL,
                minute_dk INTEGER NOT NULL,

                price_area VARCHAR NOT NULL,

                day_ahead_price_eur DOUBLE,
                day_ahead_price_dkk DOUBLE,

                data_status VARCHAR NOT NULL DEFAULT 'missing_not_checked',

                source_dataset VARCHAR NOT NULL,
                source VARCHAR NOT NULL,

                last_checked_at TIMESTAMP,
                last_updated_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (time_utc, price_area),

                CHECK (price_area IN ('DK1', 'DK2')),
                CHECK (minute_dk IN (0, 15, 30, 45)),
                CHECK (
                    data_status IN (
                        'observed',
                        'missing_not_checked',
                        'missing_api',
                        'outside_available_range',
                        'error'
                    )
                )
            );
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS data_quality_day_ahead_prices (
                run_id VARCHAR NOT NULL,

                checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                price_area VARCHAR NOT NULL,
                expected_start_time_dk TIMESTAMP NOT NULL,
                expected_end_time_dk TIMESTAMP NOT NULL,

                interval_minutes INTEGER NOT NULL,

                expected_rows INTEGER NOT NULL,
                observed_rows INTEGER NOT NULL,
                missing_rows INTEGER NOT NULL,

                first_missing_time_dk TIMESTAMP,
                last_missing_time_dk TIMESTAMP,

                status VARCHAR NOT NULL,
                message VARCHAR,

                PRIMARY KEY (run_id),

                CHECK (price_area IN ('DK1', 'DK2')),
                CHECK (interval_minutes > 0),
                CHECK (expected_rows >= 0),
                CHECK (observed_rows >= 0),
                CHECK (missing_rows >= 0),
                CHECK (status IN ('ok', 'missing_data', 'error'))
            );
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id VARCHAR NOT NULL,

                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,

                dataset VARCHAR NOT NULL,
                price_area VARCHAR NOT NULL,

                requested_start_time_dk TIMESTAMP,
                requested_end_time_dk TIMESTAMP,

                inserted_rows INTEGER DEFAULT 0,
                updated_rows INTEGER DEFAULT 0,
                missing_rows INTEGER DEFAULT 0,

                status VARCHAR NOT NULL,
                message VARCHAR,

                PRIMARY KEY (run_id),

                CHECK (price_area IN ('DK1', 'DK2')),
                CHECK (status IN ('running', 'completed', 'completed_with_missing_data', 'error'))
            );
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_day_ahead_prices_time_dk
            ON fact_day_ahead_prices (time_dk);
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_day_ahead_prices_date_area
            ON fact_day_ahead_prices (date_dk, price_area);
            """
        )


def print_schema_summary(db_path: Path) -> None:
    """
    Print a small confirmation that the database exists
    and show the created tables.
    """
    with duckdb.connect(str(db_path)) as con:
        tables = con.execute("SHOW TABLES;").fetchall()

    print(f"DuckDB database ready: {db_path}")
    print("Tables:")
    for table in tables:
        print(f"  - {table[0]}")