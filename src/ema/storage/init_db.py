from ema.storage.duckdb_connection import get_connection


def init_db() -> None:
    con = get_connection()

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_file_log (
            file_path TEXT PRIMARY KEY,
            source_name TEXT,
            loaded_at TIMESTAMP DEFAULT current_timestamp
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS energy_prices (
            source_name TEXT,
            area TEXT,
            timestamp_utc TIMESTAMP,
            price_eur_per_mwh DOUBLE,
            currency TEXT,
            raw_file_path TEXT,
            loaded_at TIMESTAMP DEFAULT current_timestamp
        );
    """)

    con.close()

    print("DuckDB initialized at data/warehouse/ema.duckdb")


if __name__ == "__main__":
    init_db()