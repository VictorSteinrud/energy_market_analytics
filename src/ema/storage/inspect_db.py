from ema.storage.duckdb_connection import get_connection


def inspect_db() -> None:
    con = get_connection(read_only=True)

    print("\nTables:")
    tables = con.execute("SHOW TABLES").fetchall()
    for table in tables:
        print(f"  - {table[0]}")

    print("\nenergy_prices sample:")
    try:
        rows = con.execute("""
            SELECT *
            FROM energy_prices
            ORDER BY timestamp_utc DESC
            LIMIT 10;
        """).fetchdf()

        print(rows)
    except Exception as exc:
        print(f"Could not query energy_prices: {exc}")

    con.close()


if __name__ == "__main__":
    inspect_db()