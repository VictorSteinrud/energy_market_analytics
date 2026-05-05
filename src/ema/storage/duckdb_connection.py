from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[3]
WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"
DB_PATH = WAREHOUSE_DIR / "ema.duckdb"


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Opens a connection to the project DuckDB database.

    The database is stored at:
        data/warehouse/ema.duckdb
    """
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    return duckdb.connect(
        database=str(DB_PATH),
        read_only=read_only,
    )