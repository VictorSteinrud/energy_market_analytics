from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo
import tomllib


@dataclass(frozen=True)
class DatabaseConfig:
    path: Path


@dataclass(frozen=True)
class DayAheadPricesConfig:
    start_date_dk: date
    price_area: str
    interval_minutes: int
    timezone: str


@dataclass(frozen=True)
class EnergiDataServiceConfig:
    base_url: str
    dataset: str
    provider: str
    timeout_seconds: int


@dataclass(frozen=True)
class AppConfig:
    database: DatabaseConfig
    day_ahead_prices: DayAheadPricesConfig
    energi_data_service: EnergiDataServiceConfig


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid date value: {value!r}. Expected format: YYYY-MM-DD."
        ) from exc


def _require_section(raw: dict, section_name: str) -> dict:
    section = raw.get(section_name)

    if not isinstance(section, dict):
        raise KeyError(f"Missing required config section: [{section_name}]")

    return section


def _require_string(section: dict, key: str, section_name: str) -> str:
    value = section.get(key)

    if not isinstance(value, str) or not value.strip():
        raise KeyError(f"Missing or invalid config value: [{section_name}].{key}")

    return value.strip()


def _require_int(section: dict, key: str, section_name: str) -> int:
    value = section.get(key)

    if not isinstance(value, int):
        raise KeyError(f"Missing or invalid integer config value: [{section_name}].{key}")

    return value


def _resolve_path_relative_to_config(path_value: str, config_path: Path) -> Path:
    path = Path(path_value)

    if path.is_absolute():
        return path

    return config_path.parent / path


def load_config(config_path: Path) -> AppConfig:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_path}")

    with config_path.open("rb") as f:
        raw = tomllib.load(f)

    database_raw = _require_section(raw, "database")
    day_ahead_raw = _require_section(raw, "day_ahead_prices")
    energi_raw = _require_section(raw, "energi_data_service")

    database_path = _require_string(database_raw, "path", "database")

    start_date_dk = _require_string(
        day_ahead_raw,
        "start_date_dk",
        "day_ahead_prices",
    )

    price_area = _require_string(
        day_ahead_raw,
        "price_area",
        "day_ahead_prices",
    ).upper()

    interval_minutes = _require_int(
        day_ahead_raw,
        "interval_minutes",
        "day_ahead_prices",
    )

    timezone = _require_string(
        day_ahead_raw,
        "timezone",
        "day_ahead_prices",
    )

    base_url = _require_string(
        energi_raw,
        "base_url",
        "energi_data_service",
    )

    dataset = _require_string(
        energi_raw,
        "dataset",
        "energi_data_service",
    )

    provider = _require_string(
        energi_raw,
        "provider",
        "energi_data_service",
    )

    timeout_seconds = _require_int(
        energi_raw,
        "timeout_seconds",
        "energi_data_service",
    )

    if price_area not in {"DK1", "DK2"}:
        raise ValueError(
            f"Invalid price_area: {price_area!r}. Expected 'DK1' or 'DK2'."
        )

    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be positive.")
    
    if interval_minutes != 15:
        raise ValueError("interval_minutes must be 15 for DayAheadPrices.")

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive.")

    try:
        ZoneInfo(timezone)
    except Exception as exc:
        raise ValueError(f"Invalid timezone: {timezone!r}") from exc

    return AppConfig(
        database=DatabaseConfig(
            path=_resolve_path_relative_to_config(
                database_path,
                config_path,
            ),
        ),
        day_ahead_prices=DayAheadPricesConfig(
            start_date_dk=_parse_date(start_date_dk),
            price_area=price_area,
            interval_minutes=interval_minutes,
            timezone=timezone,
        ),
        energi_data_service=EnergiDataServiceConfig(
            base_url=base_url.rstrip("/"),
            dataset=dataset,
            provider=provider,
            timeout_seconds=timeout_seconds,
        ),
    )