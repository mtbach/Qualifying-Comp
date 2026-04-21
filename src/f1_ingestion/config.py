from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from dotenv_vault import load_dotenv


@dataclass(frozen=True)
class IngestionSettings:
    base_url: str = "https://api.openf1.org/v1"
    bucket: str = "f1-raw-data-mtbach"
    max_rows_per_part: int = 30_000
    lookback_days: int = 7
    driver_numbers: List[int] = field(default_factory=lambda: [1, 81])
    weekly_endpoints: List[str] = field(
        default_factory=lambda: ["car_data", "location", "laps", "stints", "session_result"]
    )
    dimension_endpoints: List[str] = field(default_factory=lambda: ["sessions", "drivers"])


def load_settings() -> IngestionSettings:
    load_dotenv()
    return IngestionSettings()
