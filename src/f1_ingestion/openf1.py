from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests


class OpenF1Client:
    def __init__(self, base_url: str, timeout_seconds: int = 60):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def fetch(self, endpoint: str, **params: Any) -> List[Dict[str, Any]]:
        response = requests.get(f"{self.base_url}/{endpoint}", params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected payload for endpoint {endpoint}: {type(data)}")
        return data

    def fetch_url(self, url: str, endpoint_name: str) -> List[Dict[str, Any]]:
        response = requests.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected payload for endpoint {endpoint_name}: {type(data)}")
        return data

    def fetch_recent_sessions(self, run_dt: datetime, lookback_days: int) -> List[Dict[str, Any]]:
        cutoff = (run_dt - timedelta(days=lookback_days)).isoformat()
        sessions_url = (
            f"{self.base_url}/sessions?date_start>={cutoff}&date_end<={run_dt.isoformat()}"
        )
        return self.fetch_url(sessions_url, "sessions")
