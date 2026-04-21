from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List

from .clients import build_s3_client
from .config import IngestionSettings, load_settings
from .openf1 import OpenF1Client
from .storage import S3ParquetWriter, build_weekly_prefix, chunked


class OpenF1IngestionPipeline:
    def __init__(
        self,
        settings: IngestionSettings,
        api_client: OpenF1Client,
        writer: S3ParquetWriter,
    ):
        self.settings = settings
        self.api_client = api_client
        self.writer = writer

    def run(self, run_dt: datetime | None = None) -> None:
        effective_run_dt = run_dt or datetime.now(timezone.utc)
        cutoff = (effective_run_dt - timedelta(days=self.settings.lookback_days)).isoformat()

        sessions = self.api_client.fetch_recent_sessions(
            run_dt=effective_run_dt,
            lookback_days=self.settings.lookback_days,
        )
        print(f"Found {len(sessions)} sessions since {cutoff}")

        self.ingest_weekly_dimensions(sessions, effective_run_dt)
        self.ingest_weekly_telemetry(sessions, effective_run_dt)

        print("Done!")

    def ingest_weekly_dimensions(self, sessions: List[Dict[str, Any]], run_dt: datetime) -> None:
        for session, session_key in self._iter_sessions(sessions):
            for endpoint in self.settings.dimension_endpoints:
                if self._is_partition_complete(
                    endpoint=endpoint,
                    session_key=session_key,
                    run_dt=run_dt,
                ):
                    print(f"Skip completed partition endpoint={endpoint} session={session_key}")
                    continue
                records = self._fetch_dimension_records(endpoint=endpoint, session=session)
                print(f"Fetched {len(records)} records endpoint={endpoint} session={session_key}")
                self._write_endpoint_records(
                    endpoint=endpoint,
                    session_key=session_key,
                    run_dt=run_dt,
                    records=records,
                )

    def ingest_weekly_telemetry(self, sessions: List[Dict[str, Any]], run_dt: datetime) -> None:
        for _, session_key in self._iter_sessions(sessions):
            for endpoint in self.settings.weekly_endpoints:
                for driver_number in self.settings.driver_numbers:
                    if self._is_partition_complete(
                        endpoint=endpoint,
                        session_key=session_key,
                        run_dt=run_dt,
                        driver_number=driver_number,
                    ):
                        print(
                            f"Skip completed partition endpoint={endpoint} "
                            f"session={session_key} driver={driver_number}"
                        )
                        continue
                    records = self._fetch_weekly_records(
                        endpoint=endpoint,
                        session_key=session_key,
                        driver_number=driver_number,
                    )
                    if records is None:
                        continue

                    print(
                        f"Fetched {len(records)} records endpoint={endpoint} "
                        f"session={session_key} driver={driver_number}"
                    )
                    self._write_endpoint_records(
                        endpoint=endpoint,
                        session_key=int(session_key),
                        run_dt=run_dt,
                        records=records,
                        driver_number=driver_number,
                    )

    def _iter_sessions(
        self,
        sessions: List[Dict[str, Any]],
    ) -> Iterable[tuple[Dict[str, Any], int]]:
        for session in sessions:
            session_key = session.get("session_key")
            if session_key is None:
                continue
            yield session, int(session_key)

    def _fetch_dimension_records(
        self,
        endpoint: str,
        session: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        session_key = session["session_key"]
        if endpoint == "sessions":
            return [session]
        if endpoint == "drivers":
            return self.api_client.fetch(endpoint, session_key=session_key)
        raise ValueError(f"Unsupported dimension endpoint: {endpoint}")

    def _fetch_weekly_records(
        self,
        endpoint: str,
        session_key: int,
        driver_number: int,
    ) -> List[Dict[str, Any]] | None:
        try:
            return self.api_client.fetch(
                endpoint,
                session_key=session_key,
                driver_number=driver_number,
            )
        except Exception:
            return None

    def _write_endpoint_records(
        self,
        endpoint: str,
        session_key: int,
        run_dt: datetime,
        records: List[Dict[str, Any]],
        driver_number: int | None = None,
    ) -> None:
        if not records:
            return

        prefix = self._build_partition_prefix(
            endpoint=endpoint,
            session_key=session_key,
            run_dt=run_dt,
            driver_number=driver_number,
        )

        part_count = 0
        record_count = 0
        for part, part_records in enumerate(chunked(records, self.settings.max_rows_per_part)):
            key = f"{prefix}part-{part:05d}.parquet"
            self.writer.write_records(part_records, key)
            part_count += 1
            record_count += len(part_records)

        self.writer.write_success_marker(
            prefix,
            part_count=part_count,
            record_count=record_count,
            completed_at=run_dt,
        )

    def _is_partition_complete(
        self,
        endpoint: str,
        session_key: int,
        run_dt: datetime,
        driver_number: int | None = None,
    ) -> bool:
        prefix = self._build_partition_prefix(
            endpoint=endpoint,
            session_key=session_key,
            run_dt=run_dt,
            driver_number=driver_number,
        )
        return self.writer.is_partition_complete(prefix)

    def _build_partition_prefix(
        self,
        endpoint: str,
        session_key: int,
        run_dt: datetime,
        driver_number: int | None = None,
    ) -> str:
        return build_weekly_prefix(
            endpoint=endpoint,
            session_key=session_key,
            run_dt=run_dt,
            driver_number=driver_number,
        )


def build_default_pipeline() -> OpenF1IngestionPipeline:
    settings = load_settings()
    api_client = OpenF1Client(base_url=settings.base_url)
    writer = S3ParquetWriter(s3_client=build_s3_client(), bucket=settings.bucket)
    return OpenF1IngestionPipeline(settings=settings, api_client=api_client, writer=writer)


def main() -> None:
    build_default_pipeline().run()
