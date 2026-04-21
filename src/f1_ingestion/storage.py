from __future__ import annotations

from datetime import datetime
import io
import json
from typing import Dict, Iterable, List, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


def chunked(records: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(records), chunk_size):
        yield records[i : i + chunk_size]


def build_weekly_prefix(
    endpoint: str,
    session_key: int,
    run_dt: datetime,
    driver_number: int | None = None,
) -> str:
    week_str = run_dt.strftime("%G-W%V")
    prefix = (
        f"endpoint={endpoint}/"
        f"year={run_dt.year}/ingestion_week={week_str}/"
        f"session_key={session_key}/"
    )
    if driver_number is not None:
        prefix += f"driver_number={driver_number}/"
    return prefix


class S3ParquetWriter:
    success_marker_name = "_SUCCESS"

    def __init__(self, s3_client, bucket: str):
        self.s3 = s3_client
        self.bucket = bucket

    def object_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def success_marker_key(self, prefix: str) -> str:
        return f"{prefix}{self.success_marker_name}"

    def is_partition_complete(self, prefix: str) -> bool:
        return self.object_exists(self.success_marker_key(prefix))

    def write_success_marker(
        self,
        prefix: str,
        *,
        part_count: int,
        record_count: int,
        completed_at: datetime,
    ) -> None:
        marker_key = self.success_marker_key(prefix)
        payload = {
            "status": "complete",
            "part_count": part_count,
            "record_count": record_count,
            "completed_at": completed_at.isoformat(),
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=marker_key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Wrote completion marker s3://{self.bucket}/{marker_key}")

    def write_records(self, records: List[Dict[str, Any]], key: str) -> None:
        if self.object_exists(key):
            print(f"Skip existing object s3://{self.bucket}/{key}")
            return

        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        print(f"Wrote {len(records)} rows to s3://{self.bucket}/{key}")
