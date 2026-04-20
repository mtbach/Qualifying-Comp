from __future__ import annotations

from datetime import datetime, timedelta, timezone
import io
import os
from typing import Iterable, List, Dict, Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError
from dotenv_vault import load_dotenv


load_dotenv()

# QUALIFYING SESSIONS
# SESSIONS = [11230,11241,11249,11257,11265,11276,11287,11295,11303,11311,11322,11330,11338,11349,11357,11365,11373,11384,11392,11400,11408,11416,11424,11432]
DRIVER_NUMBERS = [1, 81]

WEEKLY_ENDPOINTS = ["car_data", "location", "laps", "stints", "session_result"]

DIMENSION_ENDPOINTS = ["sessions", "drivers", "meetings"]

BUCKET = "f1-raw-data-mtbach"
MAX_ROWS_PER_PART = 30_000
LOOKBACK_DAYS = 7

BASE_URL = "https://api.openf1.org/v1"


def _get_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


s3 = boto3.client(
    "s3",
    aws_access_key_id=_get_env("AWS_ACCESS_KEY_ID", "AWS-ACCESS_KEY_ID"),
    aws_secret_access_key=_get_env("AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY"),
)

def fetch_openf1(endpoint: str, **params: Any) -> List[Dict[str, Any]]:
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected payload for endpoint {endpoint}: {type(data)}")
    return data

def fetch_data(url, endpoint) -> List[Dict[str, Any]]:
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    if not isinstance(data, list):
        raise ValueError(f"Unexpected payload for endpoint {endpoint}: {type(data)}")
    return data

def chunked(records: List[Dict[str, Any]], chunk_size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(records), chunk_size):
        yield records[i : i + chunk_size]


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as err:
        code = err.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def write_parquet_to_s3(records: List[Dict[str, Any]], bucket: str, key: str) -> None:
 
    if object_exists(bucket, key):
        print(f"Skip existing object s3://{bucket}/{key}")
        return
    
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)

    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")

    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"Wrote {len(records)} rows to s3://{bucket}/{key}")



def build_weekly_prefix(endpoint: str, session_key: int, driver_number: int, run_dt: datetime) -> str:
    week_str = run_dt.strftime("%G-W%V")  
    return (
        f"endpoint={endpoint}/"
        f"year={run_dt.year}/ingestion_week={week_str}/"
        f"session_key={session_key}/driver_number={driver_number}/"
    )


def build_dimension_key(endpoint: str, session_key: int, run_dt: datetime) -> str:
    return f"endpoint={endpoint}/year={run_dt.year}/session_key={session_key}/snapshot.parquet"


def ingest_dimensions_once(sessions: List[Dict[str, Any]], run_dt: datetime) -> None:
    for session in sessions:
        session_key = session.get("session_key")
        if session_key is None:
            continue

        sessions_key = build_dimension_key("sessions", int(session_key), run_dt)
        write_parquet_to_s3([session], BUCKET, sessions_key)

    for endpoint in ["meetings", "drivers"]:
        records: List[Dict[str, Any]] = []
        payload = fetch_openf1(endpoint)
        records.extend(payload)

        if not records:
            print(f"No {endpoint} records for session={session_key}")
            continue
        
        key = build_dimension_key(endpoint, int(session_key), run_dt)
        write_parquet_to_s3(records, BUCKET, key)


 

def ingest_weekly_telemetry(sessions: List[Dict[str, Any]], run_dt: datetime) -> None:
    for session in sessions:
        session_key = session.get("session_key")
        if session_key is None:
            continue
        for endpoint in WEEKLY_ENDPOINTS:
            for driver_number in DRIVER_NUMBERS:
                try:
                    records = fetch_openf1(endpoint, session_key=session_key, driver_number=driver_number)
                except:
                    continue
                print(
                    f"Fetched {len(records)} records endpoint={endpoint} "
                    f"session={session_key} driver={driver_number}"
                )
                if not records:
                    continue

                prefix = build_weekly_prefix(endpoint, int(session_key), driver_number, run_dt)
                for part, part_records in enumerate(chunked(records, MAX_ROWS_PER_PART)):
                    key = f"{prefix}part-{part:05d}.parquet"
                    write_parquet_to_s3(part_records, BUCKET, key)


def main() -> None:
    run_dt = datetime.now(timezone.utc)
    cutoff = (run_dt - timedelta(days=LOOKBACK_DAYS)).isoformat()
    # cutoff = "2026-03-01T10:00:00+00:00"
    session_url = f'{BASE_URL}/sessions?date_start>={cutoff}&date_end<={run_dt.isoformat()}'
    
    sessions = fetch_data(session_url, "Sessions")

    print(f"Found {len(sessions)} sessions since {cutoff}")

    ingest_dimensions_once(sessions, run_dt)

    ingest_weekly_telemetry(sessions, run_dt)

    print("Done!")


if __name__ == "__main__":
    main()