from __future__ import annotations

import os

import boto3


def get_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def build_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=get_env("AWS_ACCESS_KEY_ID", "AWS-ACCESS_KEY_ID"),
        aws_secret_access_key=get_env("AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY"),
    )
