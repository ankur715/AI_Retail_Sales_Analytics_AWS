"""Thin S3 helpers. Credentials come from the standard boto3 chain
(AWS_PROFILE locally, an instance/task role in a real deployment)."""
import re
from datetime import date, datetime

import boto3

from etl import config

# Retailer files are named <retailer>_<client>_<YYYYMMDD>[_partN].<ext>, the
# date being the last week-ending Saturday in the file; a large drop may come
# split into parts that share the date.
FILE_WEEK = re.compile(r"_(\d{8})(?:_part\d+)?\.(csv|xlsx|json)$", re.IGNORECASE)


def client():
    return boto3.client("s3", region_name=config.AWS_REGION)


def put_bytes(key: str, body: bytes) -> None:
    client().put_object(Bucket=config.S3_BUCKET, Key=key, Body=body)


def get_bytes(key: str) -> bytes:
    return client().get_object(Bucket=config.S3_BUCKET, Key=key)["Body"].read()


def list_keys(prefix: str) -> list[str]:
    keys = []
    for page in client().get_paginator("list_objects_v2").paginate(Bucket=config.S3_BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith("/")]
    return keys


def file_week_end(key: str) -> date | None:
    m = FILE_WEEK.search(key)
    return datetime.strptime(m.group(1), "%Y%m%d").date() if m else None


def pending_files(landing_prefix: str) -> list[str]:
    """Source files waiting in landing/, oldest week first. All of them are
    loaded in one run (parsers.parse_files), newest file winning each week."""
    keys = [k for k in list_keys(landing_prefix) if file_week_end(k)]
    return sorted(keys, key=lambda k: (file_week_end(k), k))


def move(src_key: str, dest_key: str) -> None:
    """Copy then delete (S3 has no rename). The bucket is versioned, so the
    delete only hides the landing copy -- nothing is lost."""
    s3 = client()
    s3.copy_object(Bucket=config.S3_BUCKET, Key=dest_key, CopySource={"Bucket": config.S3_BUCKET, "Key": src_key})
    s3.delete_object(Bucket=config.S3_BUCKET, Key=src_key)
