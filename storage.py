"""
Small storage helper for the Data Lake.

By default it writes to the local data_lake folder. If DATA_LAKE_BACKEND=s3,
the same functions write to MinIO or another S3-compatible storage.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BACKEND = os.getenv("DATA_LAKE_BACKEND", "local").lower()
DATA_LAKE_ROOT = Path(os.getenv("DATA_LAKE_ROOT", "data_lake"))
PROJECT_ROOT = Path(__file__).resolve().parent
if not DATA_LAKE_ROOT.is_absolute():
    DATA_LAKE_ROOT = PROJECT_ROOT / DATA_LAKE_ROOT

S3_CLIENT = None
S3_BUCKET = None
S3_ENDPOINT = None
S3_ACCESS_KEY = None
S3_SECRET_KEY = None

if BACKEND == "s3":
    try:
        import boto3
        from botocore.client import Config
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "DATA_LAKE_BACKEND=s3 requires boto3. Install it with `pip install boto3`."
        ) from exc

    S3_ENDPOINT = os.getenv("DATA_LAKE_S3_ENDPOINT", "http://minio:9000")
    S3_ACCESS_KEY = os.getenv("DATA_LAKE_S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY = os.getenv("DATA_LAKE_S3_SECRET_KEY", "minioadmin")
    S3_BUCKET = os.getenv("DATA_LAKE_S3_BUCKET", "news-data-lake")

    config = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    S3_CLIENT = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=config,
    )
    
    def ensure_bucket():
        try:
            S3_CLIENT.head_bucket(Bucket=S3_BUCKET)
            return
        except ClientError:
            logger.info(f"Creating S3 bucket: {S3_BUCKET}")
            S3_CLIENT.create_bucket(Bucket=S3_BUCKET)

else:
    def ensure_bucket():
        return


def _normalize_key(path: str) -> str:
    return str(path).lstrip("/")


def _local_path(key: str) -> Path:
    return DATA_LAKE_ROOT / _normalize_key(key)


def _ensure_local_parent(key: str) -> None:
    path = _local_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(key: str, data):
    """Save JSON in the selected storage backend."""
    key = _normalize_key(key)
    if BACKEND == "s3":
        ensure_bucket()
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        S3_CLIENT.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
        logger.debug(f"Wrote S3 object s3://{S3_BUCKET}/{key}")
    else:
        _ensure_local_parent(key)
        _local_path(key).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.debug(f"Wrote local file {_local_path(key)}")


def read_json(key: str):
    """Read JSON from the selected storage backend."""
    key = _normalize_key(key)
    if BACKEND == "s3":
        ensure_bucket()
        response = S3_CLIENT.get_object(Bucket=S3_BUCKET, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    else:
        return json.loads(_local_path(key).read_text(encoding="utf-8"))


def exists(key: str) -> bool:
    key = _normalize_key(key)
    if BACKEND == "s3":
        ensure_bucket()
        try:
            S3_CLIENT.head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except Exception:
            return False
    return _local_path(key).exists()


def modified_at(key: str) -> datetime | None:
    """Return the last modification time when it is available."""
    key = _normalize_key(key)
    if BACKEND == "s3":
        ensure_bucket()
        try:
            response = S3_CLIENT.head_object(Bucket=S3_BUCKET, Key=key)
            return response.get("LastModified")
        except Exception:
            return None

    path = _local_path(key)
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def list_json(prefix: str = "") -> list[str]:
    """List JSON files under one folder/prefix."""
    prefix = _normalize_key(prefix)
    if BACKEND == "s3":
        ensure_bucket()
        paginator = S3_CLIENT.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".json"):
                    keys.append(key)
        return keys

    path = _local_path(prefix)
    if path.is_file() and path.suffix == ".json":
        return [prefix]
    if not path.exists():
        return []
    return [
        str(p.relative_to(DATA_LAKE_ROOT)).replace("\\", "/")
        for p in path.rglob("*.json")
    ]


def ensure_prefix(prefix: str) -> None:
    prefix = _normalize_key(prefix)
    if BACKEND == "s3":
        ensure_bucket()
        return
    _local_path(prefix).mkdir(parents=True, exist_ok=True)


def storage_uri(key: str) -> str:
    """Return a readable path or S3 URI."""
    key = _normalize_key(key)
    if BACKEND == "s3":
        return f"s3://{S3_BUCKET}/{key}"
    return str(_local_path(key))


def get_backend_description() -> str:
    if BACKEND == "s3":
        return f"S3-compatible storage on {S3_ENDPOINT} bucket {S3_BUCKET}"
    return f"local filesystem at {DATA_LAKE_ROOT}"
