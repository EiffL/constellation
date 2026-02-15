"""S3 upload utilities and run_id resolution for structured output."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def get_run_id(field_name: str) -> str:
    """Resolve a run ID for the current execution.

    Uses the Flyte execution ID when running inside a Flyte task,
    otherwise falls back to ``{field_name}_{YYYYMMDD_HHMMSS}``.

    Args:
        field_name: Survey field name, used in the fallback ID.

    Returns:
        A stable run identifier string.
    """
    try:
        from flytekit import current_context

        ctx = current_context()
        exec_name = ctx.execution_id.name
        # "local" is the default when not running inside a Flyte execution
        if exec_name and exec_name != "local":
            return exec_name
    except Exception:
        pass

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{field_name}_{ts}"


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse ``s3://bucket/key`` into ``(bucket, key)``.

    Args:
        s3_uri: Full S3 URI.

    Returns:
        ``(bucket, key)`` tuple.
    """
    without_scheme = s3_uri.replace("s3://", "", 1)
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def upload_file(
    local_path: str | Path,
    s3_uri: str,
    skip_existing: bool = True,
) -> bool:
    """Upload a single file to S3.

    Args:
        local_path: Path to the local file.
        s3_uri: Destination ``s3://bucket/key``.
        skip_existing: If True, skip upload when the object already exists.

    Returns:
        True if the file was uploaded, False if skipped.
    """
    bucket, key = _parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")

    if skip_existing:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            logger.debug("Skipping existing: %s", s3_uri)
            return False
        except ClientError:
            pass

    logger.info("Uploading %s -> %s", local_path, s3_uri)
    s3.upload_file(str(local_path), bucket, key)
    return True


def upload_directory(
    local_dir: str | Path,
    s3_uri_prefix: str,
    skip_existing: bool = True,
    max_workers: int = 10,
) -> int:
    """Recursively upload a directory tree to S3 with concurrent uploads.

    Args:
        local_dir: Local directory to upload.
        s3_uri_prefix: S3 URI prefix (e.g. ``s3://bucket/run/tile/0_0``).
        skip_existing: If True, skip files that already exist on S3.
        max_workers: Maximum number of concurrent upload threads.

    Returns:
        Number of files uploaded (not skipped).
    """
    from concurrent.futures import ThreadPoolExecutor

    local_dir = Path(local_dir)
    s3_uri_prefix = s3_uri_prefix.rstrip("/")
    bucket, base_key = _parse_s3_uri(s3_uri_prefix)

    # Collect all files to upload
    files = [
        (path, f"{base_key}/{path.relative_to(local_dir)}")
        for path in sorted(local_dir.rglob("*"))
        if not path.is_dir()
    ]

    if not files:
        return 0

    # Single client shared across threads (boto3 clients are thread-safe)
    s3 = boto3.client("s3")

    def _upload_one(item: tuple[Path, str]) -> bool:
        local_path, key = item
        if skip_existing:
            try:
                s3.head_object(Bucket=bucket, Key=key)
                return False
            except ClientError:
                pass
        s3.upload_file(str(local_path), bucket, key)
        return True

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(_upload_one, files))

    uploaded = sum(results)
    logger.info(
        "Uploaded %d/%d files from %s to %s",
        uploaded,
        len(files),
        local_dir,
        s3_uri_prefix,
    )
    return uploaded


def build_subtile_prefix(
    base: str,
    run_id: str,
    tile_id: int,
    row: int,
    col: int,
) -> str:
    """Build the S3 prefix for a sub-tile's output.

    Returns:
        ``{base}/{run_id}/{tile_id}/{row}_{col}``
    """
    base = base.rstrip("/")
    return f"{base}/{run_id}/{tile_id}/{row}_{col}"
