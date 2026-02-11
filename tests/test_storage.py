"""Tests for constellation.storage."""

from __future__ import annotations

import re
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from constellation.storage import (
    build_subtile_prefix,
    get_run_id,
    upload_directory,
    upload_file,
)

MOCK_BUCKET = "test-output-bucket"
MOCK_REGION = "us-east-1"


@pytest.fixture
def s3_bucket():
    """Create a moto-mocked S3 bucket for upload tests."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=MOCK_REGION)
        s3.create_bucket(Bucket=MOCK_BUCKET)
        yield s3


class TestGetRunId:
    def test_fallback_format(self):
        """Without Flyte context, produces {field}_{YYYYMMDD_HHMMSS}."""
        run_id = get_run_id("EDFF")
        assert run_id.startswith("EDFF_")
        # Check timestamp portion matches expected pattern
        ts_part = run_id[len("EDFF_"):]
        assert re.match(r"\d{8}_\d{6}", ts_part)


class TestUploadFile:
    def test_upload_file(self, s3_bucket, tmp_path):
        """Upload a file and verify it exists on S3."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("hello")

        s3_uri = f"s3://{MOCK_BUCKET}/run1/tile/0_0/manifest.yaml"
        result = upload_file(local_file, s3_uri)

        assert result is True
        obj = s3_bucket.get_object(
            Bucket=MOCK_BUCKET, Key="run1/tile/0_0/manifest.yaml"
        )
        assert obj["Body"].read() == b"hello"

    def test_upload_file_skip_existing(self, s3_bucket, tmp_path):
        """Verify HEAD check skips re-upload."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("hello")

        s3_uri = f"s3://{MOCK_BUCKET}/run1/file.txt"

        # First upload succeeds
        assert upload_file(local_file, s3_uri) is True
        # Second upload is skipped
        assert upload_file(local_file, s3_uri, skip_existing=True) is False

    def test_upload_file_no_skip(self, s3_bucket, tmp_path):
        """With skip_existing=False, always uploads."""
        local_file = tmp_path / "test.txt"
        local_file.write_text("v1")

        s3_uri = f"s3://{MOCK_BUCKET}/run1/file.txt"
        upload_file(local_file, s3_uri)

        local_file.write_text("v2")
        assert upload_file(local_file, s3_uri, skip_existing=False) is True

        obj = s3_bucket.get_object(Bucket=MOCK_BUCKET, Key="run1/file.txt")
        assert obj["Body"].read() == b"v2"


class TestUploadDirectory:
    def test_upload_directory(self, s3_bucket, tmp_path):
        """Recursively upload a directory tree."""
        # Create directory structure
        (tmp_path / "exposures").mkdir()
        (tmp_path / "psf").mkdir()
        (tmp_path / "exposures" / "det.fits").write_bytes(b"fits-data")
        (tmp_path / "psf" / "psf.fits").write_bytes(b"psf-data")
        (tmp_path / "catalog.fits").write_bytes(b"catalog-data")
        (tmp_path / "manifest_local.yaml").write_text("manifest-data")

        s3_prefix = f"s3://{MOCK_BUCKET}/run1/102018211/0_0"
        uploaded = upload_directory(tmp_path, s3_prefix)

        assert uploaded == 4

        # Verify files exist
        obj = s3_bucket.get_object(
            Bucket=MOCK_BUCKET, Key="run1/102018211/0_0/catalog.fits"
        )
        assert obj["Body"].read() == b"catalog-data"

        obj = s3_bucket.get_object(
            Bucket=MOCK_BUCKET, Key="run1/102018211/0_0/exposures/det.fits"
        )
        assert obj["Body"].read() == b"fits-data"


class TestBuildSubtilePrefix:
    def test_format(self):
        result = build_subtile_prefix(
            "s3://my-bucket/output", "run123", 102018211, 2, 3
        )
        assert result == "s3://my-bucket/output/run123/102018211/2_3"

    def test_trailing_slash_stripped(self):
        result = build_subtile_prefix(
            "s3://my-bucket/output/", "run123", 102018211, 0, 0
        )
        assert result == "s3://my-bucket/output/run123/102018211/0_0"
