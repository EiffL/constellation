"""Integration test: end-to-end local pipeline with moto S3."""

import pytest

from constellation.config import PipelineConfig
from constellation.workflows.local_runner import run_local_pipeline


@pytest.mark.integration
class TestEndToEnd:
    def test_local_pipeline_three_tiles(self, mock_s3, tmp_path):
        """Run the full pipeline with 3 tiles using moto-mocked S3.

        Expected: 3 tiles × 16 sub-tiles = 48 rows in Iceberg.
        """
        config = PipelineConfig(
            field_name="EDFF_TEST",
            tile_ids=[102018211, 102018212, 102018213],
            output={
                "catalog_warehouse": str(tmp_path / "warehouse"),
                "result_dir": str(tmp_path / "results"),
                "manifest_dir": str(tmp_path / "manifests"),
            },
            mock_shine=True,
        )

        stats = run_local_pipeline(config)

        assert stats["row_count"] == 48
        assert stats["tile_count"] == 3
        assert stats["completeness"] == 1.0
        assert abs(stats["g1_mean"]) < 0.1  # mock shear should be near 0

    def test_local_pipeline_single_tile(self, mock_s3, tmp_path):
        """Single tile produces 16 rows."""
        config = PipelineConfig(
            field_name="EDFF_TEST",
            tile_ids=[102018211],
            output={
                "catalog_warehouse": str(tmp_path / "warehouse"),
                "result_dir": str(tmp_path / "results"),
                "manifest_dir": str(tmp_path / "manifests"),
            },
            mock_shine=True,
        )

        stats = run_local_pipeline(config)

        assert stats["row_count"] == 16
        assert stats["tile_count"] == 1
        assert stats["completeness"] == 1.0
