"""Integration test: end-to-end local pipeline with moto S3."""

from __future__ import annotations

from pathlib import Path

import pytest

from constellation.config import PipelineConfig
from constellation.workflows.local_runner import run_local_pipeline


@pytest.mark.integration
class TestEndToEnd:
    def test_local_pipeline_three_tiles(self, mock_s3, tmp_path):
        """Run the full pipeline with 3 tiles using moto-mocked S3.

        Expected: 3 tiles × 16 sub-tiles = 48 rows in Iceberg.
        Uses legacy mode (empty FITS files, no WCS headers).
        Note: s3_no_sign_request=False because moto doesn't support
        UNSIGNED for get_object.
        """
        config = PipelineConfig(
            field_name="EDFF_TEST",
            tile_ids=[102018211, 102018212, 102018213],
            data={"s3_no_sign_request": False},
            output={
                "catalog_warehouse": str(tmp_path / "warehouse"),
                "result_dir": str(tmp_path / "results"),
                "manifest_dir": str(tmp_path / "manifests"),
                "extraction_dir": str(tmp_path / "subtiles"),
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
            data={"s3_no_sign_request": False},
            output={
                "catalog_warehouse": str(tmp_path / "warehouse"),
                "result_dir": str(tmp_path / "results"),
                "manifest_dir": str(tmp_path / "manifests"),
                "extraction_dir": str(tmp_path / "subtiles"),
            },
            mock_shine=True,
        )

        stats = run_local_pipeline(config)

        assert stats["row_count"] == 16
        assert stats["tile_count"] == 1
        assert stats["completeness"] == 1.0


@pytest.mark.integration
class TestEndToEndWithFits:
    """Integration tests using mock FITS files with valid WCS headers."""

    def test_pipeline_with_quadrant_resolution(self, mock_s3_with_fits, tmp_path):
        """Full pipeline with WCS-based quadrant resolution and extraction.

        Uses mock_s3_with_fits which has real FITS files with valid WCS
        headers, so quadrant resolution succeeds and extraction runs.
        """
        config = PipelineConfig(
            field_name="EDFF_TEST",
            tile_ids=[102018211],
            data={"s3_no_sign_request": False},
            output={
                "catalog_warehouse": str(tmp_path / "warehouse"),
                "result_dir": str(tmp_path / "results"),
                "manifest_dir": str(tmp_path / "manifests"),
                "extraction_dir": str(tmp_path / "subtiles"),
            },
            mock_shine=True,
        )

        stats = run_local_pipeline(config)

        assert stats["row_count"] == 16
        assert stats["tile_count"] == 1
        assert stats["completeness"] == 1.0

    def test_extraction_creates_subtile_dirs(self, mock_s3_with_fits, tmp_path):
        """Verify that extraction creates the expected directory structure."""
        from constellation.discovery import build_observation_index
        from constellation.extractor import extract_subtile
        from constellation.manifest import write_manifests_for_tile
        from constellation.quadrant_resolver import build_quadrant_index

        config = PipelineConfig(
            field_name="EDFF_TEST",
            tile_ids=[102018211],
            data={"s3_no_sign_request": False},
            output={
                "catalog_warehouse": str(tmp_path / "warehouse"),
                "result_dir": str(tmp_path / "results"),
                "manifest_dir": str(tmp_path / "manifests"),
                "extraction_dir": str(tmp_path / "subtiles"),
            },
            mock_shine=True,
        )

        obs_index = build_observation_index(
            vis_base_uri=config.data.vis_base_uri,
            s3_region=config.data.s3_region,
            s3_no_sign_request=config.data.s3_no_sign_request,
        )

        quadrant_index = build_quadrant_index(obs_index, s3_anon=False)
        assert len(quadrant_index) > 0

        manifest_paths = write_manifests_for_tile(
            102018211, config, obs_index, quadrant_index=quadrant_index
        )
        assert len(manifest_paths) == 16

        # Extract first sub-tile
        subtile_dir = extract_subtile(
            manifest_paths[0],
            extraction_dir=config.output.extraction_dir,
            s3_anon=False,
        )

        subtile_path = Path(subtile_dir)
        assert subtile_path.exists()
        assert (subtile_path / "manifest.yaml").exists()
        assert (subtile_path / "catalog.fits").exists()
        assert (subtile_path / "exposures").is_dir()
        assert (subtile_path / "psf").is_dir()
