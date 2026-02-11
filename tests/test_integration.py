"""Integration tests: end-to-end pipeline stages with moto S3.

Tests call Flyte task functions directly (they're callable as plain Python).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from constellation.config import PipelineConfig
from constellation.workflows.tasks import (
    assemble_results,
    build_obs_index,
    infer_subtile,
    prepare_tile,
    validate_results,
)


@pytest.mark.integration
class TestEndToEnd:
    def test_pipeline_stages_three_tiles(self, mock_s3, tmp_path):
        """Run pipeline stages sequentially with 3 tiles using moto-mocked S3.

        Expected: 3 tiles x 16 sub-tiles = 48 rows in Iceberg.
        Uses legacy mode (empty FITS files, no WCS headers).
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
        config_path = tmp_path / "config.yaml"
        config.to_yaml(config_path)
        config_yaml = str(config_path)

        # Build observation index
        obs_dict = build_obs_index(config_yaml=config_yaml)

        # Prepare manifests for all tiles
        all_manifest_paths: list[str] = []
        for tile_id in config.tile_ids:
            paths = prepare_tile(
                tile_id=tile_id,
                config_yaml=config_yaml,
                obs_index_dict=obs_dict,
            )
            all_manifest_paths.extend(paths)

        assert len(all_manifest_paths) == 48

        # Infer all sub-tiles
        result_paths: list[str] = []
        for mp in all_manifest_paths:
            rp = infer_subtile(manifest_path=mp, config_yaml=config_yaml)
            result_paths.append(rp)

        assert len(result_paths) == 48

        # Assemble and validate
        n_rows = assemble_results(
            result_paths=result_paths, config_yaml=config_yaml
        )
        assert n_rows == 48

        stats = validate_results(
            config_yaml=config_yaml, expected_subtiles=48
        )
        assert stats["row_count"] == 48
        assert stats["tile_count"] == 3
        assert stats["completeness"] == 1.0
        assert abs(stats["g1_mean"]) < 0.1

    def test_pipeline_stages_single_tile(self, mock_s3, tmp_path):
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
        config_path = tmp_path / "config.yaml"
        config.to_yaml(config_path)
        config_yaml = str(config_path)

        obs_dict = build_obs_index(config_yaml=config_yaml)
        manifest_paths = prepare_tile(
            tile_id=102018211,
            config_yaml=config_yaml,
            obs_index_dict=obs_dict,
        )
        assert len(manifest_paths) == 16

        result_paths = [
            infer_subtile(manifest_path=mp, config_yaml=config_yaml)
            for mp in manifest_paths
        ]

        n_rows = assemble_results(
            result_paths=result_paths, config_yaml=config_yaml
        )
        assert n_rows == 16

        stats = validate_results(
            config_yaml=config_yaml, expected_subtiles=16
        )
        assert stats["row_count"] == 16
        assert stats["tile_count"] == 1
        assert stats["completeness"] == 1.0


@pytest.mark.integration
class TestEndToEndWithFits:
    """Integration tests using mock FITS files with valid WCS headers."""

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
        assert (subtile_path / "manifest_local.yaml").exists()
        assert (subtile_path / "catalog.fits").exists()
        assert (subtile_path / "exposures").is_dir()
        assert (subtile_path / "psf").is_dir()
