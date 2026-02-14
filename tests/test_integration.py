"""Integration tests: end-to-end pipeline stages with moto S3.

Tests call Flyte task functions directly (they're callable as plain Python).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from constellation.config import PipelineConfig
from constellation.workflows.tasks import (
    build_obs_index,
    prepare_and_extract_tile,
)


@pytest.mark.integration
class TestEndToEnd:
    def test_prepare_and_extract_three_tiles(self, mock_s3_with_fits, tmp_path):
        """Run prepare_and_extract_tile for 3 tiles using moto-mocked S3.

        Expected: each tile produces 16 sub-tile directories.
        Uses mock FITS files with valid WCS headers.
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
        config_content = config.to_yaml_content()

        # Build observation index
        obs_dict = build_obs_index(config_content=config_content)

        # Write an empty quadrant index file (no spatial filtering)
        import json

        qi_path = tmp_path / "quadrant_index.json"
        qi_path.write_text(json.dumps([]))

        # Prepare and extract each tile
        for tile_id in config.tile_ids:
            result = prepare_and_extract_tile(
                tile_id=tile_id,
                config_content=config_content,
                obs_index_dict=obs_dict,
                quadrant_index_file=str(qi_path),
            )
            assert result["tile_id"] == tile_id
            assert result["n_subtiles"] == 16

    def test_prepare_and_extract_single_tile(self, mock_s3_with_fits, tmp_path):
        """Single tile produces 16 sub-tile directories."""
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
        config_content = config.to_yaml_content()

        obs_dict = build_obs_index(config_content=config_content)

        import json

        qi_path = tmp_path / "quadrant_index.json"
        qi_path.write_text(json.dumps([]))

        result = prepare_and_extract_tile(
            tile_id=102018211,
            config_content=config_content,
            obs_index_dict=obs_dict,
            quadrant_index_file=str(qi_path),
        )
        assert result["tile_id"] == 102018211
        assert result["n_subtiles"] == 16
        assert len(result["subtile_dirs"]) == 16


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
