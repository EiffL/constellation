"""Tests for constellation.config."""

from pathlib import Path

import pytest

from constellation.config import PipelineConfig, TilingConfig


class TestTilingConfig:
    def test_defaults(self):
        cfg = TilingConfig()
        assert cfg.sub_tile_grid == (4, 4)
        assert cfg.sub_tile_margin_arcmin == 1.0

    def test_rejects_negative_margin(self):
        with pytest.raises(ValueError, match="margin must be positive"):
            TilingConfig(sub_tile_margin_arcmin=-1.0)

    def test_rejects_zero_grid(self):
        with pytest.raises(ValueError, match="grid dimensions must be positive"):
            TilingConfig(sub_tile_grid=(0, 4))


class TestPipelineConfig:
    def test_rejects_empty_tile_ids(self):
        with pytest.raises(ValueError, match="tile_ids must not be empty"):
            PipelineConfig(
                field_name="test",
                tile_ids=[],
                output={"catalog_warehouse": "/tmp/wh"},
            )

    def test_yaml_round_trip(self, sample_config, tmp_path):
        path = tmp_path / "config.yaml"
        sample_config.to_yaml(path)
        loaded = PipelineConfig.from_yaml(path)
        assert loaded.field_name == sample_config.field_name
        assert loaded.tile_ids == sample_config.tile_ids
        assert loaded.tiling.sub_tile_grid == sample_config.tiling.sub_tile_grid
        assert loaded.mock_shine == sample_config.mock_shine

    def test_from_edff_config(self):
        cfg = PipelineConfig.from_yaml("configs/edff_mock.yaml")
        assert cfg.field_name == "EDFF"
        assert len(cfg.tile_ids) == 48
        assert cfg.mock_shine is True
        assert cfg.tiling.sub_tile_grid == (4, 4)

    def test_shine_passthrough_preserved(self, sample_config, tmp_path):
        sample_config.inference = {"method": "map", "rng_seed": 42}
        sample_config.gal = {"type": "Exponential"}
        path = tmp_path / "config.yaml"
        sample_config.to_yaml(path)
        loaded = PipelineConfig.from_yaml(path)
        assert loaded.inference["method"] == "map"
        assert loaded.gal["type"] == "Exponential"
