"""Tests for constellation.schemas."""

from constellation.schemas import (
    SHEAR_CATALOG_SCHEMA,
    QuadrantRef,
    SkyBounds,
    SubTileManifest,
    SubTileResult,
)


class TestSkyBounds:
    def test_creation(self, sample_sky_bounds):
        assert sample_sky_bounds.core_ra[0] < sample_sky_bounds.core_ra[1]
        assert sample_sky_bounds.extended_ra[0] < sample_sky_bounds.core_ra[0]
        assert sample_sky_bounds.extended_ra[1] > sample_sky_bounds.core_ra[1]


class TestQuadrantRef:
    def test_fields(self, sample_quadrant_ref):
        assert "DET" in sample_quadrant_ref.sci_path
        assert "BKG" in sample_quadrant_ref.bkg_path
        assert "WGT" in sample_quadrant_ref.wgt_path
        assert "PSF" in sample_quadrant_ref.psf_path
        assert sample_quadrant_ref.quadrant == "3-4.F"


class TestSubTileManifest:
    def test_yaml_round_trip(self, sample_manifest, tmp_path):
        path = tmp_path / "manifest.yaml"
        sample_manifest.to_yaml(path)
        loaded = SubTileManifest.from_yaml(path)
        assert loaded.tile_id == sample_manifest.tile_id
        assert loaded.sub_tile_row == sample_manifest.sub_tile_row
        assert loaded.sub_tile_col == sample_manifest.sub_tile_col
        assert loaded.sky_bounds == sample_manifest.sky_bounds
        assert len(loaded.quadrants) == len(sample_manifest.quadrants)
        assert loaded.quadrants[0].quadrant == "3-4.F"
        assert loaded.source_ids == sample_manifest.source_ids
        assert loaded.core_source_ids == sample_manifest.core_source_ids

    def test_core_ids_subset_of_source_ids(self, sample_manifest):
        assert set(sample_manifest.core_source_ids).issubset(
            set(sample_manifest.source_ids)
        )

    def test_creates_parent_dirs(self, sample_manifest, tmp_path):
        path = tmp_path / "deep" / "nested" / "manifest.yaml"
        sample_manifest.to_yaml(path)
        assert path.exists()


class TestSubTileResult:
    def test_fields(self, sample_result):
        assert sample_result.METHOD == "mock"
        assert -1 < sample_result.G1 < 1
        assert sample_result.N_SOURCES > 0


class TestShearCatalogSchema:
    def test_field_count(self):
        assert len(SHEAR_CATALOG_SCHEMA) == 12

    def test_expected_fields(self):
        names = {f.name for f in SHEAR_CATALOG_SCHEMA}
        expected = {
            "TILE_ID", "SUBTILE_ROW", "SUBTILE_COL",
            "RA", "DEC", "G1", "G2", "G1_ERR", "G2_ERR",
            "N_SOURCES", "METHOD", "CONVERGENCE",
        }
        assert names == expected
