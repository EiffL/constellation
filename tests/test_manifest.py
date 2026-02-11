"""Tests for constellation.manifest."""

from constellation.manifest import (
    generate_manifest,
    generate_mock_source_ids,
    write_manifests_for_tile,
)
from constellation.schemas import QuadrantRef, SubTileManifest
from constellation.tiling import generate_subtile_grid, make_tile_footprint
from tests.conftest import MOCK_BUCKET, MOCK_REGION


class TestGenerateMockSourceIds:
    def test_returns_correct_counts(self):
        from constellation.tiling import SubTile

        subtile = SubTile(
            tile_id=102018211, row=0, col=0,
            core_ra_range=(52.8, 52.9), core_dec_range=(-28.2, -28.1),
            extended_ra_range=(52.78, 52.92), extended_dec_range=(-28.22, -28.08),
            center_ra=52.85, center_dec=-28.15,
        )
        all_ids, core_ids = generate_mock_source_ids(subtile, n_sources=2500)
        assert len(all_ids) == 2500
        assert len(core_ids) == int(2500 * 0.64)

    def test_core_is_subset(self):
        from constellation.tiling import SubTile

        subtile = SubTile(
            tile_id=102018211, row=1, col=2,
            core_ra_range=(52.8, 52.9), core_dec_range=(-28.2, -28.1),
            extended_ra_range=(52.78, 52.92), extended_dec_range=(-28.22, -28.08),
            center_ra=52.85, center_dec=-28.15,
        )
        all_ids, core_ids = generate_mock_source_ids(subtile)
        assert set(core_ids).issubset(set(all_ids))

    def test_deterministic(self):
        from constellation.tiling import SubTile

        subtile = SubTile(
            tile_id=102018211, row=0, col=0,
            core_ra_range=(52.8, 52.9), core_dec_range=(-28.2, -28.1),
            extended_ra_range=(52.78, 52.92), extended_dec_range=(-28.22, -28.08),
            center_ra=52.85, center_dec=-28.15,
        )
        ids1, _ = generate_mock_source_ids(subtile)
        ids2, _ = generate_mock_source_ids(subtile)
        assert ids1 == ids2


class TestGenerateManifest:
    def test_creates_manifest(self, sample_sky_bounds, sample_quadrant_ref):
        from constellation.tiling import SubTile

        subtile = SubTile(
            tile_id=102018211, row=0, col=0,
            core_ra_range=(52.8, 52.9), core_dec_range=(-28.2, -28.1),
            extended_ra_range=(52.78, 52.92), extended_dec_range=(-28.22, -28.08),
            center_ra=52.85, center_dec=-28.15,
        )
        manifest = generate_manifest(
            subtile, [sample_quadrant_ref], "s3://bucket/cat.fits",
            [1, 2, 3], [1, 2],
        )
        assert manifest.tile_id == 102018211
        assert len(manifest.quadrants) == 1
        assert len(manifest.source_ids) == 3
        assert len(manifest.core_source_ids) == 2


class TestWriteManifestsForTile:
    def test_writes_16_manifests(self, sample_config, mock_s3):
        # Override S3 config to use moto (not unsigned)
        sample_config.data.s3_no_sign_request = False
        sample_config.data.vis_base_uri = f"s3://{MOCK_BUCKET}/q1/VIS/"
        sample_config.data.catalog_base_uri = (
            f"s3://{MOCK_BUCKET}/q1/catalogs/MER_FINAL_CATALOG/"
        )

        paths = write_manifests_for_tile(102018211, sample_config)
        assert len(paths) == 16

        # Verify YAML round-trip
        loaded = SubTileManifest.from_yaml(paths[0])
        assert loaded.tile_id == 102018211
        assert len(loaded.source_ids) > 0
