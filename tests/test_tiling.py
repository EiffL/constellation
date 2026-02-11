"""Tests for constellation.tiling."""

import math

import pytest

from constellation.tiling import (
    TileFootprint,
    generate_subtile_grid,
    get_edff_tiles,
    get_tile_by_id,
    make_tile_footprint,
)


class TestMakeTileFootprint:
    def test_basic(self):
        tile = make_tile_footprint(123, 53.0, -28.0)
        assert tile.tile_id == 123
        assert tile.center_ra == 53.0
        assert tile.center_dec == -28.0

    def test_core_smaller_than_extended(self):
        tile = make_tile_footprint(123, 53.0, -28.0)
        assert tile.extended_ra_range[0] < tile.core_ra_range[0]
        assert tile.extended_ra_range[1] > tile.core_ra_range[1]
        assert tile.extended_dec_range[0] < tile.core_dec_range[0]
        assert tile.extended_dec_range[1] > tile.core_dec_range[1]

    def test_cos_dec_correction(self):
        """RA interval should be wider at higher |dec| due to cos(dec)."""
        tile_equator = make_tile_footprint(1, 53.0, 0.0)
        tile_edff = make_tile_footprint(2, 53.0, -28.0)
        ra_span_eq = tile_equator.core_ra_range[1] - tile_equator.core_ra_range[0]
        ra_span_edff = tile_edff.core_ra_range[1] - tile_edff.core_ra_range[0]
        # At dec=-28, RA span should be wider by 1/cos(28°) ≈ 1.133
        ratio = ra_span_edff / ra_span_eq
        expected = 1.0 / math.cos(math.radians(28.0))
        assert abs(ratio - expected) < 0.001


class TestGenerateSubtileGrid:
    def test_4x4_produces_16_subtiles(self):
        tile = make_tile_footprint(123, 53.0, -28.0)
        subtiles = generate_subtile_grid(tile, 4, 4, 1.0)
        assert len(subtiles) == 16

    def test_2x2_produces_4_subtiles(self):
        tile = make_tile_footprint(123, 53.0, -28.0)
        subtiles = generate_subtile_grid(tile, 2, 2, 1.0)
        assert len(subtiles) == 4

    def test_subtile_cores_tile_parent(self):
        """Sub-tile core areas should tile the parent's core area exactly."""
        tile = make_tile_footprint(123, 53.0, -28.0)
        subtiles = generate_subtile_grid(tile, 4, 4, 1.0)

        # Check that the union of sub-tile cores covers the parent core
        min_ra = min(s.core_ra_range[0] for s in subtiles)
        max_ra = max(s.core_ra_range[1] for s in subtiles)
        min_dec = min(s.core_dec_range[0] for s in subtiles)
        max_dec = max(s.core_dec_range[1] for s in subtiles)

        assert abs(min_ra - tile.core_ra_range[0]) < 1e-10
        assert abs(max_ra - tile.core_ra_range[1]) < 1e-10
        assert abs(min_dec - tile.core_dec_range[0]) < 1e-10
        assert abs(max_dec - tile.core_dec_range[1]) < 1e-10

    def test_adjacent_extended_areas_overlap(self):
        """Adjacent sub-tiles' extended areas should overlap."""
        tile = make_tile_footprint(123, 53.0, -28.0)
        subtiles = generate_subtile_grid(tile, 4, 4, 1.0)

        # Sub-tile (0,0) and (0,1) should overlap in RA
        s00 = [s for s in subtiles if s.row == 0 and s.col == 0][0]
        s01 = [s for s in subtiles if s.row == 0 and s.col == 1][0]
        assert s00.extended_ra_range[1] > s01.extended_ra_range[0]

    def test_row_col_ordering(self):
        tile = make_tile_footprint(123, 53.0, -28.0)
        subtiles = generate_subtile_grid(tile, 4, 4, 1.0)
        # Row 0 should have lowest dec
        row0 = [s for s in subtiles if s.row == 0]
        row3 = [s for s in subtiles if s.row == 3]
        assert row0[0].center_dec < row3[0].center_dec


class TestEdffCatalog:
    def test_get_edff_tiles_count(self):
        tiles = get_edff_tiles()
        assert len(tiles) == 48

    def test_get_tile_by_id(self):
        tile = get_tile_by_id(102018211)
        assert tile.tile_id == 102018211

    def test_get_tile_by_id_not_found(self):
        with pytest.raises(KeyError):
            get_tile_by_id(999999999)
