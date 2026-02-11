"""MER tile footprints and sub-tile grid generation.

Handles the spatial decomposition of the Euclid sky into MER tiles and
sub-tiles. Each MER tile (32'×32') is subdivided into a grid of sub-tiles
(default 4×4 = 8'×8' core + 1' overlap margin).
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class TileFootprint:
    """MER tile with center coordinates and computed boundaries.

    All coordinates in degrees. RA ranges account for cos(dec) shrinkage.
    """

    tile_id: int
    center_ra: float
    center_dec: float
    core_ra_range: tuple[float, float]
    core_dec_range: tuple[float, float]
    extended_ra_range: tuple[float, float]
    extended_dec_range: tuple[float, float]


@dataclass(frozen=True)
class SubTile:
    """One sub-tile within a MER tile."""

    tile_id: int
    row: int
    col: int
    core_ra_range: tuple[float, float]
    core_dec_range: tuple[float, float]
    extended_ra_range: tuple[float, float]
    extended_dec_range: tuple[float, float]
    center_ra: float
    center_dec: float


def _arcmin_to_deg_ra(arcmin: float, dec_deg: float) -> float:
    """Convert arcminutes to RA degrees at a given declination.

    RA intervals shrink toward the poles: 1' of sky = 1'/(60*cos(dec)) degrees.
    """
    cos_dec = math.cos(math.radians(dec_deg))
    if cos_dec < 1e-6:
        raise ValueError(f"Cannot compute RA interval at dec={dec_deg}")
    return arcmin / (60.0 * cos_dec)


def _arcmin_to_deg_dec(arcmin: float) -> float:
    """Convert arcminutes to Dec degrees (no correction needed)."""
    return arcmin / 60.0


def make_tile_footprint(
    tile_id: int,
    center_ra: float,
    center_dec: float,
    core_size_arcmin: float = 30.0,
    extended_size_arcmin: float = 32.0,
) -> TileFootprint:
    """Construct a TileFootprint from center coordinates and sizes."""
    half_core_ra = _arcmin_to_deg_ra(core_size_arcmin / 2, center_dec)
    half_core_dec = _arcmin_to_deg_dec(core_size_arcmin / 2)
    half_ext_ra = _arcmin_to_deg_ra(extended_size_arcmin / 2, center_dec)
    half_ext_dec = _arcmin_to_deg_dec(extended_size_arcmin / 2)

    return TileFootprint(
        tile_id=tile_id,
        center_ra=center_ra,
        center_dec=center_dec,
        core_ra_range=(center_ra - half_core_ra, center_ra + half_core_ra),
        core_dec_range=(center_dec - half_core_dec, center_dec + half_core_dec),
        extended_ra_range=(center_ra - half_ext_ra, center_ra + half_ext_ra),
        extended_dec_range=(center_dec - half_ext_dec, center_dec + half_ext_dec),
    )


def generate_subtile_grid(
    tile: TileFootprint,
    grid_rows: int = 4,
    grid_cols: int = 4,
    margin_arcmin: float = 1.0,
) -> list[SubTile]:
    """Subdivide a MER tile into a grid of sub-tiles with overlap margins.

    Each sub-tile has a core area (no overlap between neighbors) and an
    extended area (core + margin on each side). The core areas tile the
    MER tile's core area exactly.

    Args:
        tile: The MER tile to subdivide.
        grid_rows: Number of sub-tile rows (Dec direction).
        grid_cols: Number of sub-tile columns (RA direction).
        margin_arcmin: Overlap margin per side in arcminutes.

    Returns:
        List of SubTile objects, ordered row-major (row 0 = lowest Dec).
    """
    # Core area spans of the parent tile
    tile_core_ra_min, tile_core_ra_max = tile.core_ra_range
    tile_core_dec_min, tile_core_dec_max = tile.core_dec_range

    # Sub-tile core size in degrees
    subtile_core_ra_span = (tile_core_ra_max - tile_core_ra_min) / grid_cols
    subtile_core_dec_span = (tile_core_dec_max - tile_core_dec_min) / grid_rows

    # Margin in degrees (using tile center dec for cos correction)
    margin_ra = _arcmin_to_deg_ra(margin_arcmin, tile.center_dec)
    margin_dec = _arcmin_to_deg_dec(margin_arcmin)

    subtiles = []
    for row in range(grid_rows):
        for col in range(grid_cols):
            core_ra_min = tile_core_ra_min + col * subtile_core_ra_span
            core_ra_max = core_ra_min + subtile_core_ra_span
            core_dec_min = tile_core_dec_min + row * subtile_core_dec_span
            core_dec_max = core_dec_min + subtile_core_dec_span

            subtiles.append(
                SubTile(
                    tile_id=tile.tile_id,
                    row=row,
                    col=col,
                    core_ra_range=(core_ra_min, core_ra_max),
                    core_dec_range=(core_dec_min, core_dec_max),
                    extended_ra_range=(
                        core_ra_min - margin_ra,
                        core_ra_max + margin_ra,
                    ),
                    extended_dec_range=(
                        core_dec_min - margin_dec,
                        core_dec_max + margin_dec,
                    ),
                    center_ra=(core_ra_min + core_ra_max) / 2,
                    center_dec=(core_dec_min + core_dec_max) / 2,
                )
            )

    return subtiles


# --- EDFF tile catalog (hardcoded for Q1 milestone) ---

# First 48 Q1 MER tile IDs used for the EDFF milestone.
# Center coordinates are approximate — computed from a regular grid
# covering the EDFF field (RA~53°, Dec~-28°).
_EDFF_TILE_IDS = [
    102018211, 102018212, 102018213, 102018664, 102018665, 102018666,
    102018667, 102018668, 102018669, 102018670, 102018671, 102019122,
    102019123, 102019124, 102019125, 102019126, 102019127, 102019128,
    102019129, 102019130, 102019131, 102019585, 102019586, 102019587,
    102019588, 102019589, 102019590, 102019591, 102019592, 102019593,
    102019594, 102019595, 102019596, 102020053, 102020054, 102020055,
    102020056, 102020057, 102020058, 102020059, 102020060, 102020061,
    102020062, 102020063, 102020064, 102020065, 102020066, 102020527,
]


def _edff_tile_centers() -> list[tuple[int, float, float]]:
    """Generate approximate tile centers for EDFF tiles.

    Lays out tiles in a regular grid covering the EDFF field
    (centered at RA~52.9°, Dec~-28.1°, ~12 deg²).
    Rows of ~8 tiles across RA, ~6 rows in Dec, spaced 0.5° apart.
    """
    base_ra = 50.5
    base_dec = -29.5
    spacing_dec = 0.5  # degrees between tile centers
    cos_dec = math.cos(math.radians(-28.0))
    spacing_ra = 0.5 / cos_dec  # corrected RA spacing

    centers = []
    idx = 0
    n_cols = 8
    for row in range(6):
        for col in range(n_cols):
            if idx >= len(_EDFF_TILE_IDS):
                break
            ra = base_ra + col * spacing_ra
            dec = base_dec + row * spacing_dec
            centers.append((_EDFF_TILE_IDS[idx], ra, dec))
            idx += 1
    return centers


def get_edff_tiles() -> list[TileFootprint]:
    """Return TileFootprint objects for the ~48 EDFF MER tiles."""
    return [
        make_tile_footprint(tile_id, ra, dec)
        for tile_id, ra, dec in _edff_tile_centers()
    ]


def get_tile_by_id(tile_id: int) -> TileFootprint:
    """Look up a single EDFF tile by ID.

    Raises:
        KeyError: If the tile_id is not in the EDFF catalog.
    """
    for tid, ra, dec in _edff_tile_centers():
        if tid == tile_id:
            return make_tile_footprint(tid, ra, dec)
    raise KeyError(f"Tile {tile_id} not found in EDFF catalog")
