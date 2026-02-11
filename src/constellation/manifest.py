"""Manifest generation: assembles per-sub-tile manifest YAML files.

Orchestrates tiling, discovery, and source partitioning to produce
one manifest per sub-tile. Each manifest is the contract between
constellation and SHINE.
"""

from __future__ import annotations

import logging
from pathlib import Path

from constellation.config import PipelineConfig
from constellation.discovery import (
    ObservationIndex,
    build_observation_index,
    build_quadrant_refs,
    list_mer_catalog,
)
from constellation.schemas import QuadrantRef, SkyBounds, SubTileManifest
from constellation.tiling import SubTile, generate_subtile_grid, get_tile_by_id

logger = logging.getLogger(__name__)


def generate_mock_source_ids(
    subtile: SubTile,
    n_sources: int = 2500,
    core_fraction: float = 0.64,
) -> tuple[list[int], list[int]]:
    """Generate deterministic synthetic source IDs for mock mode.

    The core fraction is approximately (8/10)^2 = 0.64 (ratio of core
    area to extended area).

    Returns:
        (all_source_ids, core_source_ids) where core is a subset of all.
    """
    base = subtile.tile_id * 100 + subtile.row * 10 + subtile.col
    base_id = base * 10000
    all_ids = list(range(base_id, base_id + n_sources))
    n_core = int(n_sources * core_fraction)
    core_ids = all_ids[:n_core]
    return all_ids, core_ids


def _subtile_to_sky_bounds(subtile: SubTile) -> SkyBounds:
    """Convert a SubTile to SkyBounds."""
    return SkyBounds(
        core_ra=subtile.core_ra_range,
        core_dec=subtile.core_dec_range,
        extended_ra=subtile.extended_ra_range,
        extended_dec=subtile.extended_dec_range,
    )


def generate_manifest(
    subtile: SubTile,
    quadrants: list[QuadrantRef],
    catalog_path: str,
    source_ids: list[int],
    core_source_ids: list[int],
) -> SubTileManifest:
    """Build a SubTileManifest for one sub-tile."""
    return SubTileManifest(
        tile_id=subtile.tile_id,
        sub_tile_row=subtile.row,
        sub_tile_col=subtile.col,
        sky_bounds=_subtile_to_sky_bounds(subtile),
        quadrants=quadrants,
        source_catalog=catalog_path,
        source_ids=source_ids,
        core_source_ids=core_source_ids,
    )


def write_manifests_for_tile(
    tile_id: int,
    config: PipelineConfig,
    obs_index: ObservationIndex | None = None,
) -> list[str]:
    """Generate and write all sub-tile manifests for one MER tile.

    Args:
        tile_id: MER tile ID to process.
        config: Pipeline configuration.
        obs_index: Pre-built observation index. If None, built from S3.

    Returns:
        List of manifest file paths written.
    """
    tile = get_tile_by_id(tile_id)
    rows, cols = config.tiling.sub_tile_grid
    margin = config.tiling.sub_tile_margin_arcmin
    subtiles = generate_subtile_grid(tile, rows, cols, margin)

    # Build observation index if not provided
    if obs_index is None:
        obs_index = build_observation_index(
            vis_base_uri=config.data.vis_base_uri,
            s3_region=config.data.s3_region,
            s3_no_sign_request=config.data.s3_no_sign_request,
        )

    # Find MER catalog for this tile
    catalog_path = list_mer_catalog(
        tile_id,
        catalog_base_uri=config.data.catalog_base_uri,
        s3_region=config.data.s3_region,
        s3_no_sign_request=config.data.s3_no_sign_request,
    )
    if catalog_path is None:
        catalog_path = ""
        logger.warning("No MER catalog found for tile %d", tile_id)

    # For the milestone: assign all observations to all sub-tiles.
    # Production would check WCS sky overlap per quadrant.
    all_quadrant_refs: list[QuadrantRef] = []
    for obs_id in obs_index.obs_ids():
        all_quadrant_refs.extend(build_quadrant_refs(obs_index, obs_id))

    manifest_paths = []
    manifest_dir = Path(config.output.manifest_dir) / str(tile_id)

    for subtile in subtiles:
        source_ids, core_source_ids = generate_mock_source_ids(subtile)

        manifest = generate_manifest(
            subtile=subtile,
            quadrants=all_quadrant_refs,
            catalog_path=catalog_path,
            source_ids=source_ids,
            core_source_ids=core_source_ids,
        )

        path = manifest_dir / f"{tile_id}_{subtile.row}_{subtile.col}.yaml"
        manifest.to_yaml(path)
        manifest_paths.append(str(path))

    logger.info(
        "Wrote %d manifests for tile %d", len(manifest_paths), tile_id
    )
    return manifest_paths
