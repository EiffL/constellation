"""Flyte workflow definition for the constellation data-preparation pipeline.

Orchestrates: observation index -> quadrant index (fan-out via map_task)
-> per-tile manifest generation + sub-tile extraction (single @dynamic).

Each tile runs as a single ``prepare_and_extract_tile`` task, which
downloads FITS files once and extracts all sub-tiles from local data.
This avoids the catastrophic N-times re-download of the old per-sub-tile
fan-out.

Large intermediate data (quadrant footprints) is passed via FlyteFile
(blob storage) rather than inline protobuf to stay under Flyte's 2 MB
metadata limit.
"""

from __future__ import annotations

from flytekit import WorkflowFailurePolicy, dynamic, map_task, workflow
from flytekit.types.file import FlyteFile

from constellation.workflows.tasks import (
    build_config,
    build_det_work_items,
    build_obs_index,
    merge_footprints,
    prepare_and_extract_tile,
    read_det_footprints,
    resolve_run_id,
)


@dynamic
def prepare_and_extract_all_tiles(
    tile_ids: list[int],
    config_content: str,
    obs_index_dict: dict,
    quadrant_index_file: FlyteFile,
    run_id: str = "",
) -> list[dict]:
    """Fan out prepare_and_extract_tile for each MER tile.

    tile_ids is resolved (input to @dynamic), so we can iterate.
    Each tile gets a single pod that downloads FITS once and
    extracts all sub-tiles.
    """
    results: list[dict] = []
    for tile_id in tile_ids:
        result = prepare_and_extract_tile(
            tile_id=tile_id,
            config_content=config_content,
            obs_index_dict=obs_index_dict,
            quadrant_index_file=quadrant_index_file,
            run_id=run_id,
        )
        results.append(result)
    return results


@workflow(failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE)
def data_preparation_pipeline(
    config_yaml: str,
    tile_ids: list[int],
    storage_base_uri: str = "",
    sub_tile_grid: list[int] = [],
) -> list[dict]:
    """Data-preparation pipeline: build quadrant index, then prepare and extract tiles.

    Args:
        config_yaml: Path to the pipeline config YAML (baked into Docker image).
        tile_ids: List of MER tile IDs to process.
        storage_base_uri: Override for output.storage_base_uri (pass at launch time).
        sub_tile_grid: Override for tiling.sub_tile_grid as [rows, cols].

    Returns:
        List of per-tile summary dicts with tile_id, n_subtiles, subtile_dirs.
    """
    # Step 0: Merge base config (Docker image) with workflow-level overrides
    config_content = build_config(
        config_yaml=config_yaml,
        storage_base_uri=storage_base_uri,
        sub_tile_grid=sub_tile_grid,
    )

    # Step 1: Resolve run ID (Flyte execution ID or timestamp fallback)
    run_id = resolve_run_id(config_yaml=config_yaml)

    # Step 2: Build observation index once (shared across all tiles)
    obs_index_dict = build_obs_index(config_content=config_content)

    # Step 3: Build quadrant spatial index — fan out via map_task
    work_items = build_det_work_items(
        config_content=config_content,
        obs_index_dict=obs_index_dict,
    )
    footprint_files = map_task(read_det_footprints)(work_item=work_items)
    quadrant_index_file = merge_footprints(footprint_files=footprint_files)

    # Step 4: Prepare and extract all tiles (one pod per tile)
    return prepare_and_extract_all_tiles(
        tile_ids=tile_ids,
        config_content=config_content,
        obs_index_dict=obs_index_dict,
        quadrant_index_file=quadrant_index_file,
        run_id=run_id,
    )
