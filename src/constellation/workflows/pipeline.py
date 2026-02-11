"""Flyte workflow definition for the constellation shear pipeline.

Orchestrates: observation index -> quadrant index -> tile preparation
-> extraction -> sub-tile inference -> catalog assembly.

Uses @dynamic for the per-tile loop since Flyte workflows cannot
iterate over Promise inputs directly.
"""

from __future__ import annotations

from flytekit import dynamic, workflow
from flytekit.types.file import FlyteFile

from constellation.workflows.tasks import (
    assemble_results,
    build_obs_index,
    build_quadrant_index_task,
    extract_tile_task,
    infer_subtile,
    prepare_tile,
    resolve_run_id,
    validate_results,
)


@dynamic
def process_tiles(
    tile_ids: list[int],
    config_yaml: str,
    obs_index_dict: dict,
    quadrant_index_dict: list[dict],
    run_id: str = "",
) -> list[FlyteFile]:
    """Process all tiles: prepare, extract, infer. Runs as @dynamic so we can iterate."""
    all_result_paths: list[FlyteFile] = []
    for tile_id in tile_ids:
        manifest_paths = prepare_tile(
            tile_id=tile_id,
            config_yaml=config_yaml,
            obs_index_dict=obs_index_dict,
            quadrant_index_dict=quadrant_index_dict,
            run_id=run_id,
        )

        extract_tile_task(
            tile_id=tile_id,
            config_yaml=config_yaml,
            manifest_paths=manifest_paths,
            run_id=run_id,
        )

        result_paths = infer_tile(
            tile_id=tile_id,
            config_yaml=config_yaml,
            manifest_paths=manifest_paths,
            run_id=run_id,
        )
        all_result_paths.extend(result_paths)

    return all_result_paths


@dynamic
def infer_tile(
    tile_id: int,
    config_yaml: str,
    manifest_paths: list[FlyteFile],
    run_id: str = "",
) -> list[FlyteFile]:
    """Infer all sub-tiles of one tile. @dynamic so we can iterate over manifest_paths."""
    result_paths: list[FlyteFile] = []
    for manifest_path in manifest_paths:
        result_path = infer_subtile(
            manifest_path=manifest_path,
            config_yaml=config_yaml,
            run_id=run_id,
        )
        result_paths.append(result_path)
    return result_paths


@workflow
def shear_pipeline(
    config_yaml: str,
    tile_ids: list[int],
) -> dict:
    """End-to-end shear inference pipeline for a set of MER tiles.

    Args:
        config_yaml: Path to the pipeline config YAML.
        tile_ids: List of MER tile IDs to process.

    Returns:
        Validation statistics dict.
    """
    # Step 0: Resolve run ID (Flyte execution ID or timestamp fallback)
    run_id = resolve_run_id(config_yaml=config_yaml)

    # Step 1: Build observation index once (shared across all tiles)
    obs_index_dict = build_obs_index(config_yaml=config_yaml)

    # Step 2: Build quadrant spatial index from FITS headers
    quadrant_index_dict = build_quadrant_index_task(
        config_yaml=config_yaml,
        obs_index_dict=obs_index_dict,
    )

    # Step 3, 4, 5: Process all tiles (dynamic — can iterate)
    all_result_paths = process_tiles(
        tile_ids=tile_ids,
        config_yaml=config_yaml,
        obs_index_dict=obs_index_dict,
        quadrant_index_dict=quadrant_index_dict,
        run_id=run_id,
    )

    # Step 6: Assemble all results into Iceberg catalog
    n_rows = assemble_results(
        result_paths=all_result_paths,
        config_yaml=config_yaml,
    )

    # Step 7: Validate (takes n_rows to create a data dependency on assembly)
    stats = validate_results(
        config_yaml=config_yaml,
        expected_subtiles=n_rows,
    )

    return stats
