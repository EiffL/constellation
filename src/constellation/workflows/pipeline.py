"""Flyte workflow definition for the constellation shear pipeline.

Orchestrates: observation index -> quadrant index -> tile preparation
-> extraction -> sub-tile inference -> catalog assembly.

Uses @dynamic for the per-tile loop since Flyte workflows cannot
iterate over Promise inputs directly.
"""

from __future__ import annotations

try:
    from flytekit import dynamic, workflow
except Exception:

    def workflow(fn):
        return fn

    def dynamic(fn=None, **kwargs):
        if fn is not None:
            return fn
        return lambda f: f


from constellation.workflows.tasks import (
    assemble_results,
    build_obs_index,
    build_quadrant_index_task,
    extract_tile_task,
    infer_subtile,
    prepare_tile,
    validate_results,
)


@dynamic
def process_tiles(
    tile_ids: list[int],
    config_yaml: str,
    obs_index_dict: dict,
    quadrant_index_dict: list[dict],
) -> list[str]:
    """Process all tiles: prepare, extract, infer. Runs as @dynamic so we can iterate."""
    all_result_paths: list[str] = []
    for tile_id in tile_ids:
        manifest_paths = prepare_tile(
            tile_id=tile_id,
            config_yaml=config_yaml,
            obs_index_dict=obs_index_dict,
            quadrant_index_dict=quadrant_index_dict,
        )

        extract_tile_task(
            tile_id=tile_id,
            config_yaml=config_yaml,
            manifest_paths=manifest_paths,
        )

        result_paths = infer_tile(
            tile_id=tile_id,
            config_yaml=config_yaml,
            manifest_paths=manifest_paths,
        )
        all_result_paths.extend(result_paths)

    return all_result_paths


@dynamic
def infer_tile(
    tile_id: int,
    config_yaml: str,
    manifest_paths: list[str],
) -> list[str]:
    """Infer all sub-tiles of one tile. @dynamic so we can iterate over manifest_paths."""
    result_paths: list[str] = []
    for manifest_path in manifest_paths:
        result_path = infer_subtile(
            manifest_path=manifest_path,
            config_yaml=config_yaml,
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
    )

    # Step 6: Assemble all results into Iceberg catalog
    n_rows = assemble_results(
        result_paths=all_result_paths,
        config_yaml=config_yaml,
    )

    # Step 7: Validate
    stats = validate_results(
        config_yaml=config_yaml,
        expected_subtiles=0,
    )

    return stats
