"""Flyte workflow definition for the constellation shear pipeline.

Orchestrates: observation index -> quadrant index (fan-out via map_task)
-> tile preparation -> per-sub-tile extraction -> inference -> catalog assembly.

Uses map_task for embarrassingly parallel quadrant header reads, and
nested @dynamic for per-sub-tile extract→infer pipelines with pairwise
data dependencies.

Large intermediate data (quadrant footprints) is passed via FlyteFile
(blob storage) rather than inline protobuf to stay under Flyte's 2 MB
metadata limit.

Note on @dynamic nesting: Inside a @dynamic, task calls return Promises
(not resolved values). You cannot iterate over a Promise of list[...].
Only the *inputs* to a @dynamic are resolved. So we split into two
levels: process_tiles iterates over tile_ids (resolved input), and
process_subtiles iterates over manifest_paths (resolved input from
the outer @dynamic passing it as a parameter).
"""

from __future__ import annotations

from flytekit import dynamic, map_task, workflow
from flytekit.types.file import FlyteFile

from constellation.workflows.tasks import (
    assemble_results,
    build_det_work_items,
    build_obs_index,
    extract_subtile_task,
    infer_subtile,
    merge_footprints,
    prepare_tile,
    read_det_footprints,
    resolve_run_id,
    validate_results,
)


@dynamic
def process_subtiles(
    manifest_paths: list[FlyteFile],
    config_yaml: str,
    run_id: str = "",
) -> list[FlyteFile]:
    """Fan out extract→infer for each sub-tile of one tile.

    manifest_paths is resolved here (it's an input to the @dynamic),
    so we can iterate over it. Each sub-tile gets independent
    extract + infer pods, with infer waiting on its matching
    extraction via the extracted_dir data dependency.
    """
    result_paths: list[FlyteFile] = []
    for manifest_path in manifest_paths:
        extracted_dir = extract_subtile_task(
            manifest_path=manifest_path,
            config_yaml=config_yaml,
            run_id=run_id,
        )
        result_path = infer_subtile(
            manifest_path=manifest_path,
            config_yaml=config_yaml,
            run_id=run_id,
            extracted_dir=extracted_dir,
        )
        result_paths.append(result_path)
    return result_paths


@dynamic
def process_tiles(
    tile_ids: list[int],
    config_yaml: str,
    obs_index_dict: dict,
    quadrant_index_file: FlyteFile,
    run_id: str = "",
) -> list[FlyteFile]:
    """Process all tiles: prepare, then fan out per-sub-tile.

    tile_ids is resolved (input to @dynamic), so we can iterate.
    prepare_tile returns a Promise, so we pass it to
    process_subtiles (another @dynamic) which receives it as a
    resolved input.
    """
    all_result_paths: list[FlyteFile] = []
    for tile_id in tile_ids:
        manifest_paths = prepare_tile(
            tile_id=tile_id,
            config_yaml=config_yaml,
            obs_index_dict=obs_index_dict,
            quadrant_index_file=quadrant_index_file,
            run_id=run_id,
        )

        # Pass the Promise to a nested @dynamic where it gets resolved
        tile_results = process_subtiles(
            manifest_paths=manifest_paths,
            config_yaml=config_yaml,
            run_id=run_id,
        )
        all_result_paths.extend(tile_results)

    return all_result_paths


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

    # Step 2: Build quadrant spatial index — fan out via map_task
    work_items = build_det_work_items(
        config_yaml=config_yaml,
        obs_index_dict=obs_index_dict,
    )
    footprint_files = map_task(read_det_footprints)(work_item=work_items)
    quadrant_index_file = merge_footprints(footprint_files=footprint_files)

    # Steps 3-5: Process all tiles (dynamic — fan-out per sub-tile)
    all_result_paths = process_tiles(
        tile_ids=tile_ids,
        config_yaml=config_yaml,
        obs_index_dict=obs_index_dict,
        quadrant_index_file=quadrant_index_file,
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
