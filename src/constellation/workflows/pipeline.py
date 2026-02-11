"""Flyte workflow definition for the constellation shear pipeline.

Orchestrates: observation index -> tile preparation -> sub-tile inference -> catalog assembly.
"""

from __future__ import annotations

try:
    from flytekit import workflow
except Exception:
    def workflow(fn):
        return fn

from constellation.workflows.tasks import (
    assemble_results,
    build_obs_index,
    infer_subtile,
    prepare_tile,
    validate_results,
)


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

    # Step 2 & 3: For each tile, prepare manifests then run inference
    all_result_paths: list[str] = []
    for tile_id in tile_ids:
        manifest_paths = prepare_tile(
            tile_id=tile_id,
            config_yaml=config_yaml,
            obs_index_dict=obs_index_dict,
        )

        for manifest_path in manifest_paths:
            result_path = infer_subtile(
                manifest_path=manifest_path,
                config_yaml=config_yaml,
            )
            all_result_paths.append(result_path)

    # Step 4: Assemble all results into Iceberg catalog
    n_rows = assemble_results(
        result_paths=all_result_paths,
        config_yaml=config_yaml,
    )

    # Step 5: Validate
    expected = len(tile_ids) * 16  # 4x4 grid default
    stats = validate_results(
        config_yaml=config_yaml,
        expected_subtiles=expected,
    )

    return stats
