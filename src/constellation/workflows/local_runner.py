"""Local sequential pipeline runner — no Flyte cluster required.

Exercises the same code paths as the Flyte workflow, but runs
everything in a single process with sequential for-loops.
"""

from __future__ import annotations

import logging

from constellation.catalog_assembler import assemble_catalog, validate_catalog
from constellation.config import PipelineConfig
from constellation.discovery import build_observation_index
from constellation.extractor import extract_subtile
from constellation.manifest import write_manifests_for_tile
from constellation.mock_shine import run_mock_inference
from constellation.quadrant_resolver import build_quadrant_index
from constellation.result_writer import write_subtile_result

logger = logging.getLogger(__name__)


def run_local_pipeline(config: PipelineConfig) -> dict:
    """Run the full shear pipeline locally and sequentially.

    Args:
        config: Pipeline configuration.

    Returns:
        Validation statistics dict.
    """
    # Step 1: Build observation index
    logger.info("Building observation index from %s", config.data.vis_base_uri)
    obs_index = build_observation_index(
        vis_base_uri=config.data.vis_base_uri,
        s3_region=config.data.s3_region,
        s3_no_sign_request=config.data.s3_no_sign_request,
    )
    logger.info(
        "Found %d observations", len(obs_index.obs_ids())
    )

    # Step 2: Build quadrant spatial index from FITS headers
    logger.info("Building quadrant index from FITS WCS headers")
    quadrant_index = None
    try:
        idx = build_quadrant_index(
            obs_index,
            s3_anon=config.data.s3_no_sign_request,
        )
        if idx:
            quadrant_index = idx
            logger.info(
                "Built quadrant index with %d footprints", len(quadrant_index)
            )
        else:
            logger.warning(
                "Quadrant index is empty, falling back to legacy mode"
            )
    except Exception:
        logger.warning(
            "Failed to build quadrant index, falling back to legacy mode",
            exc_info=True,
        )

    # Step 3: Prepare manifests for all tiles
    all_manifest_paths: list[str] = []
    for tile_id in config.tile_ids:
        logger.info("Preparing tile %d", tile_id)
        manifest_paths = write_manifests_for_tile(
            tile_id, config, obs_index, quadrant_index=quadrant_index
        )
        all_manifest_paths.extend(manifest_paths)

    logger.info("Generated %d manifests total", len(all_manifest_paths))

    # Step 4: Extract sub-tile data (if quadrant index was built)
    if quadrant_index is not None:
        logger.info("Extracting sub-tile data")
        for i, manifest_path in enumerate(all_manifest_paths):
            extract_subtile(
                manifest_path,
                extraction_dir=config.output.extraction_dir,
                s3_anon=config.data.s3_no_sign_request,
            )
            if (i + 1) % 50 == 0 or (i + 1) == len(all_manifest_paths):
                logger.info(
                    "Extracted %d / %d sub-tiles",
                    i + 1,
                    len(all_manifest_paths),
                )

    # Step 5: Run inference on each sub-tile
    result_paths: list[str] = []
    for i, manifest_path in enumerate(all_manifest_paths):
        if config.mock_shine:
            result = run_mock_inference(manifest_path)
        else:
            raise NotImplementedError(
                "Real SHINE inference not yet integrated. "
                "Set mock_shine=true in config."
            )

        path = write_subtile_result(result, config.output.result_dir)
        result_paths.append(path)

        if (i + 1) % 50 == 0 or (i + 1) == len(all_manifest_paths):
            logger.info("Inferred %d / %d sub-tiles", i + 1, len(all_manifest_paths))

    # Step 6: Assemble catalog
    logger.info("Assembling catalog from %d results", len(result_paths))
    n_rows = assemble_catalog(
        result_paths,
        warehouse=config.output.catalog_warehouse,
        namespace=config.output.catalog_namespace,
        table_name=config.output.catalog_table,
    )
    logger.info("Assembled %d rows into Iceberg catalog", n_rows)

    # Step 7: Validate
    rows, cols = config.tiling.sub_tile_grid
    expected = len(config.tile_ids) * rows * cols
    stats = validate_catalog(
        warehouse=config.output.catalog_warehouse,
        namespace=config.output.catalog_namespace,
        table_name=config.output.catalog_table,
        expected_subtiles=expected,
    )

    logger.info(
        "Validation: %d rows, %d tiles, completeness=%.2f",
        stats["row_count"],
        stats["tile_count"],
        stats["completeness"],
    )

    return stats
