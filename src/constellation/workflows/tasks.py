"""Flyte task definitions for the constellation pipeline.

Each task wraps a pure-Python function from the pipeline modules,
making them callable both as Flyte tasks and as regular functions.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from flytekit import task
from flytekit.types.file import FlyteFile

from constellation.catalog_assembler import assemble_catalog, validate_catalog
from constellation.config import PipelineConfig
from constellation.discovery import ObservationIndex, build_observation_index
from constellation.extractor import extract_subtile
from constellation.manifest import write_manifests_for_tile
from constellation.mock_shine import run_mock_inference
from constellation.quadrant_resolver import (
    build_quadrant_index,
    quadrant_index_from_dict,
    quadrant_index_to_dict,
)
from constellation.result_writer import write_subtile_result
from constellation.storage import (
    build_subtile_prefix,
    get_run_id,
    upload_directory,
    upload_file,
)

logger = logging.getLogger(__name__)


def _parse_subtile_from_manifest_path(manifest_path: str) -> tuple[int, int, int]:
    """Extract (tile_id, row, col) from a manifest filename.

    Expected pattern: ``.../{tile_id}_{row}_{col}.yaml``
    """
    stem = Path(manifest_path).stem  # e.g. "102018211_0_0"
    parts = stem.split("_")
    return int(parts[0]), int(parts[1]), int(parts[2])


@task
def resolve_run_id(config_yaml: str) -> str:
    """Resolve a run ID for this execution.

    Not cached — must run each execution to pick up a fresh Flyte
    execution ID or generate a new timestamp-based fallback.
    """
    config = PipelineConfig.from_yaml(config_yaml)
    return get_run_id(field_name=config.field_name)


@task(cache=True, cache_version="1")
def build_obs_index(config_yaml: str) -> dict:
    """Build the observation index from S3 listings.

    Returns the index serialized as a dict so Flyte can pass it between tasks.
    """
    config = PipelineConfig.from_yaml(config_yaml)
    obs_index = build_observation_index(
        vis_base_uri=config.data.vis_base_uri,
        s3_region=config.data.s3_region,
        s3_no_sign_request=config.data.s3_no_sign_request,
    )
    return obs_index.to_dict()


@task(cache=True, cache_version="1")
def build_quadrant_index_task(
    config_yaml: str,
    obs_index_dict: dict,
) -> list[dict]:
    """Build the quadrant spatial index by reading WCS from FITS headers.

    Args:
        config_yaml: Path to the pipeline config YAML.
        obs_index_dict: Serialized ObservationIndex from build_obs_index.

    Returns:
        Serialized quadrant index (list of dicts).
    """
    config = PipelineConfig.from_yaml(config_yaml)
    obs_index = ObservationIndex.from_dict(obs_index_dict)
    index = build_quadrant_index(
        obs_index,
        s3_anon=config.data.s3_no_sign_request,
    )
    return quadrant_index_to_dict(index)


@task(cache=True, cache_version="3")
def prepare_tile(
    tile_id: int,
    config_yaml: str,
    obs_index_dict: dict,
    quadrant_index_dict: list[dict] | None = None,
    run_id: str = "",
) -> list[FlyteFile]:
    """Generate manifests for all sub-tiles of one MER tile.

    Args:
        tile_id: MER tile ID.
        config_yaml: Path to the pipeline config YAML.
        obs_index_dict: Serialized ObservationIndex from build_obs_index.
        quadrant_index_dict: Serialized quadrant index. If provided,
            enables WCS-based spatial filtering.
        run_id: Pipeline run identifier for S3 output.

    Returns:
        List of manifest file paths.
    """
    config = PipelineConfig.from_yaml(config_yaml)
    obs_index = ObservationIndex.from_dict(obs_index_dict)

    quadrant_index = None
    if quadrant_index_dict is not None:
        quadrant_index = quadrant_index_from_dict(quadrant_index_dict)

    manifest_paths = write_manifests_for_tile(
        tile_id, config, obs_index, quadrant_index=quadrant_index
    )

    # Upload manifests to S3
    base = config.output.storage_base_uri
    if base and run_id:
        for mp in manifest_paths:
            tile_id_p, row, col = _parse_subtile_from_manifest_path(mp)
            prefix = build_subtile_prefix(base, run_id, tile_id_p, row, col)
            upload_file(mp, f"{prefix}/manifest.yaml")

    return manifest_paths


@task(cache=True, cache_version="2")
def extract_tile_task(
    tile_id: int,
    config_yaml: str,
    manifest_paths: list[FlyteFile],
    run_id: str = "",
) -> list[str]:
    """Extract quadrant FITS and catalog subsets for all sub-tiles of a tile.

    Downloads source FITS files, extracts relevant quadrant HDUs,
    subsets the MER catalog, and writes self-contained sub-tile
    directories with relative-path manifests.

    Args:
        tile_id: MER tile ID (for logging).
        config_yaml: Path to the pipeline config YAML.
        manifest_paths: List of manifest file paths to extract.
        run_id: Pipeline run identifier for S3 output.

    Returns:
        List of extracted sub-tile directory paths.
    """
    config = PipelineConfig.from_yaml(config_yaml)
    subtile_dirs: list[str] = []

    for manifest_path in manifest_paths:
        subtile_dir = extract_subtile(
            manifest_path,
            extraction_dir=config.output.extraction_dir,
            s3_anon=config.data.s3_no_sign_request,
        )
        subtile_dirs.append(subtile_dir)

    # Upload extracted directories to S3
    base = config.output.storage_base_uri
    if base and run_id:
        for i, subtile_dir in enumerate(subtile_dirs):
            mp = str(manifest_paths[i])
            tile_id_p, row, col = _parse_subtile_from_manifest_path(mp)
            prefix = build_subtile_prefix(base, run_id, tile_id_p, row, col)
            upload_directory(subtile_dir, prefix)

    logger.info(
        "Extracted %d sub-tiles for tile %d", len(subtile_dirs), tile_id
    )
    return subtile_dirs


@task(cache=True, cache_version="2")
def infer_subtile(
    manifest_path: FlyteFile,
    config_yaml: str,
    run_id: str = "",
) -> FlyteFile:
    """Run inference on a single sub-tile.

    Uses mock SHINE if configured, otherwise would invoke real SHINE.

    Args:
        manifest_path: Path to the sub-tile manifest YAML.
        config_yaml: Path to the pipeline config YAML.
        run_id: Pipeline run identifier for S3 output.

    Returns:
        Path to the result Parquet file.
    """
    config = PipelineConfig.from_yaml(config_yaml)

    if config.mock_shine:
        result = run_mock_inference(manifest_path)
    else:
        raise NotImplementedError(
            "Real SHINE inference not yet integrated. "
            "Set mock_shine=true in config."
        )

    result_path = write_subtile_result(result, config.output.result_dir)

    # Upload result to S3
    base = config.output.storage_base_uri
    if base and run_id:
        tile_id, row, col = _parse_subtile_from_manifest_path(
            str(manifest_path)
        )
        prefix = build_subtile_prefix(base, run_id, tile_id, row, col)
        upload_file(result_path, f"{prefix}/result.parquet")

    return result_path


@task
def assemble_results(
    result_paths: list[FlyteFile],
    config_yaml: str,
) -> int:
    """Merge all sub-tile results into the Iceberg catalog.

    Returns:
        Number of rows written.
    """
    config = PipelineConfig.from_yaml(config_yaml)
    return assemble_catalog(
        result_paths,
        warehouse=config.output.catalog_warehouse,
        namespace=config.output.catalog_namespace,
        table_name=config.output.catalog_table,
    )


@task
def validate_results(
    config_yaml: str,
    expected_subtiles: int,
) -> dict:
    """Run quality checks on the assembled catalog.

    Returns:
        Summary statistics dict.
    """
    config = PipelineConfig.from_yaml(config_yaml)
    return validate_catalog(
        warehouse=config.output.catalog_warehouse,
        namespace=config.output.catalog_namespace,
        table_name=config.output.catalog_table,
        expected_subtiles=expected_subtiles,
    )
