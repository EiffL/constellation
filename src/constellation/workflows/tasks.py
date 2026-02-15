"""Flyte task definitions for the constellation data-preparation pipeline.

Each task wraps a pure-Python function from the pipeline modules,
making them callable both as Flyte tasks and as regular functions.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

from flytekit import Cache, Resources, task
from flytekit.types.file import FlyteFile

from constellation.config import PipelineConfig
from constellation.discovery import ObservationIndex, build_observation_index
from constellation.extractor import extract_all_subtiles_for_tile
from constellation.manifest import write_manifests_for_tile
from constellation.quadrant_resolver import (
    QuadrantFootprint,
    quadrant_index_from_dict,
    read_quadrant_footprints,
)
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


@task
def build_config(
    config_yaml: str,
    storage_base_uri: str = "",
    sub_tile_grid: list[int] = [],
) -> str:
    """Read base config from Docker image, apply workflow overrides, return YAML string.

    Args:
        config_yaml: Path to the base config YAML (baked into Docker image).
        storage_base_uri: Override for output.storage_base_uri.
        sub_tile_grid: Override for tiling.sub_tile_grid as [rows, cols].

    Returns:
        Merged configuration as a YAML content string.
    """
    config = PipelineConfig.from_yaml(config_yaml)
    if storage_base_uri:
        config.output.storage_base_uri = storage_base_uri
    if sub_tile_grid and len(sub_tile_grid) == 2:
        config.tiling.sub_tile_grid = (sub_tile_grid[0], sub_tile_grid[1])
    return config.to_yaml_content()


@task(cache=Cache(version="2", serialize=True))
def build_obs_index(config_content: str) -> dict:
    """Build the observation index from S3 listings.

    Returns the index serialized as a dict so Flyte can pass it between tasks.
    """
    config = PipelineConfig.from_yaml_content(config_content)
    obs_index = build_observation_index(
        vis_base_uri=config.data.vis_base_uri,
        s3_region=config.data.s3_region,
        s3_no_sign_request=config.data.s3_no_sign_request,
    )
    return obs_index.to_dict()


# ---------------------------------------------------------------------------
# Quadrant index: fan-out via map_task
# ---------------------------------------------------------------------------


@task(cache=Cache(version="2"), requests=Resources(cpu="0.5", mem="512Mi"))
def build_det_work_items(
    config_content: str,
    obs_index_dict: dict,
) -> list[dict]:
    """Enumerate all (obs_id, dither, ccd) DET files into a flat work-item list.

    Each dict contains paths and metadata for one DET file — the unit
    of parallelism for ``read_det_footprints``.

    Args:
        config_content: Pipeline config as YAML string (unused directly,
            but needed for Flyte lineage).
        obs_index_dict: Serialized ObservationIndex from build_obs_index.

    Returns:
        List of work-item dicts, one per DET file.
    """
    obs_index = ObservationIndex.from_dict(obs_index_dict)
    bucket = obs_index.bucket
    work_items: list[dict] = []

    for obs_id in obs_index.obs_ids():
        psf_key = obs_index.get_psf_key(obs_id)
        psf_path = f"s3://{bucket}/{psf_key}" if psf_key else ""

        for dither, ccd in obs_index.get_dither_ccd_pairs(obs_id):
            det_key = obs_index.get_file(obs_id, "DET", dither, ccd)
            if not det_key:
                continue

            det_path = f"s3://{bucket}/{det_key}"
            bkg_key = obs_index.get_file(obs_id, "BKG", dither, ccd)
            wgt_key = obs_index.get_file(obs_id, "WGT", dither, ccd)

            work_items.append(
                {
                    "det_path": det_path,
                    "bkg_path": f"s3://{bucket}/{bkg_key}" if bkg_key else "",
                    "wgt_path": f"s3://{bucket}/{wgt_key}" if wgt_key else "",
                    "psf_path": psf_path,
                    "obs_id": obs_id,
                    "dither": dither,
                    "ccd": ccd,
                }
            )

    logger.info("Built %d DET work items", len(work_items))
    return work_items


@task(cache=Cache(version="2"), requests=Resources(cpu="0.5", mem="512Mi"))
def read_det_footprints(
    work_item: dict,
) -> FlyteFile:
    """Read WCS headers from one DET file and return quadrant footprints.

    This is the unit of parallelism — one Flyte pod per DET file,
    ~1-2 min each over S3 range requests.

    Returns a FlyteFile (JSON) rather than a list[dict] so that the
    map_task output stays under Flyte's 2 MB protobuf metadata limit
    (~840 tasks x ~72 quadrants = ~60K footprints would exceed it).

    Args:
        work_item: Dict with det_path, bkg_path, wgt_path, psf_path,
            obs_id, dither, ccd.

    Returns:
        FlyteFile pointing to a JSON file with serialized footprints.
    """
    det_path = work_item["det_path"]
    logger.info(
        "Reading footprints from %s (obs=%s, d=%s, c=%s)",
        det_path,
        work_item["obs_id"],
        work_item["dither"],
        work_item["ccd"],
    )

    # Determine if anonymous S3 access based on URI scheme
    s3_anon = det_path.startswith("s3://")

    try:
        footprints = read_quadrant_footprints(det_path, anon=s3_anon)
    except Exception:
        logger.error("Failed to read footprints from %s", det_path, exc_info=True)
        footprints = []

    # Attach bkg/wgt/psf paths and metadata, then serialize
    result: list[dict] = []
    for fp in footprints:
        fp.bkg_path = work_item["bkg_path"]
        fp.wgt_path = work_item["wgt_path"]
        fp.psf_path = work_item["psf_path"]
        fp.obs_id = work_item["obs_id"]
        fp.dither = work_item["dither"]
        fp.ccd = work_item["ccd"]

        result.append(
            {
                "quadrant": fp.quadrant,
                "ra_min": fp.ra_min,
                "ra_max": fp.ra_max,
                "dec_min": fp.dec_min,
                "dec_max": fp.dec_max,
                "det_path": fp.det_path,
                "bkg_path": fp.bkg_path,
                "wgt_path": fp.wgt_path,
                "psf_path": fp.psf_path,
                "obs_id": fp.obs_id,
                "dither": fp.dither,
                "ccd": fp.ccd,
            }
        )

    # Write to a JSON file — Flyte transfers the file via blob storage
    out_path = os.path.join(
        tempfile.mkdtemp(), f"footprints_{work_item['obs_id']}_{work_item['dither']}_{work_item['ccd']}.json"
    )
    with open(out_path, "w") as f:
        json.dump(result, f)

    return FlyteFile(out_path)


@task(cache=Cache(version="2"), requests=Resources(cpu="0.5", mem="1Gi"))
def merge_footprints(
    footprint_files: list[FlyteFile],
) -> FlyteFile:
    """Flatten per-DET footprint files into a single quadrant index file.

    Reads JSON files produced by ``read_det_footprints`` and writes a
    single merged JSON file. Uses FlyteFile throughout to keep large
    data (~60K footprints) on blob storage, not in Flyte's protobuf
    metadata.

    Args:
        footprint_files: List of FlyteFile paths from
            ``read_det_footprints``.

    Returns:
        FlyteFile pointing to the merged quadrant index JSON.
    """
    merged: list[dict] = []
    for ff in footprint_files:
        local_path = ff.download() if hasattr(ff, "download") else str(ff)
        with open(local_path) as f:
            merged.extend(json.load(f))
    logger.info("Merged quadrant index with %d footprints", len(merged))

    out_path = os.path.join(tempfile.mkdtemp(), "quadrant_index.json")
    with open(out_path, "w") as f:
        json.dump(merged, f)

    return FlyteFile(out_path)


# ---------------------------------------------------------------------------
# Combined tile preparation + sub-tile extraction
# ---------------------------------------------------------------------------


@task(
    cache=Cache(version="6", ignored_inputs=("run_id",)),
    requests=Resources(cpu="1", mem="1Gi", ephemeral_storage="40Gi"),
    retries=1,
)
def prepare_and_extract_tile(
    tile_id: int,
    config_content: str,
    obs_index_dict: dict,
    quadrant_index_file: FlyteFile,
    run_id: str = "",
) -> dict:
    """Prepare manifests and extract all sub-tiles for one MER tile.

    Combines the former ``prepare_tile`` and per-sub-tile
    ``extract_subtile_task`` into a single task. One pod downloads each
    multi-GB FITS file once via the tile-level ``_cache/`` directory,
    then extracts all 16 sub-tiles from local data.

    Uses ``Cache(ignored_inputs=("run_id",))`` so re-executions with a
    different ``run_id`` still hit cache for identical ``(tile_id,
    config_content, obs_index_dict, quadrant_index_file)`` inputs.

    Args:
        tile_id: MER tile ID.
        config_content: Pipeline config as YAML content string.
        obs_index_dict: Serialized ObservationIndex from build_obs_index.
        quadrant_index_file: FlyteFile pointing to a JSON quadrant index.
        run_id: Pipeline run identifier for S3 output.

    Returns:
        Summary dict with ``tile_id``, ``n_subtiles``, and ``subtile_dirs``.
    """
    config = PipelineConfig.from_yaml_content(config_content)
    obs_index = ObservationIndex.from_dict(obs_index_dict)

    # Load quadrant index from FlyteFile
    local_path = (
        quadrant_index_file.download()
        if hasattr(quadrant_index_file, "download")
        else str(quadrant_index_file)
    )
    with open(local_path) as f:
        quadrant_index_dict = json.load(f)
    quadrant_index = quadrant_index_from_dict(quadrant_index_dict)

    # Step 1: Generate manifests for all sub-tiles of this tile
    manifest_paths = write_manifests_for_tile(
        tile_id, config, obs_index, quadrant_index=quadrant_index
    )

    # Step 2: Extract all sub-tiles (shared download cache)
    subtile_dirs = extract_all_subtiles_for_tile(
        manifest_paths=manifest_paths,
        extraction_dir=config.output.extraction_dir,
        s3_anon=config.data.s3_no_sign_request,
    )

    # Step 3: Upload to S3 if configured
    base = config.output.storage_base_uri
    logger.info("storage_base_uri=%r, run_id=%r", base, run_id)
    if base and run_id:
        for mp in manifest_paths:
            tile_id_p, row, col = _parse_subtile_from_manifest_path(mp)
            prefix = build_subtile_prefix(base, run_id, tile_id_p, row, col)
            logger.info("Uploading manifest %s -> %s/manifest.yaml", mp, prefix)
            upload_file(mp, f"{prefix}/manifest.yaml")

        for sd in subtile_dirs:
            # Parse tile/row_col from the directory path
            sd_path = Path(sd)
            row_col = sd_path.name  # e.g. "0_0"
            parts = row_col.split("_")
            row, col = int(parts[0]), int(parts[1])
            prefix = build_subtile_prefix(base, run_id, tile_id, row, col)
            logger.info("Uploading subtile dir %s -> %s", sd, prefix)
            upload_directory(sd, prefix)
    else:
        logger.warning("Skipping S3 upload: base=%r, run_id=%r", base, run_id)

    logger.info(
        "Prepared and extracted tile %d: %d sub-tiles",
        tile_id,
        len(subtile_dirs),
    )

    return {
        "tile_id": tile_id,
        "n_subtiles": len(subtile_dirs),
        "subtile_dirs": subtile_dirs,
    }
