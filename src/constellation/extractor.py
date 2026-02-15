"""FITS extraction and catalog subsetting for self-contained sub-tile directories.

Downloads multi-extension VIS FITS files, extracts quadrant HDUs grouped
by exposure into consolidated FITS files, subsets the MER catalog to the
sub-tile's extended area, and rewrites the manifest with relative paths.
"""

from __future__ import annotations

import logging
import shutil
from collections import defaultdict
from pathlib import Path

import numpy as np
from astropy.io import fits
from astropy.table import Table

from constellation.schemas import QuadrantRef, SubTileManifest

logger = logging.getLogger(__name__)


def _ensure_local(src_path: str, cache_dir: Path, anon: bool = True) -> Path:
    """Ensure a file is available locally, downloading from S3 if needed.

    If ``src_path`` is an S3 URI, downloads it to ``cache_dir`` (if not
    already cached) and returns the local path. If it's already local,
    returns it directly.

    Uses boto3 for S3 access (compatible with moto mocking).

    Args:
        src_path: Local path or ``s3://`` URI.
        cache_dir: Directory for caching downloaded files.
        anon: Use anonymous S3 access.

    Returns:
        Local filesystem path to the file.
    """
    if not src_path.startswith("s3://"):
        return Path(src_path)

    # Derive a cache filename from the S3 key
    # s3://bucket/path/to/file.fits -> cache_dir/path/to/file.fits
    s3_key = src_path.split("://", 1)[1]  # bucket/path/to/file.fits
    local_path = cache_dir / s3_key.split("/", 1)[1]  # path/to/file.fits

    if local_path.exists():
        return local_path

    local_path.parent.mkdir(parents=True, exist_ok=True)

    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    # Parse s3://bucket/key
    parts = src_path.replace("s3://", "").split("/", 1)
    bucket, key = parts[0], parts[1]

    config = Config(signature_version=UNSIGNED) if anon else Config()
    s3 = boto3.client("s3", region_name="us-east-1", config=config)

    logger.info("Downloading %s -> %s", src_path, local_path)
    s3.download_file(bucket, key, str(local_path))

    return local_path


def extract_quadrant_fits(
    src_path: str | Path,
    quadrant_name: str,
    dest_path: str | Path,
) -> None:
    """Extract a single quadrant's HDUs from a multi-extension FITS file.

    Copies the ``{quadrant_name}.SCI``, ``.RMS``, and ``.FLG`` extensions
    from the source file into a new, smaller FITS file at ``dest_path``.

    Args:
        src_path: Path to the multi-extension FITS file.
        quadrant_name: Quadrant HDU prefix (e.g. ``"3-4.F"``).
        dest_path: Output path for the extracted FITS file.
    """
    extract_quadrants_fits(src_path, [quadrant_name], dest_path)


def extract_quadrants_fits(
    src_path: str | Path,
    quadrant_names: list[str],
    dest_path: str | Path,
) -> None:
    """Extract multiple quadrants' HDUs from a multi-extension FITS file.

    Copies the ``{quadrant}.SCI``, ``.RMS``, and ``.FLG`` extensions for
    each quadrant into a single consolidated FITS file at ``dest_path``.

    Args:
        src_path: Path to the multi-extension FITS file.
        quadrant_names: Quadrant HDU prefixes (e.g. ``["3-4.F", "3-5.E"]``).
        dest_path: Output path for the extracted FITS file.
    """
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    suffixes = [".SCI", ".RMS", ".FLG"]

    with fits.open(str(src_path)) as hdul:
        new_hdul = fits.HDUList([fits.PrimaryHDU()])
        for qname in quadrant_names:
            for suffix in suffixes:
                hdu_name = f"{qname}{suffix}"
                try:
                    ext = hdul[hdu_name]
                    new_hdu = fits.ImageHDU(
                        data=ext.data,
                        header=ext.header,
                        name=hdu_name,
                    )
                    new_hdul.append(new_hdu)
                except KeyError:
                    logger.warning(
                        "HDU %s not found in %s, skipping", hdu_name, src_path
                    )
        new_hdul.writeto(str(dest_path), overwrite=True)


def extract_psf_fits(
    src_path: str | Path,
    quadrant_name: str,
    dest_path: str | Path,
) -> None:
    """Extract a single quadrant's PSF extension from a PSF grid FITS file.

    Args:
        src_path: Path to the PSF grid FITS file.
        quadrant_name: Quadrant HDU name (e.g. ``"3-4.F"``).
        dest_path: Output path for the extracted PSF FITS file.
    """
    extract_psfs_fits(src_path, [quadrant_name], dest_path)


def extract_psfs_fits(
    src_path: str | Path,
    quadrant_names: list[str],
    dest_path: str | Path,
) -> None:
    """Extract multiple quadrants' PSF extensions from a PSF grid FITS file.

    Args:
        src_path: Path to the PSF grid FITS file.
        quadrant_names: Quadrant HDU names (e.g. ``["3-4.F", "3-5.E"]``).
        dest_path: Output path for the extracted PSF FITS file.
    """
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with fits.open(str(src_path)) as hdul:
        new_hdul = fits.HDUList([fits.PrimaryHDU()])
        for qname in quadrant_names:
            try:
                ext = hdul[qname]
                new_hdu = fits.ImageHDU(
                    data=ext.data,
                    header=ext.header,
                    name=qname,
                )
                new_hdul.append(new_hdu)
            except KeyError:
                logger.warning(
                    "PSF HDU %s not found in %s", qname, src_path
                )
        new_hdul.writeto(str(dest_path), overwrite=True)


def subset_catalog(
    catalog_path: str | Path,
    sky_bounds_extended: tuple[tuple[float, float], tuple[float, float]],
    sky_bounds_core: tuple[tuple[float, float], tuple[float, float]],
    dest_path: str | Path,
    ra_col: str = "RIGHT_ASCENSION",
    dec_col: str = "DECLINATION",
    id_col: str = "OBJECT_ID",
) -> tuple[list[int], list[int]]:
    """Subset a MER FITS catalog to sources within the sub-tile bounds.

    Reads the catalog, filters by RA/Dec within the extended bounds,
    identifies which sources are also in the core bounds, and writes
    the filtered catalog to ``dest_path``.

    Args:
        catalog_path: Path to the MER FITS catalog.
        sky_bounds_extended: ``((ra_min, ra_max), (dec_min, dec_max))``
            for the extended area.
        sky_bounds_core: ``((ra_min, ra_max), (dec_min, dec_max))``
            for the core area.
        dest_path: Output path for the subset catalog.
        ra_col: Column name for right ascension.
        dec_col: Column name for declination.
        id_col: Column name for object ID.

    Returns:
        ``(all_source_ids, core_source_ids)`` — lists of object IDs
        in the extended and core areas respectively.
    """
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    table = Table.read(str(catalog_path))

    # Normalize column names to uppercase for case-insensitive matching
    col_map = {c.upper(): c for c in table.colnames}
    ra_col_actual = col_map.get(ra_col.upper(), ra_col)
    dec_col_actual = col_map.get(dec_col.upper(), dec_col)
    id_col_actual = col_map.get(id_col.upper(), id_col)

    ra = np.array(table[ra_col_actual])
    dec = np.array(table[dec_col_actual])

    # Extended area filter
    ext_ra_min, ext_ra_max = sky_bounds_extended[0]
    ext_dec_min, ext_dec_max = sky_bounds_extended[1]
    ext_mask = (
        (ra >= ext_ra_min)
        & (ra <= ext_ra_max)
        & (dec >= ext_dec_min)
        & (dec <= ext_dec_max)
    )

    # Core area filter (subset of extended)
    core_ra_min, core_ra_max = sky_bounds_core[0]
    core_dec_min, core_dec_max = sky_bounds_core[1]
    core_mask = (
        (ra >= core_ra_min)
        & (ra <= core_ra_max)
        & (dec >= core_dec_min)
        & (dec <= core_dec_max)
    )

    subset = table[ext_mask]
    subset.write(str(dest_path), format="fits", overwrite=True)

    all_ids = list(table[id_col_actual][ext_mask].astype(int))
    core_ids = list(table[id_col_actual][ext_mask & core_mask].astype(int))

    logger.info(
        "Subset catalog: %d extended, %d core sources (from %d total)",
        len(all_ids),
        len(core_ids),
        len(table),
    )

    return all_ids, core_ids


def _exposure_key(obs_id: str, dither: str, ccd: str) -> str:
    """Build a filesystem-safe key for an exposure."""
    return f"{obs_id}_{dither}_{ccd}"


def extract_subtile(
    manifest_path: str | Path,
    extraction_dir: str | Path,
    s3_anon: bool = True,
) -> str:
    """Extract all data for one sub-tile into a self-contained directory.

    Reads the manifest, downloads and extracts the relevant quadrant HDUs
    from each FITS file (consolidated per exposure), subsets the catalog,
    and rewrites the manifest with relative paths.

    Args:
        manifest_path: Path to the sub-tile manifest YAML.
        extraction_dir: Base directory for extracted sub-tile directories.
        s3_anon: Use anonymous S3 access.

    Returns:
        Path to the extracted sub-tile directory.
    """
    manifest = SubTileManifest.from_yaml(manifest_path)
    extraction_dir = Path(extraction_dir)

    # Sub-tile directory: extraction_dir/{tile_id}/{row}_{col}/
    subtile_dir = (
        extraction_dir
        / str(manifest.tile_id)
        / f"{manifest.sub_tile_row}_{manifest.sub_tile_col}"
    )
    subtile_dir.mkdir(parents=True, exist_ok=True)

    exposures_dir = subtile_dir / "exposures"
    psf_dir = subtile_dir / "psf"
    exposures_dir.mkdir(exist_ok=True)
    psf_dir.mkdir(exist_ok=True)

    # Tile-level cache for downloaded source files
    cache_dir = extraction_dir / str(manifest.tile_id) / "_cache"

    # Group quadrants by exposure (obs_id, dither, ccd)
    exposure_groups: dict[str, list[QuadrantRef]] = defaultdict(list)
    for qref in manifest.quadrants:
        ekey = _exposure_key(qref.obs_id, qref.dither, qref.ccd)
        exposure_groups[ekey].append(qref)

    # Track new quadrant refs with relative paths
    new_quadrants: list[QuadrantRef] = []

    for ekey, qrefs in exposure_groups.items():
        quadrant_names = [q.quadrant for q in qrefs]
        first = qrefs[0]

        # Consolidated paths — one FITS per exposure per type
        sci_rel = f"exposures/{ekey}_sci.fits" if first.sci_path else ""
        bkg_rel = f"exposures/{ekey}_bkg.fits" if first.bkg_path else ""
        wgt_rel = f"exposures/{ekey}_wgt.fits" if first.wgt_path else ""
        psf_rel = f"psf/{ekey}_psf.fits" if first.psf_path else ""

        if first.sci_path:
            local_sci = _ensure_local(first.sci_path, cache_dir, anon=s3_anon)
            extract_quadrants_fits(local_sci, quadrant_names, subtile_dir / sci_rel)

        if first.bkg_path:
            local_bkg = _ensure_local(first.bkg_path, cache_dir, anon=s3_anon)
            extract_quadrants_fits(local_bkg, quadrant_names, subtile_dir / bkg_rel)

        if first.wgt_path:
            local_wgt = _ensure_local(first.wgt_path, cache_dir, anon=s3_anon)
            extract_quadrants_fits(local_wgt, quadrant_names, subtile_dir / wgt_rel)

        if first.psf_path:
            local_psf = _ensure_local(first.psf_path, cache_dir, anon=s3_anon)
            extract_psfs_fits(local_psf, quadrant_names, subtile_dir / psf_rel)

        # All quadrants in this exposure share the same file paths
        for qref in qrefs:
            new_quadrants.append(
                QuadrantRef(
                    sci_path=sci_rel,
                    bkg_path=bkg_rel,
                    wgt_path=wgt_rel,
                    psf_path=psf_rel,
                    quadrant=qref.quadrant,
                    obs_id=qref.obs_id,
                    dither=qref.dither,
                    ccd=qref.ccd,
                )
            )

    # Subset catalog
    catalog_rel = "catalog.fits"
    source_ids: list[int] = []
    core_source_ids: list[int] = []

    if manifest.source_catalog:
        local_catalog = _ensure_local(
            manifest.source_catalog, cache_dir, anon=s3_anon
        )
        source_ids, core_source_ids = subset_catalog(
            local_catalog,
            sky_bounds_extended=(
                manifest.sky_bounds.extended_ra,
                manifest.sky_bounds.extended_dec,
            ),
            sky_bounds_core=(
                manifest.sky_bounds.core_ra,
                manifest.sky_bounds.core_dec,
            ),
            dest_path=subtile_dir / catalog_rel,
        )
    else:
        logger.warning("No source catalog in manifest, skipping subset")

    # Write updated manifest with relative paths
    new_manifest = SubTileManifest(
        tile_id=manifest.tile_id,
        sub_tile_row=manifest.sub_tile_row,
        sub_tile_col=manifest.sub_tile_col,
        sky_bounds=manifest.sky_bounds,
        quadrants=new_quadrants,
        source_catalog=catalog_rel,
        source_ids=source_ids,
        core_source_ids=core_source_ids,
    )
    new_manifest.to_yaml(subtile_dir / "manifest_local.yaml")

    logger.info(
        "Extracted sub-tile %d/%d_%d: %d quadrants, %d sources",
        manifest.tile_id,
        manifest.sub_tile_row,
        manifest.sub_tile_col,
        len(new_quadrants),
        len(source_ids),
    )

    return str(subtile_dir)


def extract_all_subtiles_for_tile(
    manifest_paths: list[str],
    extraction_dir: str | Path,
    s3_anon: bool = True,
) -> list[str]:
    """Extract all sub-tiles for one tile, streaming one source file at a time.

    Consolidates quadrant HDUs by exposure — all quadrants from the same
    ``(obs_id, dither, ccd)`` for a given sub-tile go into one FITS file
    per type (sci, bkg, wgt, psf). This dramatically reduces file count.

    Streams source files one at a time: download -> extract all sub-tile
    quadrants from it -> delete. Caps peak disk at ~1 source file (~7 GB)
    plus the extracted output.

    Args:
        manifest_paths: Paths to sub-tile manifest YAML files (all for the same tile).
        extraction_dir: Base directory for extracted sub-tile directories.
        s3_anon: Use anonymous S3 access.

    Returns:
        List of extracted sub-tile directory paths.
    """
    from dataclasses import dataclass

    extraction_dir = Path(extraction_dir)

    @dataclass
    class _SubTileState:
        """Accumulated state for one sub-tile during streaming extraction."""

        manifest: SubTileManifest
        subtile_dir: Path

    # --- Step 1: Parse all manifests, set up directories, build index ---

    # Per-sub-tile state keyed by (row, col)
    states: dict[tuple[int, int], _SubTileState] = {}
    # (row, col) -> list of QuadrantRef templates
    quadrant_templates: dict[tuple[int, int], list[dict]] = defaultdict(list)
    # source_path -> {dest_path: (list[quadrant_names], is_psf)}
    source_index: dict[str, dict[Path, tuple[list[str], bool]]] = defaultdict(dict)

    tile_id: int | None = None

    for manifest_path in manifest_paths:
        manifest = SubTileManifest.from_yaml(manifest_path)
        tile_id = manifest.tile_id
        key = (manifest.sub_tile_row, manifest.sub_tile_col)

        subtile_dir = (
            extraction_dir
            / str(manifest.tile_id)
            / f"{manifest.sub_tile_row}_{manifest.sub_tile_col}"
        )
        subtile_dir.mkdir(parents=True, exist_ok=True)
        (subtile_dir / "exposures").mkdir(exist_ok=True)
        (subtile_dir / "psf").mkdir(exist_ok=True)

        states[key] = _SubTileState(manifest=manifest, subtile_dir=subtile_dir)

        # Group quadrants by exposure within this sub-tile
        exposure_groups: dict[str, list[QuadrantRef]] = defaultdict(list)
        for qref in manifest.quadrants:
            ekey = _exposure_key(qref.obs_id, qref.dither, qref.ccd)
            exposure_groups[ekey].append(qref)

        for ekey, qrefs in exposure_groups.items():
            quadrant_names = [q.quadrant for q in qrefs]
            first = qrefs[0]

            # Consolidated paths — one FITS per exposure per type
            sci_rel = f"exposures/{ekey}_sci.fits" if first.sci_path else ""
            bkg_rel = f"exposures/{ekey}_bkg.fits" if first.bkg_path else ""
            wgt_rel = f"exposures/{ekey}_wgt.fits" if first.wgt_path else ""
            psf_rel = f"psf/{ekey}_psf.fits" if first.psf_path else ""

            # Register consolidated extraction jobs in source_index
            if first.sci_path:
                dest = subtile_dir / sci_rel
                source_index[first.sci_path][dest] = (quadrant_names, False)
            if first.bkg_path:
                dest = subtile_dir / bkg_rel
                source_index[first.bkg_path][dest] = (quadrant_names, False)
            if first.wgt_path:
                dest = subtile_dir / wgt_rel
                source_index[first.wgt_path][dest] = (quadrant_names, False)
            if first.psf_path:
                dest = subtile_dir / psf_rel
                source_index[first.psf_path][dest] = (quadrant_names, True)

            # Build QuadrantRef templates for the manifest
            for qref in qrefs:
                quadrant_templates[key].append(
                    {
                        "sci_path": sci_rel,
                        "bkg_path": bkg_rel,
                        "wgt_path": wgt_rel,
                        "psf_path": psf_rel,
                        "quadrant": qref.quadrant,
                        "obs_id": qref.obs_id,
                        "dither": qref.dither,
                        "ccd": qref.ccd,
                    }
                )

    # Tile-level cache for downloaded source files
    cache_dir = extraction_dir / str(tile_id) / "_cache"

    # --- Step 2: Stream source files one at a time ---

    for src_path, jobs_by_dest in source_index.items():
        local_path = _ensure_local(src_path, cache_dir, anon=s3_anon)

        for dest_path, (quadrant_names, is_psf) in jobs_by_dest.items():
            if is_psf:
                extract_psfs_fits(local_path, quadrant_names, dest_path)
            else:
                extract_quadrants_fits(local_path, quadrant_names, dest_path)

        # Delete the cached file immediately after all extractions
        if local_path.is_relative_to(cache_dir) and local_path.exists():
            local_path.unlink()
            logger.info("Deleted cached file %s", local_path)

    # --- Step 3: Subset catalogs and write manifests ---

    subtile_dirs: list[str] = []

    for key, state in states.items():
        manifest = state.manifest
        subtile_dir = state.subtile_dir

        # Subset catalog (catalogs are small, no streaming needed)
        catalog_rel = "catalog.fits"
        source_ids: list[int] = []
        core_source_ids: list[int] = []

        if manifest.source_catalog:
            local_catalog = _ensure_local(
                manifest.source_catalog, cache_dir, anon=s3_anon
            )
            source_ids, core_source_ids = subset_catalog(
                local_catalog,
                sky_bounds_extended=(
                    manifest.sky_bounds.extended_ra,
                    manifest.sky_bounds.extended_dec,
                ),
                sky_bounds_core=(
                    manifest.sky_bounds.core_ra,
                    manifest.sky_bounds.core_dec,
                ),
                dest_path=subtile_dir / catalog_rel,
            )
            # Delete cached catalog after use
            if local_catalog.is_relative_to(cache_dir) and local_catalog.exists():
                local_catalog.unlink()
                logger.info("Deleted cached catalog %s", local_catalog)
        else:
            logger.warning("No source catalog in manifest, skipping subset")

        # Build QuadrantRef list from templates
        new_quadrants = [
            QuadrantRef(**tmpl) for tmpl in quadrant_templates[key]
        ]

        # Write updated manifest with relative paths
        new_manifest = SubTileManifest(
            tile_id=manifest.tile_id,
            sub_tile_row=manifest.sub_tile_row,
            sub_tile_col=manifest.sub_tile_col,
            sky_bounds=manifest.sky_bounds,
            quadrants=new_quadrants,
            source_catalog=catalog_rel,
            source_ids=source_ids,
            core_source_ids=core_source_ids,
        )
        new_manifest.to_yaml(subtile_dir / "manifest_local.yaml")

        logger.info(
            "Extracted sub-tile %d/%d_%d: %d quadrants, %d sources",
            manifest.tile_id,
            manifest.sub_tile_row,
            manifest.sub_tile_col,
            len(new_quadrants),
            len(source_ids),
        )

        subtile_dirs.append(str(subtile_dir))

    # --- Step 4: Clean up cache directory ---

    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        logger.info("Cleaned up cache directory %s", cache_dir)

    logger.info(
        "Extracted %d sub-tiles into %s",
        len(subtile_dirs),
        extraction_dir,
    )
    return subtile_dirs
