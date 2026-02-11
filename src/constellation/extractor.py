"""FITS extraction and catalog subsetting for self-contained sub-tile directories.

Downloads multi-extension VIS FITS files, extracts individual quadrant HDUs
into small single-quadrant files, subsets the MER catalog to the sub-tile's
extended area, and rewrites the manifest with relative paths.
"""

from __future__ import annotations

import logging
import shutil
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
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    suffixes = [".SCI", ".RMS", ".FLG"]
    hdu_names = [f"{quadrant_name}{s}" for s in suffixes]

    with fits.open(str(src_path)) as hdul:
        new_hdul = fits.HDUList([fits.PrimaryHDU()])
        for name in hdu_names:
            try:
                ext = hdul[name]
                new_hdu = fits.ImageHDU(
                    data=ext.data,
                    header=ext.header,
                    name=name,
                )
                new_hdul.append(new_hdu)
            except KeyError:
                logger.warning(
                    "HDU %s not found in %s, skipping", name, src_path
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
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with fits.open(str(src_path)) as hdul:
        new_hdul = fits.HDUList([fits.PrimaryHDU()])
        try:
            ext = hdul[quadrant_name]
            new_hdu = fits.ImageHDU(
                data=ext.data,
                header=ext.header,
                name=quadrant_name,
            )
            new_hdul.append(new_hdu)
        except KeyError:
            logger.warning(
                "PSF HDU %s not found in %s", quadrant_name, src_path
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


def extract_subtile(
    manifest_path: str | Path,
    extraction_dir: str | Path,
    s3_anon: bool = True,
) -> str:
    """Extract all data for one sub-tile into a self-contained directory.

    Reads the manifest, downloads and extracts the relevant quadrant HDUs
    from each FITS file, subsets the catalog, and rewrites the manifest
    with relative paths.

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

    # Track new quadrant refs with relative paths
    new_quadrants: list[QuadrantRef] = []

    for qref in manifest.quadrants:
        qname = qref.quadrant
        # Build filename stem: {obs}_{dither}_{ccd}_{quadrant}
        safe_qname = qname.replace("-", "").replace(".", "")
        stem = f"{qref.obs_id}_{qref.dither}_{qref.ccd}_{safe_qname}"

        # Extract science (DET) quadrant
        sci_rel = f"exposures/{stem}_sci.fits"
        if qref.sci_path:
            local_sci = _ensure_local(qref.sci_path, cache_dir, anon=s3_anon)
            extract_quadrant_fits(local_sci, qname, subtile_dir / sci_rel)
        else:
            sci_rel = ""

        # Extract background (BKG) quadrant
        bkg_rel = f"exposures/{stem}_bkg.fits"
        if qref.bkg_path:
            local_bkg = _ensure_local(qref.bkg_path, cache_dir, anon=s3_anon)
            extract_quadrant_fits(local_bkg, qname, subtile_dir / bkg_rel)
        else:
            bkg_rel = ""

        # Extract weight (WGT) quadrant
        wgt_rel = f"exposures/{stem}_wgt.fits"
        if qref.wgt_path:
            local_wgt = _ensure_local(qref.wgt_path, cache_dir, anon=s3_anon)
            extract_quadrant_fits(local_wgt, qname, subtile_dir / wgt_rel)
        else:
            wgt_rel = ""

        # Extract PSF
        psf_rel = f"psf/{qref.obs_id}_{safe_qname}_psf.fits"
        if qref.psf_path:
            local_psf = _ensure_local(qref.psf_path, cache_dir, anon=s3_anon)
            extract_psf_fits(local_psf, qname, subtile_dir / psf_rel)
        else:
            psf_rel = ""

        new_quadrants.append(
            QuadrantRef(
                sci_path=sci_rel,
                bkg_path=bkg_rel,
                wgt_path=wgt_rel,
                psf_path=psf_rel,
                quadrant=qname,
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
    new_manifest.to_yaml(subtile_dir / "manifest.yaml")

    logger.info(
        "Extracted sub-tile %d/%d_%d: %d quadrants, %d sources",
        manifest.tile_id,
        manifest.sub_tile_row,
        manifest.sub_tile_col,
        len(new_quadrants),
        len(source_ids),
    )

    return str(subtile_dir)
