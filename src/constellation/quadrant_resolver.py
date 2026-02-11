"""WCS-based quadrant resolution for VIS observations.

Reads FITS headers from multi-extension VIS DET files to determine
which quadrant HDUs overlap each sub-tile's sky footprint. This
replaces the placeholder ``quadrant="TBD"`` approach with real
spatial filtering.

For S3 files, uses HTTP range requests so only FITS headers (~KB)
are downloaded — not the full pixel data (~GB per DET file).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
from astropy.io import fits
from astropy.wcs import WCS

from constellation.discovery import ObservationIndex
from constellation.schemas import QuadrantRef, SkyBounds

logger = logging.getLogger(__name__)

# Read-ahead buffer: how many bytes to fetch per S3 range request.
# Larger = fewer HTTP round-trips; smaller = less wasted bandwidth.
# FITS headers are multiples of 2880 bytes; 64 KB covers ~22 header blocks.
_S3_READ_AHEAD = 64 * 1024


@dataclass
class QuadrantFootprint:
    """Sky footprint of one VIS quadrant HDU.

    Attributes:
        quadrant: HDU name prefix (e.g. ``"3-4.F"``).
        ra_min: Minimum RA of the bounding box (degrees).
        ra_max: Maximum RA of the bounding box (degrees).
        dec_min: Minimum Dec of the bounding box (degrees).
        dec_max: Maximum Dec of the bounding box (degrees).
        det_path: S3 URI or local path to the DET (science) FITS file.
        bkg_path: S3 URI or local path to the BKG (background) FITS file.
        wgt_path: S3 URI or local path to the WGT (weight) FITS file.
        psf_path: S3 URI or local path to the PSF grid FITS file.
        obs_id: Observation ID.
        dither: Dither index (e.g. ``"00"``).
        ccd: CCD half (``"1"`` or ``"2"``).
    """

    quadrant: str
    ra_min: float
    ra_max: float
    dec_min: float
    dec_max: float
    det_path: str
    bkg_path: str = ""
    wgt_path: str = ""
    psf_path: str = ""
    obs_id: str = ""
    dither: str = ""
    ccd: str = ""


def _get_s3_client(region: str = "us-east-1", anon: bool = True):
    """Create a boto3 S3 client for FITS access."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config as BotoConfig

    config = BotoConfig(signature_version=UNSIGNED) if anon else BotoConfig()
    return boto3.client("s3", region_name=region, config=config)


class _S3SeekableFile:
    """Seekable file-like object backed by S3 range requests.

    Allows ``astropy.io.fits.open(..., lazy_load_hdus=True)`` to read
    only FITS headers from S3 without downloading the full file.
    Astropy reads headers in small chunks and seeks past multi-GB data
    blocks — the seeks translate to zero network I/O.

    A read-ahead buffer reduces the number of HTTP round-trips: each
    ``read()`` fetches at least ``_S3_READ_AHEAD`` bytes, so sequential
    small reads (e.g. 2880-byte FITS header blocks) are served from the
    buffer without extra requests.
    """

    def __init__(self, bucket: str, key: str, s3_client, size: int):
        self._bucket = bucket
        self._key = key
        self._s3 = s3_client
        self._size = size
        self._pos = 0
        # Read-ahead buffer
        self._buf = b""
        self._buf_start = 0  # offset in file where _buf starts

    def read(self, n: int = -1) -> bytes:
        if n == -1 or n is None:
            n = self._size - self._pos
        if n <= 0 or self._pos >= self._size:
            return b""

        # Check if the buffer covers this read
        buf_end = self._buf_start + len(self._buf)
        if self._buf and self._buf_start <= self._pos < buf_end:
            offset_in_buf = self._pos - self._buf_start
            available = len(self._buf) - offset_in_buf
            if available >= n:
                # Fully covered by buffer
                data = self._buf[offset_in_buf : offset_in_buf + n]
                self._pos += len(data)
                return data

        # Fetch from S3 with read-ahead
        fetch_size = max(n, _S3_READ_AHEAD)
        end = min(self._pos + fetch_size - 1, self._size - 1)
        resp = self._s3.get_object(
            Bucket=self._bucket,
            Key=self._key,
            Range=f"bytes={self._pos}-{end}",
        )
        self._buf = resp["Body"].read()
        self._buf_start = self._pos

        data = self._buf[:n]
        self._pos += len(data)
        return data

    def seek(self, pos: int, whence: int = 0) -> int:
        if whence == 0:
            self._pos = pos
        elif whence == 1:
            self._pos += pos
        elif whence == 2:
            self._pos = self._size + pos
        self._pos = max(0, min(self._pos, self._size))
        return self._pos

    def tell(self) -> int:
        return self._pos

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def close(self) -> None:
        self._buf = b""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _open_fits(path: str, anon: bool = True) -> fits.HDUList:
    """Open a FITS file from a local path or S3 URI.

    For S3 paths, uses range requests via :class:`_S3SeekableFile` so
    that ``lazy_load_hdus=True`` reads only header blocks (~KB) and
    seeks past data blocks (~GB) without downloading them.

    For local paths, uses memory-mapping for efficient access.

    Args:
        path: Local filesystem path or ``s3://`` URI.
        anon: If True, use anonymous (unsigned) S3 access.

    Returns:
        An open ``HDUList``.
    """
    if path.startswith("s3://"):
        # Parse s3://bucket/key
        parts = path.replace("s3://", "").split("/", 1)
        bucket, key = parts[0], parts[1]

        s3 = _get_s3_client(anon=anon)

        # Get file size with HEAD request
        head = s3.head_object(Bucket=bucket, Key=key)
        size = head["ContentLength"]

        f = _S3SeekableFile(bucket, key, s3, size)
        return fits.open(f, lazy_load_hdus=True)
    else:
        return fits.open(path, memmap=True)


def read_quadrant_footprints(
    det_path: str,
    anon: bool = True,
) -> list[QuadrantFootprint]:
    """Read sky footprints for all quadrant HDUs in a DET FITS file.

    Opens the multi-extension DET file and finds all ``*.SCI`` extensions.
    For each, reads the WCS from the header and computes the bounding box
    in sky coordinates (RA/Dec).

    For S3 files, only headers are downloaded via range requests — the
    multi-GB pixel data is never transferred.

    Args:
        det_path: Path to the DET FITS file (local or ``s3://``).
        anon: Use anonymous S3 access.

    Returns:
        List of ``QuadrantFootprint`` objects, one per quadrant HDU.
    """
    footprints: list[QuadrantFootprint] = []

    with _open_fits(det_path, anon=anon) as hdul:
        for hdu in hdul:
            if hdu.name.endswith(".SCI"):
                # Quadrant name prefix: e.g. "3-4.F" from "3-4.F.SCI"
                quadrant = hdu.name.rsplit(".SCI", 1)[0]
                try:
                    wcs = WCS(hdu.header)
                    naxis1 = hdu.header.get("NAXIS1", 0)
                    naxis2 = hdu.header.get("NAXIS2", 0)
                    if naxis1 == 0 or naxis2 == 0:
                        logger.warning(
                            "Skipping quadrant %s in %s: zero-size axes",
                            quadrant,
                            det_path,
                        )
                        continue

                    # Compute sky coords for the four corners
                    corners_pix = np.array(
                        [
                            [0, 0],
                            [naxis1 - 1, 0],
                            [naxis1 - 1, naxis2 - 1],
                            [0, naxis2 - 1],
                        ],
                        dtype=float,
                    )
                    corners_sky = wcs.all_pix2world(corners_pix, 0)
                    ra_vals = corners_sky[:, 0]
                    dec_vals = corners_sky[:, 1]

                    footprints.append(
                        QuadrantFootprint(
                            quadrant=quadrant,
                            ra_min=float(np.min(ra_vals)),
                            ra_max=float(np.max(ra_vals)),
                            dec_min=float(np.min(dec_vals)),
                            dec_max=float(np.max(dec_vals)),
                            det_path=det_path,
                        )
                    )
                except Exception:
                    logger.warning(
                        "Failed to read WCS for quadrant %s in %s",
                        quadrant,
                        det_path,
                        exc_info=True,
                    )

    return footprints


def footprint_overlaps(fp: QuadrantFootprint, sky_bounds: SkyBounds) -> bool:
    """Test whether a quadrant footprint overlaps a sub-tile's extended area.

    Uses a simple 2D bounding-box intersection test. This is conservative
    (may include quadrants that only barely touch the margin) but fast.

    Args:
        fp: Quadrant footprint with RA/Dec bounding box.
        sky_bounds: Sub-tile sky bounds (uses extended area).

    Returns:
        True if the bounding boxes overlap.
    """
    ext_ra_min, ext_ra_max = sky_bounds.extended_ra
    ext_dec_min, ext_dec_max = sky_bounds.extended_dec

    # No overlap if one box is entirely to the left/right/above/below the other
    if fp.ra_max < ext_ra_min or fp.ra_min > ext_ra_max:
        return False
    if fp.dec_max < ext_dec_min or fp.dec_min > ext_dec_max:
        return False

    return True


def build_quadrant_index(
    obs_index: ObservationIndex,
    s3_anon: bool = True,
) -> list[QuadrantFootprint]:
    """Build a spatial index of all quadrant footprints across observations.

    Iterates all (obs_id, dither, ccd) DET files from the observation index,
    reads their WCS headers, and returns a flat list of quadrant footprints
    with attached bkg/wgt/psf paths.

    For S3 files, only downloads FITS headers via range requests (~KB per
    DET file instead of ~7 GB).

    Args:
        obs_index: Pre-built observation index from S3 listing.
        s3_anon: Use anonymous S3 access for reading FITS headers.

    Returns:
        List of all ``QuadrantFootprint`` objects across all observations.
    """
    bucket = obs_index.bucket
    all_footprints: list[QuadrantFootprint] = []

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

            bkg_path = f"s3://{bucket}/{bkg_key}" if bkg_key else ""
            wgt_path = f"s3://{bucket}/{wgt_key}" if wgt_key else ""

            logger.info(
                "Reading quadrant footprints from %s (obs=%s, d=%s, c=%s)",
                det_path,
                obs_id,
                dither,
                ccd,
            )

            try:
                footprints = read_quadrant_footprints(det_path, anon=s3_anon)
            except Exception:
                logger.error(
                    "Failed to read footprints from %s", det_path, exc_info=True
                )
                continue

            for fp in footprints:
                fp.bkg_path = bkg_path
                fp.wgt_path = wgt_path
                fp.psf_path = psf_path
                fp.obs_id = obs_id
                fp.dither = dither
                fp.ccd = ccd

            all_footprints.extend(footprints)

    logger.info("Built quadrant index with %d footprints", len(all_footprints))
    return all_footprints


def resolve_quadrants_for_subtile(
    index: list[QuadrantFootprint],
    sky_bounds: SkyBounds,
) -> list[QuadrantRef]:
    """Filter the quadrant index to those overlapping a sub-tile.

    Args:
        index: Full quadrant index from ``build_quadrant_index``.
        sky_bounds: Sub-tile sky bounds.

    Returns:
        List of ``QuadrantRef`` objects for overlapping quadrants.
    """
    refs: list[QuadrantRef] = []
    for fp in index:
        if footprint_overlaps(fp, sky_bounds):
            refs.append(
                QuadrantRef(
                    sci_path=fp.det_path,
                    bkg_path=fp.bkg_path,
                    wgt_path=fp.wgt_path,
                    psf_path=fp.psf_path,
                    quadrant=fp.quadrant,
                    obs_id=fp.obs_id,
                    dither=fp.dither,
                    ccd=fp.ccd,
                )
            )
    return refs


# --- Serialization for Flyte transport ---


def quadrant_index_to_dict(index: list[QuadrantFootprint]) -> list[dict]:
    """Serialize a quadrant index to a list of plain dicts."""
    return [
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
        for fp in index
    ]


def quadrant_index_from_dict(data: list[dict]) -> list[QuadrantFootprint]:
    """Deserialize a quadrant index from a list of plain dicts."""
    return [QuadrantFootprint(**d) for d in data]
