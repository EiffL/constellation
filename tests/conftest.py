"""Shared test fixtures for constellation."""

from __future__ import annotations

import io

import numpy as np
import pytest
from astropy.io import fits
from astropy.table import Table
from astropy.wcs import WCS

from constellation.config import (
    DataSourceConfig,
    OutputConfig,
    PipelineConfig,
    TilingConfig,
)
from constellation.schemas import (
    QuadrantRef,
    SkyBounds,
    SubTileManifest,
    SubTileResult,
)

# Three tiles for integration tests
TEST_TILE_IDS = [102018211, 102018212, 102018213]


@pytest.fixture
def sample_config(tmp_path):
    """A minimal PipelineConfig for testing."""
    return PipelineConfig(
        field_name="EDFF_TEST",
        tile_ids=TEST_TILE_IDS,
        tiling=TilingConfig(sub_tile_grid=(4, 4), sub_tile_margin_arcmin=1.0),
        data=DataSourceConfig(),
        output=OutputConfig(
            catalog_warehouse=str(tmp_path / "warehouse"),
            result_dir=str(tmp_path / "results"),
            manifest_dir=str(tmp_path / "manifests"),
        ),
        mock_shine=True,
    )


@pytest.fixture
def sample_sky_bounds():
    """SkyBounds for a sub-tile near EDFF center."""
    return SkyBounds(
        core_ra=(52.800, 52.936),
        core_dec=(-28.200, -28.064),
        extended_ra=(52.783, 52.953),
        extended_dec=(-28.217, -28.047),
    )


@pytest.fixture
def sample_quadrant_ref():
    """A single QuadrantRef pointing to realistic S3 paths."""
    return QuadrantRef(
        sci_path="s3://nasa-irsa-euclid-q1/q1/VIS/2681/EUC_VIS_SWL-DET-002681-00-1-0000000__20241017T042839.727728Z.fits",
        bkg_path="s3://nasa-irsa-euclid-q1/q1/VIS/2681/EUC_VIS_SWL-BKG-002681-00-1-0000000__20241017T042839.727792Z.fits",
        wgt_path="s3://nasa-irsa-euclid-q1/q1/VIS/2681/EUC_VIS_SWL-WGT-002681-00-1-0000000__20241017T042839.727798Z.fits",
        psf_path="s3://nasa-irsa-euclid-q1/q1/VIS/2681/EUC_VIS_GRD-PSF-000-000000-0000000__20240322T192915.424564Z.fits",
        quadrant="3-4.F",
    )


@pytest.fixture
def sample_manifest(sample_sky_bounds, sample_quadrant_ref):
    """A complete SubTileManifest for testing."""
    return SubTileManifest(
        tile_id=102018211,
        sub_tile_row=0,
        sub_tile_col=0,
        sky_bounds=sample_sky_bounds,
        quadrants=[sample_quadrant_ref],
        source_catalog="s3://nasa-irsa-euclid-q1/q1/catalogs/MER_FINAL_CATALOG/102018211/EUC_MER_FINAL-CAT_TILE102018211-CC66F6_20241018T214045.289017Z_00.00.fits",
        source_ids=list(range(1000, 3500)),
        core_source_ids=list(range(1000, 2600)),
    )


@pytest.fixture
def sample_result():
    """A SubTileResult with plausible mock values."""
    return SubTileResult(
        TILE_ID=102018211,
        SUBTILE_ROW=0,
        SUBTILE_COL=0,
        RA=52.868,
        DEC=-28.132,
        G1=0.012,
        G2=-0.008,
        G1_ERR=0.009,
        G2_ERR=0.010,
        N_SOURCES=1600,
        METHOD="mock",
        CONVERGENCE=0.35,
    )


# --- moto S3 fixtures ---

MOCK_BUCKET = "nasa-irsa-euclid-q1"
MOCK_REGION = "us-east-1"

# Realistic VIS file keys for one observation
MOCK_VIS_OBS_ID = "2681"
MOCK_VIS_FILES = [
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_GRD-PSF-000-000000-0000000__20240322T192915.424564Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-DET-002681-00-1-0000000__20241017T042839.727728Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-DET-002681-00-2-0000000__20241017T042839.283308Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-DET-002681-01-1-0000000__20241017T042835.894737Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-DET-002681-01-2-0000000__20241017T042840.682574Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-BKG-002681-00-1-0000000__20241017T042839.727792Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-BKG-002681-00-2-0000000__20241017T042839.283386Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-BKG-002681-01-1-0000000__20241017T042835.894798Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-BKG-002681-01-2-0000000__20241017T042840.682672Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-WGT-002681-00-1-0000000__20241017T042839.727798Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-WGT-002681-00-2-0000000__20241017T042839.283394Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-WGT-002681-01-1-0000000__20241017T042835.894804Z.fits",
    f"q1/VIS/{MOCK_VIS_OBS_ID}/EUC_VIS_SWL-WGT-002681-01-2-0000000__20241017T042840.682681Z.fits",
]

MOCK_CATALOG_FILES = [
    "q1/catalogs/MER_FINAL_CATALOG/102018211/EUC_MER_FINAL-CAT_TILE102018211-CC66F6_20241018T214045.289017Z_00.00.fits",
    "q1/catalogs/MER_FINAL_CATALOG/102018212/EUC_MER_FINAL-CAT_TILE102018212-2F45C0_20241019T073625.426663Z_00.00.fits",
    "q1/catalogs/MER_FINAL_CATALOG/102018213/EUC_MER_FINAL-CAT_TILE102018213-7469CB_20241019T054844.965343Z_00.00.fits",
]


@pytest.fixture
def mock_s3(tmp_path):
    """Create a moto-mocked S3 bucket with realistic Euclid file listings."""
    import boto3
    from moto import mock_aws

    with mock_aws():
        s3 = boto3.client("s3", region_name=MOCK_REGION)
        # us-east-1 buckets don't use LocationConstraint
        s3.create_bucket(Bucket=MOCK_BUCKET)
        # Put empty objects to simulate file listings
        for key in MOCK_VIS_FILES + MOCK_CATALOG_FILES:
            s3.put_object(Bucket=MOCK_BUCKET, Key=key, Body=b"")
        yield s3


# --- Mock FITS helpers for WCS-based tests ---

# EDFF field center (approximate)
MOCK_FIELD_RA = 52.9  # degrees
MOCK_FIELD_DEC = -28.1  # degrees

# Quadrant names used in mock FITS (2 per DET file)
MOCK_QUADRANT_NAMES = ["3-4.F", "3-5.E"]


def _make_wcs_header(
    crval_ra: float, crval_dec: float, naxis1: int = 64, naxis2: int = 64
) -> fits.Header:
    """Create a minimal WCS header for a TAN projection.

    Returns only WCS keywords — NAXIS1/NAXIS2 are set by ImageHDU
    from the data array shape.
    """
    w = WCS(naxis=2)
    w.wcs.crpix = [naxis1 / 2, naxis2 / 2]
    w.wcs.crval = [crval_ra, crval_dec]
    # Scale: ~10 arcmin per quadrant for overlap testing
    cdelt = 10.0 / 60.0 / naxis1  # degrees/pixel
    w.wcs.cdelt = [-cdelt, cdelt]  # RA decreases with pixel X
    w.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    header = w.to_header()
    # Remove structural keys — ImageHDU will set them from the data array
    for key in ("NAXIS", "NAXIS1", "NAXIS2"):
        if key in header:
            del header[key]
    return header


def _make_mock_vis_fits(
    center_ra: float,
    center_dec: float,
    quadrant_names: list[str] | None = None,
    naxis: int = 64,
) -> bytes:
    """Create a mock multi-extension VIS FITS file with valid WCS.

    Each quadrant gets SCI, RMS, and FLG extensions with small data arrays.

    Args:
        center_ra: RA center for the WCS (degrees).
        center_dec: Dec center for the WCS (degrees).
        quadrant_names: HDU name prefixes to create.
        naxis: Image size in pixels (square).

    Returns:
        FITS file content as bytes.
    """
    if quadrant_names is None:
        quadrant_names = MOCK_QUADRANT_NAMES

    hdul = fits.HDUList([fits.PrimaryHDU()])

    for i, qname in enumerate(quadrant_names):
        # Offset each quadrant slightly so they have different footprints
        offset_ra = i * 0.05  # degrees
        offset_dec = i * 0.05

        for suffix in [".SCI", ".RMS", ".FLG"]:
            header = _make_wcs_header(
                center_ra + offset_ra,
                center_dec + offset_dec,
                naxis,
                naxis,
            )
            data = np.random.default_rng(42 + i).random((naxis, naxis)).astype(
                np.float32
            )
            hdu = fits.ImageHDU(data=data, header=header, name=f"{qname}{suffix}")
            hdul.append(hdu)

    buf = io.BytesIO()
    hdul.writeto(buf, output_verify="fix")
    return buf.getvalue()


def _make_mock_psf_fits(
    quadrant_names: list[str] | None = None,
    naxis: int = 16,
) -> bytes:
    """Create a mock PSF grid FITS with one extension per quadrant.

    Args:
        quadrant_names: HDU names for PSF extensions.
        naxis: PSF stamp size in pixels.

    Returns:
        FITS file content as bytes.
    """
    if quadrant_names is None:
        quadrant_names = MOCK_QUADRANT_NAMES

    hdul = fits.HDUList([fits.PrimaryHDU()])
    for qname in quadrant_names:
        data = np.ones((naxis, naxis), dtype=np.float32)
        hdu = fits.ImageHDU(data=data, name=qname)
        hdul.append(hdu)

    buf = io.BytesIO()
    hdul.writeto(buf)
    return buf.getvalue()


def _make_mock_catalog_fits(
    n_sources: int = 100,
    center_ra: float = MOCK_FIELD_RA,
    center_dec: float = MOCK_FIELD_DEC,
    spread: float = 0.5,
) -> bytes:
    """Create a mock MER FITS catalog with sources spread around a center.

    Args:
        n_sources: Number of sources to generate.
        center_ra: Center RA for source distribution (degrees).
        center_dec: Center Dec for source distribution (degrees).
        spread: Half-width of source distribution (degrees).

    Returns:
        FITS catalog content as bytes.
    """
    rng = np.random.default_rng(123)
    table = Table(
        {
            "OBJECT_ID": np.arange(1, n_sources + 1, dtype=np.int64),
            "RIGHT_ASCENSION": (
                center_ra + rng.uniform(-spread, spread, n_sources)
            ).astype(np.float64),
            "DECLINATION": (
                center_dec + rng.uniform(-spread, spread, n_sources)
            ).astype(np.float64),
            "FLUX_DETECTION_TOTAL": rng.uniform(100, 10000, n_sources).astype(
                np.float32
            ),
        }
    )

    buf = io.BytesIO()
    table.write(buf, format="fits", overwrite=True)
    return buf.getvalue()


@pytest.fixture
def mock_vis_fits_path(tmp_path) -> str:
    """Write a mock VIS FITS file to disk and return the path."""
    path = tmp_path / "mock_det.fits"
    path.write_bytes(_make_mock_vis_fits(MOCK_FIELD_RA, MOCK_FIELD_DEC))
    return str(path)


@pytest.fixture
def mock_psf_fits_path(tmp_path) -> str:
    """Write a mock PSF FITS file to disk and return the path."""
    path = tmp_path / "mock_psf.fits"
    path.write_bytes(_make_mock_psf_fits())
    return str(path)


@pytest.fixture
def mock_catalog_fits_path(tmp_path) -> str:
    """Write a mock MER catalog to disk and return the path."""
    path = tmp_path / "mock_catalog.fits"
    path.write_bytes(_make_mock_catalog_fits())
    return str(path)


@pytest.fixture
def mock_s3_with_fits(tmp_path):
    """Create a moto-mocked S3 bucket with real FITS files containing valid WCS.

    Uploads mock VIS DET/BKG/WGT files, PSF files, and MER catalogs
    so that quadrant resolution and extraction can be tested against S3.
    """
    import boto3
    from moto import mock_aws

    with mock_aws():
        s3 = boto3.client("s3", region_name=MOCK_REGION)
        s3.create_bucket(Bucket=MOCK_BUCKET)

        # Generate mock FITS content
        vis_content = _make_mock_vis_fits(MOCK_FIELD_RA, MOCK_FIELD_DEC)
        psf_content = _make_mock_psf_fits()
        catalog_content = _make_mock_catalog_fits()

        # Upload VIS files (DET, BKG, WGT all get the same mock content)
        for key in MOCK_VIS_FILES:
            if "PSF" in key:
                s3.put_object(Bucket=MOCK_BUCKET, Key=key, Body=psf_content)
            else:
                s3.put_object(Bucket=MOCK_BUCKET, Key=key, Body=vis_content)

        # Upload MER catalogs
        for key in MOCK_CATALOG_FILES:
            s3.put_object(Bucket=MOCK_BUCKET, Key=key, Body=catalog_content)

        yield s3
