"""Shared test fixtures for constellation."""

from __future__ import annotations

import pytest

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
