"""Tests for constellation.discovery using moto S3 mocks."""

import pytest

from constellation.discovery import (
    ObservationIndex,
    _parse_vis_filename,
    build_observation_index,
    build_quadrant_refs,
    list_mer_catalog,
)
from tests.conftest import MOCK_BUCKET, MOCK_REGION


class TestParseVisFilename:
    def test_det_file(self):
        key = "q1/VIS/2681/EUC_VIS_SWL-DET-002681-00-1-0000000__20241017T042839.727728Z.fits"
        rec = _parse_vis_filename(key)
        assert rec is not None
        assert rec.file_type == "DET"
        assert rec.obs_id == "002681"
        assert rec.dither == "00"
        assert rec.ccd == "1"

    def test_bkg_file(self):
        key = "q1/VIS/2681/EUC_VIS_SWL-BKG-002681-01-2-0000000__20241017T042840.682672Z.fits"
        rec = _parse_vis_filename(key)
        assert rec is not None
        assert rec.file_type == "BKG"
        assert rec.dither == "01"
        assert rec.ccd == "2"

    def test_psf_file(self):
        key = "q1/VIS/2681/EUC_VIS_GRD-PSF-000-000000-0000000__20240322T192915.424564Z.fits"
        rec = _parse_vis_filename(key)
        assert rec is not None
        assert rec.file_type == "PSF"

    def test_unknown_file(self):
        rec = _parse_vis_filename("q1/VIS/2681/README.txt")
        assert rec is None


class TestBuildObservationIndex:
    def test_with_moto(self, mock_s3):
        index = build_observation_index(
            vis_base_uri=f"s3://{MOCK_BUCKET}/q1/VIS/",
            s3_region=MOCK_REGION,
            s3_no_sign_request=False,  # moto doesn't need unsigned
        )
        assert len(index.obs_ids()) >= 1
        assert "2681" in index.obs_ids()

    def test_psf_found(self, mock_s3):
        index = build_observation_index(
            vis_base_uri=f"s3://{MOCK_BUCKET}/q1/VIS/",
            s3_region=MOCK_REGION,
            s3_no_sign_request=False,
        )
        psf = index.get_psf_key("2681")
        assert psf is not None
        assert "PSF" in psf

    def test_dither_ccd_pairs(self, mock_s3):
        index = build_observation_index(
            vis_base_uri=f"s3://{MOCK_BUCKET}/q1/VIS/",
            s3_region=MOCK_REGION,
            s3_no_sign_request=False,
        )
        pairs = index.get_dither_ccd_pairs("2681")
        assert len(pairs) >= 1
        # Should have DET files for dithers 00 and 01
        dithers = {d for d, _ in pairs}
        assert "00" in dithers


class TestBuildQuadrantRefs:
    def test_produces_refs(self, mock_s3):
        index = build_observation_index(
            vis_base_uri=f"s3://{MOCK_BUCKET}/q1/VIS/",
            s3_region=MOCK_REGION,
            s3_no_sign_request=False,
        )
        refs = build_quadrant_refs(index, "2681")
        assert len(refs) >= 1
        for ref in refs:
            assert "DET" in ref.sci_path
            assert ref.psf_path != ""


class TestListMerCatalog:
    def test_finds_catalog(self, mock_s3):
        path = list_mer_catalog(
            102018211,
            catalog_base_uri=f"s3://{MOCK_BUCKET}/q1/catalogs/MER_FINAL_CATALOG/",
            s3_region=MOCK_REGION,
            s3_no_sign_request=False,
        )
        assert path is not None
        assert "FINAL-CAT" in path
        assert "102018211" in path

    def test_missing_tile(self, mock_s3):
        path = list_mer_catalog(
            999999999,
            catalog_base_uri=f"s3://{MOCK_BUCKET}/q1/catalogs/MER_FINAL_CATALOG/",
            s3_region=MOCK_REGION,
            s3_no_sign_request=False,
        )
        assert path is None
