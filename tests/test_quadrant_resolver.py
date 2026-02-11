"""Tests for WCS-based quadrant resolution."""

from __future__ import annotations

import pytest

from constellation.quadrant_resolver import (
    QuadrantFootprint,
    build_quadrant_index,
    footprint_overlaps,
    quadrant_index_from_dict,
    quadrant_index_to_dict,
    read_quadrant_footprints,
    resolve_quadrants_for_subtile,
)
from constellation.schemas import SkyBounds

from tests.conftest import MOCK_FIELD_DEC, MOCK_FIELD_RA, MOCK_QUADRANT_NAMES


class TestReadQuadrantFootprints:
    def test_finds_correct_hdus(self, mock_vis_fits_path):
        """Should discover all SCI extensions and return valid bounding boxes."""
        footprints = read_quadrant_footprints(mock_vis_fits_path, anon=False)

        assert len(footprints) == len(MOCK_QUADRANT_NAMES)
        for fp in footprints:
            assert fp.quadrant in MOCK_QUADRANT_NAMES
            assert fp.ra_min < fp.ra_max
            assert fp.dec_min < fp.dec_max
            assert fp.det_path == mock_vis_fits_path

    def test_bounding_boxes_near_field_center(self, mock_vis_fits_path):
        """Bounding boxes should be near the mock field center."""
        footprints = read_quadrant_footprints(mock_vis_fits_path, anon=False)

        for fp in footprints:
            # The mock WCS is centered near MOCK_FIELD_RA/DEC
            assert abs((fp.ra_min + fp.ra_max) / 2 - MOCK_FIELD_RA) < 1.0
            assert abs((fp.dec_min + fp.dec_max) / 2 - MOCK_FIELD_DEC) < 1.0

    def test_empty_file_returns_empty(self, tmp_path):
        """A FITS file with no SCI extensions should return empty list."""
        from astropy.io import fits

        path = tmp_path / "empty.fits"
        hdul = fits.HDUList([fits.PrimaryHDU()])
        hdul.writeto(str(path))

        footprints = read_quadrant_footprints(str(path), anon=False)
        assert footprints == []


class TestFootprintOverlaps:
    def test_overlapping(self):
        """A footprint that covers the sub-tile area should overlap."""
        fp = QuadrantFootprint(
            quadrant="1-1.A",
            ra_min=52.0,
            ra_max=53.0,
            dec_min=-29.0,
            dec_max=-28.0,
            det_path="dummy",
        )
        bounds = SkyBounds(
            core_ra=(52.3, 52.7),
            core_dec=(-28.7, -28.3),
            extended_ra=(52.2, 52.8),
            extended_dec=(-28.8, -28.2),
        )
        assert footprint_overlaps(fp, bounds) is True

    def test_no_overlap_ra(self):
        """A footprint entirely outside in RA should not overlap."""
        fp = QuadrantFootprint(
            quadrant="1-1.A",
            ra_min=60.0,
            ra_max=61.0,
            dec_min=-29.0,
            dec_max=-28.0,
            det_path="dummy",
        )
        bounds = SkyBounds(
            core_ra=(52.3, 52.7),
            core_dec=(-28.7, -28.3),
            extended_ra=(52.2, 52.8),
            extended_dec=(-28.8, -28.2),
        )
        assert footprint_overlaps(fp, bounds) is False

    def test_no_overlap_dec(self):
        """A footprint entirely outside in Dec should not overlap."""
        fp = QuadrantFootprint(
            quadrant="1-1.A",
            ra_min=52.0,
            ra_max=53.0,
            dec_min=-20.0,
            dec_max=-19.0,
            det_path="dummy",
        )
        bounds = SkyBounds(
            core_ra=(52.3, 52.7),
            core_dec=(-28.7, -28.3),
            extended_ra=(52.2, 52.8),
            extended_dec=(-28.8, -28.2),
        )
        assert footprint_overlaps(fp, bounds) is False

    def test_partial_overlap(self):
        """A footprint partially overlapping should return True."""
        fp = QuadrantFootprint(
            quadrant="1-1.A",
            ra_min=52.7,  # overlaps right edge
            ra_max=53.5,
            dec_min=-28.5,
            dec_max=-27.5,
            det_path="dummy",
        )
        bounds = SkyBounds(
            core_ra=(52.3, 52.7),
            core_dec=(-28.7, -28.3),
            extended_ra=(52.2, 52.8),
            extended_dec=(-28.8, -28.2),
        )
        assert footprint_overlaps(fp, bounds) is True


class TestBuildQuadrantIndex:
    def test_build_from_mock_s3(self, mock_s3_with_fits):
        """Build quadrant index from moto S3 with valid FITS."""
        from constellation.discovery import build_observation_index

        # moto does not support UNSIGNED (anon) for get_object, so use signed
        obs_index = build_observation_index(
            vis_base_uri="s3://nasa-irsa-euclid-q1/q1/VIS/",
            s3_region="us-east-1",
            s3_no_sign_request=False,
        )

        index = build_quadrant_index(obs_index, s3_anon=False)

        # 4 DET files × 2 quadrants each = 8 footprints
        assert len(index) == 8
        for fp in index:
            assert fp.quadrant in MOCK_QUADRANT_NAMES
            assert fp.ra_min < fp.ra_max
            assert fp.dec_min < fp.dec_max
            assert fp.obs_id != ""
            assert fp.bkg_path != ""
            assert fp.wgt_path != ""
            assert fp.psf_path != ""


class TestResolveQuadrantsForSubtile:
    def test_correct_filtering(self):
        """Only overlapping quadrants should be returned."""
        index = [
            QuadrantFootprint(
                quadrant="overlap",
                ra_min=52.0,
                ra_max=53.0,
                dec_min=-29.0,
                dec_max=-28.0,
                det_path="det.fits",
                bkg_path="bkg.fits",
                wgt_path="wgt.fits",
                psf_path="psf.fits",
                obs_id="001",
                dither="00",
                ccd="1",
            ),
            QuadrantFootprint(
                quadrant="no_overlap",
                ra_min=100.0,
                ra_max=101.0,
                dec_min=10.0,
                dec_max=11.0,
                det_path="det2.fits",
                bkg_path="bkg2.fits",
                wgt_path="wgt2.fits",
                psf_path="psf2.fits",
                obs_id="002",
                dither="01",
                ccd="2",
            ),
        ]
        bounds = SkyBounds(
            core_ra=(52.3, 52.7),
            core_dec=(-28.7, -28.3),
            extended_ra=(52.2, 52.8),
            extended_dec=(-28.8, -28.2),
        )

        refs = resolve_quadrants_for_subtile(index, bounds)

        assert len(refs) == 1
        assert refs[0].quadrant == "overlap"
        assert refs[0].sci_path == "det.fits"
        assert refs[0].obs_id == "001"

    def test_empty_index_returns_empty(self):
        """An empty index should return no quadrants."""
        bounds = SkyBounds(
            core_ra=(52.3, 52.7),
            core_dec=(-28.7, -28.3),
            extended_ra=(52.2, 52.8),
            extended_dec=(-28.8, -28.2),
        )
        refs = resolve_quadrants_for_subtile([], bounds)
        assert refs == []


class TestS3SeekableFile:
    def test_range_reads_only_headers(self, mock_s3_with_fits):
        """_S3SeekableFile should serve reads from range requests, not full download."""
        from constellation.quadrant_resolver import _S3SeekableFile, _get_s3_client

        s3 = _get_s3_client(anon=False)
        bucket = "nasa-irsa-euclid-q1"
        key = "q1/VIS/2681/EUC_VIS_SWL-DET-002681-00-1-0000000__20241017T042839.727728Z.fits"

        head = s3.head_object(Bucket=bucket, Key=key)
        size = head["ContentLength"]

        f = _S3SeekableFile(bucket, key, s3, size)

        # Read a small chunk from the start (FITS primary header)
        data = f.read(2880)
        assert len(data) == 2880
        assert f.tell() == 2880

        # Seek forward (simulates skipping a data block)
        f.seek(size // 2)
        assert f.tell() == size // 2

        # Seek to end
        f.seek(0, 2)
        assert f.tell() == size

        # Read at EOF returns empty
        assert f.read(10) == b""

        f.close()

    def test_seekable_fits_open(self, mock_s3_with_fits):
        """astropy.io.fits.open should work with _S3SeekableFile."""
        from astropy.io import fits

        from constellation.quadrant_resolver import _S3SeekableFile, _get_s3_client

        s3 = _get_s3_client(anon=False)
        bucket = "nasa-irsa-euclid-q1"
        key = "q1/VIS/2681/EUC_VIS_SWL-DET-002681-00-1-0000000__20241017T042839.727728Z.fits"

        head = s3.head_object(Bucket=bucket, Key=key)
        size = head["ContentLength"]

        f = _S3SeekableFile(bucket, key, s3, size)
        with fits.open(f, lazy_load_hdus=True) as hdul:
            names = [h.name for h in hdul]
            assert "3-4.F.SCI" in names
            assert "3-5.E.SCI" in names


class TestSerialization:
    def test_round_trip(self):
        """Serialization to dict and back should preserve all fields."""
        index = [
            QuadrantFootprint(
                quadrant="3-4.F",
                ra_min=52.0,
                ra_max=53.0,
                dec_min=-29.0,
                dec_max=-28.0,
                det_path="s3://bucket/det.fits",
                bkg_path="s3://bucket/bkg.fits",
                wgt_path="s3://bucket/wgt.fits",
                psf_path="s3://bucket/psf.fits",
                obs_id="002681",
                dither="00",
                ccd="1",
            ),
        ]

        data = quadrant_index_to_dict(index)
        restored = quadrant_index_from_dict(data)

        assert len(restored) == 1
        fp = restored[0]
        assert fp.quadrant == "3-4.F"
        assert fp.ra_min == 52.0
        assert fp.ra_max == 53.0
        assert fp.dec_min == -29.0
        assert fp.dec_max == -28.0
        assert fp.det_path == "s3://bucket/det.fits"
        assert fp.obs_id == "002681"
