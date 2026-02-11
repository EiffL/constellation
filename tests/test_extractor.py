"""Tests for FITS extraction and catalog subsetting."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
from astropy.io import fits
from astropy.table import Table

from constellation.extractor import (
    extract_psf_fits,
    extract_quadrant_fits,
    extract_subtile,
    subset_catalog,
)
from constellation.schemas import QuadrantRef, SkyBounds, SubTileManifest

from tests.conftest import (
    MOCK_FIELD_DEC,
    MOCK_FIELD_RA,
    MOCK_QUADRANT_NAMES,
)


class TestExtractQuadrantFits:
    def test_output_has_correct_hdus(self, mock_vis_fits_path, tmp_path):
        """Extracted file should have SCI, RMS, FLG for the quadrant."""
        dest = tmp_path / "extracted.fits"
        qname = MOCK_QUADRANT_NAMES[0]

        extract_quadrant_fits(mock_vis_fits_path, qname, dest)

        assert dest.exists()
        with fits.open(str(dest)) as hdul:
            names = [h.name for h in hdul]
            assert f"{qname}.SCI" in names
            assert f"{qname}.RMS" in names
            assert f"{qname}.FLG" in names

    def test_data_preserved(self, mock_vis_fits_path, tmp_path):
        """Extracted data should match the source file."""
        dest = tmp_path / "extracted.fits"
        qname = MOCK_QUADRANT_NAMES[0]

        extract_quadrant_fits(mock_vis_fits_path, qname, dest)

        with fits.open(mock_vis_fits_path) as src_hdul:
            src_data = src_hdul[f"{qname}.SCI"].data

        with fits.open(str(dest)) as dst_hdul:
            dst_data = dst_hdul[f"{qname}.SCI"].data

        np.testing.assert_array_equal(src_data, dst_data)

    def test_missing_hdu_still_writes(self, mock_vis_fits_path, tmp_path):
        """If a quadrant doesn't exist, the file is still created (empty)."""
        dest = tmp_path / "extracted.fits"

        extract_quadrant_fits(mock_vis_fits_path, "NONEXISTENT", dest)

        assert dest.exists()
        with fits.open(str(dest)) as hdul:
            # Only the primary HDU should exist
            assert len(hdul) == 1


class TestExtractPsfFits:
    def test_output_has_psf_extension(self, mock_psf_fits_path, tmp_path):
        """Extracted PSF file should have the quadrant extension."""
        dest = tmp_path / "psf_extracted.fits"
        qname = MOCK_QUADRANT_NAMES[0]

        extract_psf_fits(mock_psf_fits_path, qname, dest)

        assert dest.exists()
        with fits.open(str(dest)) as hdul:
            names = [h.name for h in hdul]
            assert qname in names

    def test_psf_data_preserved(self, mock_psf_fits_path, tmp_path):
        """Extracted PSF data should match the source."""
        dest = tmp_path / "psf_extracted.fits"
        qname = MOCK_QUADRANT_NAMES[0]

        extract_psf_fits(mock_psf_fits_path, qname, dest)

        with fits.open(mock_psf_fits_path) as src:
            src_data = src[qname].data

        with fits.open(str(dest)) as dst:
            dst_data = dst[qname].data

        np.testing.assert_array_equal(src_data, dst_data)


class TestSubsetCatalog:
    def test_correct_row_filtering(self, mock_catalog_fits_path, tmp_path):
        """Should filter catalog to sources within bounds."""
        dest = tmp_path / "subset.fits"

        # Bounds covering roughly the center of the mock catalog
        ext_bounds = ((MOCK_FIELD_RA - 0.1, MOCK_FIELD_RA + 0.1),
                      (MOCK_FIELD_DEC - 0.1, MOCK_FIELD_DEC + 0.1))
        core_bounds = ((MOCK_FIELD_RA - 0.05, MOCK_FIELD_RA + 0.05),
                       (MOCK_FIELD_DEC - 0.05, MOCK_FIELD_DEC + 0.05))

        all_ids, core_ids = subset_catalog(
            mock_catalog_fits_path,
            sky_bounds_extended=ext_bounds,
            sky_bounds_core=core_bounds,
            dest_path=dest,
        )

        assert dest.exists()
        assert len(all_ids) > 0
        assert len(core_ids) > 0
        assert len(core_ids) <= len(all_ids)

        # Verify the written file has the right number of rows
        subset = Table.read(str(dest))
        assert len(subset) == len(all_ids)

    def test_core_is_subset_of_extended(self, mock_catalog_fits_path, tmp_path):
        """Core source IDs should be a subset of extended IDs."""
        dest = tmp_path / "subset.fits"

        ext_bounds = ((MOCK_FIELD_RA - 0.3, MOCK_FIELD_RA + 0.3),
                      (MOCK_FIELD_DEC - 0.3, MOCK_FIELD_DEC + 0.3))
        core_bounds = ((MOCK_FIELD_RA - 0.1, MOCK_FIELD_RA + 0.1),
                       (MOCK_FIELD_DEC - 0.1, MOCK_FIELD_DEC + 0.1))

        all_ids, core_ids = subset_catalog(
            mock_catalog_fits_path,
            sky_bounds_extended=ext_bounds,
            sky_bounds_core=core_bounds,
            dest_path=dest,
        )

        assert set(core_ids).issubset(set(all_ids))

    def test_empty_bounds_returns_empty(self, mock_catalog_fits_path, tmp_path):
        """Bounds far from any sources should return empty lists."""
        dest = tmp_path / "subset.fits"

        ext_bounds = ((200.0, 201.0), (80.0, 81.0))
        core_bounds = ((200.0, 200.5), (80.0, 80.5))

        all_ids, core_ids = subset_catalog(
            mock_catalog_fits_path,
            sky_bounds_extended=ext_bounds,
            sky_bounds_core=core_bounds,
            dest_path=dest,
        )

        assert len(all_ids) == 0
        assert len(core_ids) == 0


class TestExtractSubtile:
    def test_full_directory_structure(
        self, mock_vis_fits_path, mock_psf_fits_path, mock_catalog_fits_path, tmp_path
    ):
        """extract_subtile should create the expected directory layout."""
        # Create a manifest pointing to local files
        qname = MOCK_QUADRANT_NAMES[0]
        manifest = SubTileManifest(
            tile_id=102018211,
            sub_tile_row=0,
            sub_tile_col=0,
            sky_bounds=SkyBounds(
                core_ra=(MOCK_FIELD_RA - 0.05, MOCK_FIELD_RA + 0.05),
                core_dec=(MOCK_FIELD_DEC - 0.05, MOCK_FIELD_DEC + 0.05),
                extended_ra=(MOCK_FIELD_RA - 0.1, MOCK_FIELD_RA + 0.1),
                extended_dec=(MOCK_FIELD_DEC - 0.1, MOCK_FIELD_DEC + 0.1),
            ),
            quadrants=[
                QuadrantRef(
                    sci_path=mock_vis_fits_path,
                    bkg_path=mock_vis_fits_path,
                    wgt_path=mock_vis_fits_path,
                    psf_path=mock_psf_fits_path,
                    quadrant=qname,
                    obs_id="002681",
                    dither="00",
                    ccd="1",
                ),
            ],
            source_catalog=mock_catalog_fits_path,
            source_ids=[],
            core_source_ids=[],
        )

        manifest_path = tmp_path / "manifest.yaml"
        manifest.to_yaml(manifest_path)

        extraction_dir = tmp_path / "subtiles"
        subtile_dir = extract_subtile(
            manifest_path, extraction_dir, s3_anon=False
        )

        subtile_path = Path(subtile_dir)
        assert subtile_path.exists()
        assert (subtile_path / "manifest.yaml").exists()
        assert (subtile_path / "catalog.fits").exists()
        assert (subtile_path / "exposures").is_dir()
        assert (subtile_path / "psf").is_dir()

        # Check that exposure files exist
        exposure_files = list((subtile_path / "exposures").glob("*.fits"))
        assert len(exposure_files) == 3  # sci, bkg, wgt

        # Check that PSF files exist
        psf_files = list((subtile_path / "psf").glob("*.fits"))
        assert len(psf_files) == 1

    def test_manifest_has_relative_paths(
        self, mock_vis_fits_path, mock_psf_fits_path, mock_catalog_fits_path, tmp_path
    ):
        """Rewritten manifest should use relative paths."""
        qname = MOCK_QUADRANT_NAMES[0]
        manifest = SubTileManifest(
            tile_id=102018211,
            sub_tile_row=1,
            sub_tile_col=2,
            sky_bounds=SkyBounds(
                core_ra=(MOCK_FIELD_RA - 0.05, MOCK_FIELD_RA + 0.05),
                core_dec=(MOCK_FIELD_DEC - 0.05, MOCK_FIELD_DEC + 0.05),
                extended_ra=(MOCK_FIELD_RA - 0.1, MOCK_FIELD_RA + 0.1),
                extended_dec=(MOCK_FIELD_DEC - 0.1, MOCK_FIELD_DEC + 0.1),
            ),
            quadrants=[
                QuadrantRef(
                    sci_path=mock_vis_fits_path,
                    bkg_path=mock_vis_fits_path,
                    wgt_path=mock_vis_fits_path,
                    psf_path=mock_psf_fits_path,
                    quadrant=qname,
                    obs_id="002681",
                    dither="00",
                    ccd="1",
                ),
            ],
            source_catalog=mock_catalog_fits_path,
            source_ids=[],
            core_source_ids=[],
        )

        manifest_path = tmp_path / "manifest.yaml"
        manifest.to_yaml(manifest_path)

        extraction_dir = tmp_path / "subtiles"
        subtile_dir = extract_subtile(
            manifest_path, extraction_dir, s3_anon=False
        )

        # Read the rewritten manifest
        new_manifest = SubTileManifest.from_yaml(
            Path(subtile_dir) / "manifest.yaml"
        )

        assert new_manifest.source_catalog == "catalog.fits"
        for qref in new_manifest.quadrants:
            assert qref.sci_path.startswith("exposures/")
            assert qref.bkg_path.startswith("exposures/")
            assert qref.wgt_path.startswith("exposures/")
            assert qref.psf_path.startswith("psf/")
            # Should not contain absolute paths or s3:// URIs
            assert not qref.sci_path.startswith("/")
            assert not qref.sci_path.startswith("s3://")

    def test_source_ids_populated(
        self, mock_vis_fits_path, mock_psf_fits_path, mock_catalog_fits_path, tmp_path
    ):
        """Rewritten manifest should have source IDs from catalog subset."""
        qname = MOCK_QUADRANT_NAMES[0]
        manifest = SubTileManifest(
            tile_id=102018211,
            sub_tile_row=0,
            sub_tile_col=0,
            sky_bounds=SkyBounds(
                core_ra=(MOCK_FIELD_RA - 0.1, MOCK_FIELD_RA + 0.1),
                core_dec=(MOCK_FIELD_DEC - 0.1, MOCK_FIELD_DEC + 0.1),
                extended_ra=(MOCK_FIELD_RA - 0.3, MOCK_FIELD_RA + 0.3),
                extended_dec=(MOCK_FIELD_DEC - 0.3, MOCK_FIELD_DEC + 0.3),
            ),
            quadrants=[
                QuadrantRef(
                    sci_path=mock_vis_fits_path,
                    bkg_path=mock_vis_fits_path,
                    wgt_path=mock_vis_fits_path,
                    psf_path=mock_psf_fits_path,
                    quadrant=qname,
                    obs_id="002681",
                    dither="00",
                    ccd="1",
                ),
            ],
            source_catalog=mock_catalog_fits_path,
            source_ids=[],
            core_source_ids=[],
        )

        manifest_path = tmp_path / "manifest.yaml"
        manifest.to_yaml(manifest_path)

        extraction_dir = tmp_path / "subtiles"
        subtile_dir = extract_subtile(
            manifest_path, extraction_dir, s3_anon=False
        )

        new_manifest = SubTileManifest.from_yaml(
            Path(subtile_dir) / "manifest.yaml"
        )

        assert len(new_manifest.source_ids) > 0
        assert len(new_manifest.core_source_ids) > 0
        assert set(new_manifest.core_source_ids).issubset(
            set(new_manifest.source_ids)
        )
