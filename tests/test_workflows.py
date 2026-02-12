"""Tests for constellation.workflows (Flyte tasks)."""

import json

from constellation.discovery import ObservationIndex, VISFileRecord
from constellation.workflows.tasks import (
    assemble_results,
    build_det_work_items,
    build_obs_index,
    extract_subtile_task,
    infer_subtile,
    merge_footprints,
    prepare_tile,
    read_det_footprints,
    validate_results,
)


class TestObservationIndexSerialization:
    def test_round_trip(self):
        rec = VISFileRecord(
            file_type="DET",
            obs_id="002681",
            dither="00",
            ccd="1",
            timestamp="20241017T042839.727728Z",
            s3_key="q1/VIS/2681/EUC_VIS_SWL-DET-002681-00-1-0000000__20241017T042839.727728Z.fits",
        )
        index = ObservationIndex(bucket="test-bucket", records={"2681": [rec]})

        d = index.to_dict()
        restored = ObservationIndex.from_dict(d)

        assert restored.bucket == "test-bucket"
        assert len(restored.records["2681"]) == 1
        assert restored.records["2681"][0].file_type == "DET"
        assert restored.records["2681"][0].s3_key == rec.s3_key

    def test_empty_index(self):
        index = ObservationIndex(bucket="b")
        d = index.to_dict()
        restored = ObservationIndex.from_dict(d)
        assert restored.obs_ids() == []


class TestPrepareTile:
    def test_prepare_returns_manifests(self, mock_s3, sample_config, tmp_path):
        from constellation.discovery import build_observation_index

        config = sample_config
        config_path = tmp_path / "config.yaml"
        config.to_yaml(config_path)

        obs_index = build_observation_index(
            vis_base_uri=config.data.vis_base_uri,
            s3_region=config.data.s3_region,
            s3_no_sign_request=False,
        )
        obs_dict = obs_index.to_dict()

        paths = prepare_tile(
            tile_id=config.tile_ids[0],
            config_yaml=str(config_path),
            obs_index_dict=obs_dict,
        )
        # 4x4 grid = 16 manifests
        assert len(paths) == 16


class TestBuildDetWorkItems:
    def test_returns_work_items(self):
        """build_det_work_items enumerates all DET files from obs index."""
        det_rec = VISFileRecord(
            file_type="DET",
            obs_id="002681",
            dither="00",
            ccd="1",
            timestamp="20241017T042839.727728Z",
            s3_key="q1/VIS/2681/EUC_VIS_SWL-DET-002681-00-1-0000000__20241017T042839.727728Z.fits",
        )
        bkg_rec = VISFileRecord(
            file_type="BKG",
            obs_id="002681",
            dither="00",
            ccd="1",
            timestamp="20241017T042839.727792Z",
            s3_key="q1/VIS/2681/EUC_VIS_SWL-BKG-002681-00-1-0000000__20241017T042839.727792Z.fits",
        )
        wgt_rec = VISFileRecord(
            file_type="WGT",
            obs_id="002681",
            dither="00",
            ccd="1",
            timestamp="20241017T042839.727798Z",
            s3_key="q1/VIS/2681/EUC_VIS_SWL-WGT-002681-00-1-0000000__20241017T042839.727798Z.fits",
        )
        psf_rec = VISFileRecord(
            file_type="PSF",
            obs_id="002681",
            dither="00",
            ccd="0",
            timestamp="20240322T192915.424564Z",
            s3_key="q1/VIS/2681/EUC_VIS_GRD-PSF-000-000000-0000000__20240322T192915.424564Z.fits",
        )
        index = ObservationIndex(
            bucket="test-bucket",
            records={"2681": [det_rec, bkg_rec, wgt_rec, psf_rec]},
        )
        obs_dict = index.to_dict()

        items = build_det_work_items(
            config_yaml="unused",
            obs_index_dict=obs_dict,
        )

        assert len(items) == 1
        item = items[0]
        assert item["obs_id"] == "2681"
        assert item["dither"] == "00"
        assert item["ccd"] == "1"
        assert "DET" in item["det_path"]
        assert "BKG" in item["bkg_path"]
        assert "WGT" in item["wgt_path"]
        assert "PSF" in item["psf_path"]

    def test_multiple_dither_ccd_pairs(self):
        """Multiple (dither, ccd) combos produce multiple work items."""
        recs = []
        for dither in ["00", "01"]:
            for ccd in ["1", "2"]:
                recs.append(
                    VISFileRecord(
                        file_type="DET",
                        obs_id="002681",
                        dither=dither,
                        ccd=ccd,
                        timestamp="20241017T042839.727728Z",
                        s3_key=f"q1/VIS/2681/EUC_VIS_SWL-DET-002681-{dither}-{ccd}-0000000__20241017T042839.727728Z.fits",
                    )
                )
        index = ObservationIndex(bucket="b", records={"2681": recs})

        items = build_det_work_items(
            config_yaml="unused",
            obs_index_dict=index.to_dict(),
        )
        assert len(items) == 4


class TestReadDetFootprints:
    def test_reads_footprints_from_local_fits(self, mock_vis_fits_path):
        """read_det_footprints returns a FlyteFile with footprints JSON."""
        work_item = {
            "det_path": mock_vis_fits_path,
            "bkg_path": "/tmp/bkg.fits",
            "wgt_path": "/tmp/wgt.fits",
            "psf_path": "/tmp/psf.fits",
            "obs_id": "002681",
            "dither": "00",
            "ccd": "1",
        }

        result_file = read_det_footprints(work_item=work_item)

        # Read back the JSON file
        with open(str(result_file)) as f:
            footprints = json.load(f)

        # Mock FITS has 2 quadrants (3-4.F and 3-5.E)
        assert len(footprints) == 2
        for fp in footprints:
            assert "quadrant" in fp
            assert "ra_min" in fp
            assert "ra_max" in fp
            assert "dec_min" in fp
            assert "dec_max" in fp
            assert fp["bkg_path"] == "/tmp/bkg.fits"
            assert fp["wgt_path"] == "/tmp/wgt.fits"
            assert fp["psf_path"] == "/tmp/psf.fits"
            assert fp["obs_id"] == "002681"

    def test_returns_empty_on_missing_file(self, tmp_path):
        """read_det_footprints returns an empty JSON file when DET file doesn't exist."""
        work_item = {
            "det_path": str(tmp_path / "nonexistent.fits"),
            "bkg_path": "",
            "wgt_path": "",
            "psf_path": "",
            "obs_id": "000000",
            "dither": "00",
            "ccd": "1",
        }

        result_file = read_det_footprints(work_item=work_item)
        with open(str(result_file)) as f:
            footprints = json.load(f)
        assert footprints == []


class TestMergeFootprints:
    def test_flattens_files(self, tmp_path):
        """merge_footprints concatenates per-DET JSON files."""
        fp1 = [{"quadrant": "3-4.F", "ra_min": 52.0, "ra_max": 53.0,
                 "dec_min": -28.5, "dec_max": -27.5, "det_path": "a.fits",
                 "bkg_path": "", "wgt_path": "", "psf_path": "",
                 "obs_id": "1", "dither": "00", "ccd": "1"}]
        fp2 = [{"quadrant": "3-5.E", "ra_min": 52.5, "ra_max": 53.5,
                 "dec_min": -28.0, "dec_max": -27.0, "det_path": "b.fits",
                 "bkg_path": "", "wgt_path": "", "psf_path": "",
                 "obs_id": "2", "dither": "01", "ccd": "2"}]

        f1 = tmp_path / "fp1.json"
        f2 = tmp_path / "fp2.json"
        f1.write_text(json.dumps(fp1))
        f2.write_text(json.dumps(fp2))

        merged_file = merge_footprints(footprint_files=[str(f1), str(f2)])
        with open(str(merged_file)) as f:
            merged = json.load(f)

        assert len(merged) == 2
        assert merged[0]["quadrant"] == "3-4.F"
        assert merged[1]["quadrant"] == "3-5.E"

    def test_handles_empty_files(self, tmp_path):
        """merge_footprints handles empty per-DET JSON files."""
        f1 = tmp_path / "fp1.json"
        f2 = tmp_path / "fp2.json"
        f1.write_text(json.dumps([]))
        f2.write_text(json.dumps([]))

        merged_file = merge_footprints(footprint_files=[str(f1), str(f2)])
        with open(str(merged_file)) as f:
            merged = json.load(f)
        assert merged == []


class TestExtractSubtileTask:
    def test_extract_returns_directory(
        self, sample_manifest, sample_config, tmp_path
    ):
        """extract_subtile_task returns the extracted sub-tile directory."""
        manifest_path = tmp_path / "102018211_0_0.yaml"
        sample_manifest.to_yaml(manifest_path)

        config_path = tmp_path / "config.yaml"
        sample_config.output.extraction_dir = str(tmp_path / "subtiles")
        sample_config.to_yaml(config_path)

        subtile_dir = extract_subtile_task(
            manifest_path=str(manifest_path),
            config_yaml=str(config_path),
        )
        assert "102018211" in subtile_dir
        assert "0_0" in subtile_dir


class TestInferSubtile:
    def test_infer_returns_parquet(self, sample_manifest, sample_config, tmp_path):
        # Write manifest
        manifest_path = tmp_path / "manifest.yaml"
        sample_manifest.to_yaml(manifest_path)

        # Write config
        config_path = tmp_path / "config.yaml"
        sample_config.to_yaml(config_path)

        result_path = infer_subtile(
            manifest_path=str(manifest_path),
            config_yaml=str(config_path),
        )
        assert str(result_path).endswith(".parquet")

    def test_infer_with_extracted_dir(
        self, sample_manifest, sample_config, tmp_path
    ):
        """infer_subtile accepts extracted_dir for data dependency."""
        manifest_path = tmp_path / "manifest.yaml"
        sample_manifest.to_yaml(manifest_path)

        config_path = tmp_path / "config.yaml"
        sample_config.to_yaml(config_path)

        result_path = infer_subtile(
            manifest_path=str(manifest_path),
            config_yaml=str(config_path),
            extracted_dir="/tmp/fake_extracted",
        )
        assert str(result_path).endswith(".parquet")


class TestAssembleAndValidate:
    def test_assemble_and_validate(self, sample_result, sample_config, tmp_path):
        from constellation.result_writer import write_subtile_result

        config = sample_config
        config_path = tmp_path / "config.yaml"
        config.to_yaml(config_path)

        result_dir = str(tmp_path / "results")
        p = write_subtile_result(sample_result, result_dir)

        n = assemble_results(
            result_paths=[p],
            config_yaml=str(config_path),
        )
        assert n == 1

        stats = validate_results(
            config_yaml=str(config_path),
            expected_subtiles=1,
        )
        assert stats["row_count"] == 1
        assert stats["completeness"] == 1.0
