"""Tests for constellation.workflows (tasks + local_runner)."""

from constellation.discovery import ObservationIndex, VISFileRecord
from constellation.workflows.tasks import (
    assemble_results,
    build_obs_index,
    infer_subtile,
    prepare_tile,
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
