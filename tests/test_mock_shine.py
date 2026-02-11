"""Tests for constellation.mock_shine."""

from constellation.mock_shine import run_mock_inference


class TestRunMockInference:
    def test_returns_result(self, sample_manifest, tmp_path):
        path = tmp_path / "manifest.yaml"
        sample_manifest.to_yaml(path)
        result = run_mock_inference(str(path))
        assert result.TILE_ID == sample_manifest.tile_id
        assert result.SUBTILE_ROW == sample_manifest.sub_tile_row
        assert result.METHOD == "mock"

    def test_n_sources_matches_core(self, sample_manifest, tmp_path):
        path = tmp_path / "manifest.yaml"
        sample_manifest.to_yaml(path)
        result = run_mock_inference(str(path))
        assert result.N_SOURCES == len(sample_manifest.core_source_ids)

    def test_shear_in_range(self, sample_manifest, tmp_path):
        path = tmp_path / "manifest.yaml"
        sample_manifest.to_yaml(path)
        result = run_mock_inference(str(path))
        assert -0.3 < result.G1 < 0.3
        assert -0.3 < result.G2 < 0.3
        assert 0.005 <= result.G1_ERR <= 0.015
        assert 0.1 <= result.CONVERGENCE <= 0.5

    def test_deterministic(self, sample_manifest, tmp_path):
        path = tmp_path / "manifest.yaml"
        sample_manifest.to_yaml(path)
        r1 = run_mock_inference(str(path), seed=42)
        r2 = run_mock_inference(str(path), seed=42)
        assert r1.G1 == r2.G1
        assert r1.G2 == r2.G2

    def test_different_seeds_differ(self, sample_manifest, tmp_path):
        path = tmp_path / "manifest.yaml"
        sample_manifest.to_yaml(path)
        r1 = run_mock_inference(str(path), seed=42)
        r2 = run_mock_inference(str(path), seed=99)
        assert r1.G1 != r2.G1
