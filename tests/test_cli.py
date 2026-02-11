"""Tests for constellation.cli."""

from click.testing import CliRunner

from constellation.cli import cli


class TestCliHelp:
    def test_main_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "constellation" in result.output

    def test_run_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "--config" in result.output
        assert "--local" in result.output

    def test_validate_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--help"])
        assert result.exit_code == 0
        assert "--expected" in result.output


class TestCliRun:
    def test_run_without_local_fails(self, sample_config, tmp_path):
        config_path = tmp_path / "config.yaml"
        sample_config.to_yaml(config_path)

        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--config", str(config_path)])
        assert result.exit_code != 0
        assert "not yet implemented" in result.output

    def test_run_missing_config(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--config", "/nonexistent.yaml"])
        assert result.exit_code != 0
