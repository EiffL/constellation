"""Tests for constellation.catalog_assembler."""

from constellation.catalog_assembler import assemble_catalog, validate_catalog
from constellation.result_writer import write_subtile_result
from constellation.schemas import SubTileResult


def _make_result(tile_id: int, row: int, col: int) -> SubTileResult:
    return SubTileResult(
        TILE_ID=tile_id,
        SUBTILE_ROW=row,
        SUBTILE_COL=col,
        RA=53.0,
        DEC=-28.0,
        G1=0.01,
        G2=-0.02,
        G1_ERR=0.008,
        G2_ERR=0.009,
        N_SOURCES=1500,
        METHOD="mock",
        CONVERGENCE=0.3,
    )


class TestAssembleCatalog:
    def test_assembles_files(self, tmp_path):
        wh = str(tmp_path / "warehouse")
        result_dir = str(tmp_path / "results")

        # Write 3 result files
        paths = []
        for i in range(3):
            result = _make_result(102018211, i, 0)
            p = write_subtile_result(result, result_dir)
            paths.append(p)

        n = assemble_catalog(paths, wh)
        assert n == 3

    def test_empty_input(self, tmp_path):
        wh = str(tmp_path / "warehouse")
        n = assemble_catalog([], wh)
        assert n == 0


class TestValidateCatalog:
    def test_validates_assembled(self, tmp_path):
        wh = str(tmp_path / "warehouse")
        result_dir = str(tmp_path / "results")

        paths = []
        for row in range(4):
            for col in range(4):
                result = _make_result(102018211, row, col)
                p = write_subtile_result(result, result_dir)
                paths.append(p)

        assemble_catalog(paths, wh)
        stats = validate_catalog(wh, expected_subtiles=16)

        assert stats["row_count"] == 16
        assert stats["tile_count"] == 1
        assert stats["completeness"] == 1.0
        assert abs(stats["g1_mean"] - 0.01) < 0.001
