"""Tests for constellation.result_writer."""

import pyarrow.parquet as pq

from constellation.result_writer import (
    result_to_table,
    results_to_table,
    write_subtile_result,
)
from constellation.schemas import SHEAR_CATALOG_SCHEMA


class TestResultToTable:
    def test_single_row(self, sample_result):
        table = result_to_table(sample_result)
        assert len(table) == 1
        assert table.schema.equals(SHEAR_CATALOG_SCHEMA)

    def test_values_match(self, sample_result):
        table = result_to_table(sample_result)
        assert abs(table.column("G1")[0].as_py() - sample_result.G1) < 1e-6


class TestResultsToTable:
    def test_multiple_rows(self, sample_result):
        table = results_to_table([sample_result, sample_result])
        assert len(table) == 2

    def test_empty_list(self):
        table = results_to_table([])
        assert len(table) == 0
        assert table.schema.equals(SHEAR_CATALOG_SCHEMA)


class TestWriteSubtileResult:
    def test_writes_parquet(self, sample_result, tmp_path):
        path = write_subtile_result(sample_result, str(tmp_path))
        assert path.endswith(".parquet")

        table = pq.read_table(path)
        assert len(table) == 1
        assert table.column("TILE_ID")[0].as_py() == sample_result.TILE_ID
