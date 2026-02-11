"""Parquet result writer for per-sub-tile inference results."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from constellation.schemas import SHEAR_CATALOG_SCHEMA, SubTileResult


def result_to_table(result: SubTileResult) -> pa.Table:
    """Convert a single SubTileResult to a one-row PyArrow Table."""
    data = result.model_dump()
    arrays = {
        name: [data[name]] for name in SHEAR_CATALOG_SCHEMA.names
    }
    return pa.table(arrays, schema=SHEAR_CATALOG_SCHEMA)


def results_to_table(results: list[SubTileResult]) -> pa.Table:
    """Convert a list of SubTileResult to a PyArrow Table."""
    if not results:
        return pa.table(
            {name: [] for name in SHEAR_CATALOG_SCHEMA.names},
            schema=SHEAR_CATALOG_SCHEMA,
        )
    tables = [result_to_table(r) for r in results]
    return pa.concat_tables(tables)


def write_subtile_result(result: SubTileResult, output_dir: str) -> str:
    """Write a single sub-tile result as a Parquet file.

    File name: {TILE_ID}_{SUBTILE_ROW}_{SUBTILE_COL}.parquet

    Returns:
        Path to the written Parquet file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = f"{result.TILE_ID}_{result.SUBTILE_ROW}_{result.SUBTILE_COL}.parquet"
    filepath = output_path / filename

    table = result_to_table(result)
    pq.write_table(table, str(filepath))

    return str(filepath)
