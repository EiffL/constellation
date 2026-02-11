"""Catalog assembly: merge per-sub-tile Parquet results into Iceberg table."""

from __future__ import annotations

import logging

import pyarrow as pa
import pyarrow.parquet as pq

from constellation.iceberg_catalog import create_shear_table, get_catalog

logger = logging.getLogger(__name__)


def assemble_catalog(
    result_paths: list[str],
    warehouse: str,
    namespace: str = "shear",
    table_name: str = "edff_shear",
) -> int:
    """Read all sub-tile result Parquet files and append to Iceberg table.

    Args:
        result_paths: List of Parquet file paths to ingest.
        warehouse: Iceberg warehouse path.
        namespace: Iceberg namespace.
        table_name: Iceberg table name.

    Returns:
        Number of rows written.
    """
    table = create_shear_table(warehouse, namespace, table_name)

    tables = []
    for path in result_paths:
        t = pq.read_table(path)
        tables.append(t)

    if not tables:
        logger.warning("No result files to assemble")
        return 0

    combined = pa.concat_tables(tables)
    table.append(combined)

    n_rows = len(combined)
    logger.info(
        "Appended %d rows to %s.%s", n_rows, namespace, table_name
    )
    return n_rows


def validate_catalog(
    warehouse: str,
    namespace: str = "shear",
    table_name: str = "edff_shear",
    expected_subtiles: int = 0,
) -> dict:
    """Run quality checks on the assembled catalog.

    Returns:
        Dictionary with summary statistics:
        - row_count: total rows
        - tile_count: distinct TILE_ID values
        - g1_mean, g2_mean: should be near 0 for mock
        - g1_std, g2_std: standard deviations
        - completeness: fraction of expected sub-tiles present (if expected > 0)
    """
    catalog = get_catalog(warehouse)
    full_name = f"{namespace}.{table_name}"
    table = catalog.load_table(full_name)

    scan = table.scan()
    arrow_table = scan.to_arrow()

    row_count = len(arrow_table)
    tile_ids = arrow_table.column("TILE_ID").to_pylist()
    g1_vals = arrow_table.column("G1").to_pylist()
    g2_vals = arrow_table.column("G2").to_pylist()

    import numpy as np

    g1_arr = np.array(g1_vals, dtype=np.float64)
    g2_arr = np.array(g2_vals, dtype=np.float64)

    stats = {
        "row_count": row_count,
        "tile_count": len(set(tile_ids)),
        "g1_mean": float(np.mean(g1_arr)) if row_count > 0 else 0.0,
        "g2_mean": float(np.mean(g2_arr)) if row_count > 0 else 0.0,
        "g1_std": float(np.std(g1_arr)) if row_count > 0 else 0.0,
        "g2_std": float(np.std(g2_arr)) if row_count > 0 else 0.0,
    }

    if expected_subtiles > 0:
        stats["completeness"] = row_count / expected_subtiles
    else:
        stats["completeness"] = 1.0 if row_count > 0 else 0.0

    return stats
