"""PyIceberg table management for the shear catalog.

Uses a local SQLite catalog for development. Production would swap
to AWS Glue or Project Nessie by changing catalog configuration only.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

logger = logging.getLogger(__name__)

SHEAR_ICEBERG_SCHEMA = Schema(
    NestedField(1, "TILE_ID", LongType(), required=True),
    NestedField(2, "SUBTILE_ROW", IntegerType(), required=True),
    NestedField(3, "SUBTILE_COL", IntegerType(), required=True),
    NestedField(4, "RA", DoubleType(), required=True),
    NestedField(5, "DEC", DoubleType(), required=True),
    NestedField(6, "G1", FloatType(), required=True),
    NestedField(7, "G2", FloatType(), required=True),
    NestedField(8, "G1_ERR", FloatType(), required=True),
    NestedField(9, "G2_ERR", FloatType(), required=True),
    NestedField(10, "N_SOURCES", IntegerType(), required=True),
    NestedField(11, "METHOD", StringType(), required=True),
    NestedField(12, "CONVERGENCE", FloatType(), required=True),
)

SHEAR_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1000,
        transform=IdentityTransform(),
        name="TILE_ID",
    )
)


def get_catalog(warehouse: str) -> SqlCatalog:
    """Load or create a PyIceberg SQLite catalog.

    Args:
        warehouse: Local filesystem path for the Iceberg warehouse.
    """
    warehouse_path = Path(warehouse)
    warehouse_path.mkdir(parents=True, exist_ok=True)

    db_path = warehouse_path / "catalog.db"
    return SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{db_path}",
            "warehouse": str(warehouse_path),
        },
    )


def create_shear_table(
    warehouse: str,
    namespace: str = "shear",
    table_name: str = "edff_shear",
) -> Table:
    """Create the Iceberg shear catalog table if it doesn't exist.

    Returns the Table object (existing or newly created).
    """
    catalog = get_catalog(warehouse)

    # Ensure namespace exists
    existing_ns = [ns[0] for ns in catalog.list_namespaces()]
    if namespace not in existing_ns:
        catalog.create_namespace(namespace)
        logger.info("Created namespace '%s'", namespace)

    full_name = f"{namespace}.{table_name}"

    existing_tables = [t[1] for t in catalog.list_tables(namespace)]
    if table_name in existing_tables:
        logger.info("Table '%s' already exists", full_name)
        return catalog.load_table(full_name)

    table = catalog.create_table(
        identifier=full_name,
        schema=SHEAR_ICEBERG_SCHEMA,
        partition_spec=SHEAR_PARTITION_SPEC,
    )
    logger.info("Created Iceberg table '%s'", full_name)
    return table
