"""Tests for constellation.iceberg_catalog."""

from constellation.iceberg_catalog import (
    SHEAR_ICEBERG_SCHEMA,
    create_shear_table,
    get_catalog,
)


class TestGetCatalog:
    def test_creates_catalog(self, tmp_path):
        catalog = get_catalog(str(tmp_path / "warehouse"))
        assert catalog is not None
        assert (tmp_path / "warehouse" / "catalog.db").exists()


class TestCreateShearTable:
    def test_creates_table(self, tmp_path):
        wh = str(tmp_path / "warehouse")
        table = create_shear_table(wh)
        assert table is not None

    def test_schema_has_12_fields(self, tmp_path):
        wh = str(tmp_path / "warehouse")
        table = create_shear_table(wh)
        assert len(table.schema().fields) == 12

    def test_idempotent(self, tmp_path):
        wh = str(tmp_path / "warehouse")
        t1 = create_shear_table(wh)
        t2 = create_shear_table(wh)
        assert t1.name() == t2.name()

    def test_partitioned_by_tile_id(self, tmp_path):
        wh = str(tmp_path / "warehouse")
        table = create_shear_table(wh)
        spec = table.spec()
        assert len(spec.fields) == 1
        assert spec.fields[0].name == "TILE_ID"
