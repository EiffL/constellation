"""Data contract schemas: manifest (constellation→SHINE) and result (SHINE→constellation)."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import yaml
from pydantic import BaseModel


class SkyBounds(BaseModel):
    """Bounding box in sky coordinates (degrees)."""

    core_ra: tuple[float, float]
    core_dec: tuple[float, float]
    extended_ra: tuple[float, float]
    extended_dec: tuple[float, float]


class QuadrantRef(BaseModel):
    """Reference to a single VIS quadrant's data products on S3."""

    sci_path: str
    bkg_path: str
    wgt_path: str
    psf_path: str
    quadrant: str  # HDU name prefix, e.g. "3-4.F"
    obs_id: str = ""
    dither: str = ""
    ccd: str = ""


class SubTileManifest(BaseModel):
    """Per-sub-tile manifest: the contract between constellation and SHINE.

    Produced by constellation, consumed by SHINE. Defines one sub-tile's
    inputs: which quadrant FITS files to load, which sources to model,
    and the sky bounds for core/extended areas.
    """

    tile_id: int
    sub_tile_row: int
    sub_tile_col: int
    sky_bounds: SkyBounds
    quadrants: list[QuadrantRef]
    source_catalog: str
    source_ids: list[int]
    core_source_ids: list[int]

    def to_yaml(self, path: str | Path) -> None:
        """Write manifest to a YAML file."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(
                self.model_dump(mode="json"),
                f,
                default_flow_style=False,
                sort_keys=False,
            )

    @classmethod
    def from_yaml(cls, path: str | Path) -> SubTileManifest:
        """Load manifest from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)


class SubTileResult(BaseModel):
    """Per-sub-tile shear inference result (one row in the shear catalog)."""

    TILE_ID: int
    SUBTILE_ROW: int
    SUBTILE_COL: int
    RA: float
    DEC: float
    G1: float
    G2: float
    G1_ERR: float
    G2_ERR: float
    N_SOURCES: int
    METHOD: str
    CONVERGENCE: float


SHEAR_CATALOG_SCHEMA = pa.schema(
    [
        pa.field("TILE_ID", pa.int64(), nullable=False),
        pa.field("SUBTILE_ROW", pa.int8(), nullable=False),
        pa.field("SUBTILE_COL", pa.int8(), nullable=False),
        pa.field("RA", pa.float64(), nullable=False),
        pa.field("DEC", pa.float64(), nullable=False),
        pa.field("G1", pa.float32(), nullable=False),
        pa.field("G2", pa.float32(), nullable=False),
        pa.field("G1_ERR", pa.float32(), nullable=False),
        pa.field("G2_ERR", pa.float32(), nullable=False),
        pa.field("N_SOURCES", pa.int32(), nullable=False),
        pa.field("METHOD", pa.string(), nullable=False),
        pa.field("CONVERGENCE", pa.float32(), nullable=False),
    ]
)
