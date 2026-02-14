"""Pipeline-level Pydantic configuration models."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class TilingConfig(BaseModel):
    """Sub-tile grid configuration within each MER tile."""

    sub_tile_grid: tuple[int, int] = (4, 4)
    sub_tile_margin_arcmin: float = 1.0
    mer_tile_size_arcmin: float = 32.0
    mer_core_size_arcmin: float = 30.0

    @field_validator("sub_tile_margin_arcmin")
    @classmethod
    def margin_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"margin must be positive, got {v}")
        return v

    @field_validator("sub_tile_grid")
    @classmethod
    def grid_positive(cls, v: tuple[int, int]) -> tuple[int, int]:
        if v[0] <= 0 or v[1] <= 0:
            raise ValueError(f"grid dimensions must be positive, got {v}")
        return v


class DataSourceConfig(BaseModel):
    """Paths to input data on S3."""

    vis_base_uri: str = "s3://nasa-irsa-euclid-q1/q1/VIS/"
    mer_base_uri: str = "s3://nasa-irsa-euclid-q1/q1/MER/"
    catalog_base_uri: str = (
        "s3://nasa-irsa-euclid-q1/q1/catalogs/MER_FINAL_CATALOG/"
    )
    s3_region: str = "us-east-1"
    s3_no_sign_request: bool = True


class OutputConfig(BaseModel):
    """Output catalog and result paths."""

    catalog_warehouse: str
    catalog_namespace: str = "shear"
    catalog_table: str = "edff_shear"
    result_dir: str = "results/"
    manifest_dir: str = "manifests/"
    extraction_dir: str = "subtiles/"
    storage_base_uri: str = ""


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration."""

    field_name: str
    tile_ids: list[int]
    tiling: TilingConfig = TilingConfig()
    data: DataSourceConfig = DataSourceConfig()
    output: OutputConfig
    mock_shine: bool = False
    max_parallelism: int = Field(
        default=100,
        description="Flyte workflow-level max parallelism.",
    )
    quadrant_concurrency: int = Field(
        default=50,
        description="map_task concurrency for DET header reads.",
    )
    inference: dict[str, Any] = Field(default_factory=dict)
    gal: dict[str, Any] = Field(default_factory=dict)

    @field_validator("tile_ids")
    @classmethod
    def tile_ids_nonempty(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("tile_ids must not be empty")
        return v

    @classmethod
    def from_yaml(cls, path: str | Path) -> PipelineConfig:
        """Load configuration from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)

    def to_yaml(self, path: str | Path) -> None:
        """Write configuration to a YAML file."""
        with open(path, "w") as f:
            yaml.dump(
                self.model_dump(mode="json"),
                f,
                default_flow_style=False,
                sort_keys=False,
            )

    @classmethod
    def from_yaml_content(cls, content: str) -> PipelineConfig:
        """Load configuration from a YAML string."""
        data = yaml.safe_load(content)
        return cls.model_validate(data)

    def to_yaml_content(self) -> str:
        """Serialize configuration to a YAML string."""
        return yaml.dump(
            self.model_dump(mode="json"),
            default_flow_style=False,
            sort_keys=False,
        )
