"""Mock SHINE inference component.

Reads a manifest YAML and returns random shear values drawn from
physically plausible distributions. Used for pipeline testing without
a real SHINE installation or GPU.
"""

from __future__ import annotations

import numpy as np

from constellation.schemas import SubTileManifest, SubTileResult


def run_mock_inference(
    manifest_path: str,
    seed: int = 42,
) -> SubTileResult:
    """Run mock inference for one sub-tile.

    Reads the manifest, generates deterministic random shear values:
    - g1, g2 ~ Normal(0, 0.03)  (typical cosmic shear amplitude)
    - g1_err, g2_err ~ Uniform(0.005, 0.015)
    - convergence ~ Uniform(0.1, 0.5)

    The RNG seed is derived from the base seed plus tile/row/col indices,
    ensuring reproducibility.

    Args:
        manifest_path: Path to the sub-tile manifest YAML.
        seed: Base random seed.

    Returns:
        SubTileResult with mock shear values.
    """
    manifest = SubTileManifest.from_yaml(manifest_path)

    # Deterministic seed per sub-tile
    subtile_seed = (
        seed
        + manifest.tile_id * 100
        + manifest.sub_tile_row * 10
        + manifest.sub_tile_col
    )
    rng = np.random.default_rng(subtile_seed)

    center_ra = (
        manifest.sky_bounds.core_ra[0] + manifest.sky_bounds.core_ra[1]
    ) / 2
    center_dec = (
        manifest.sky_bounds.core_dec[0] + manifest.sky_bounds.core_dec[1]
    ) / 2

    return SubTileResult(
        TILE_ID=manifest.tile_id,
        SUBTILE_ROW=manifest.sub_tile_row,
        SUBTILE_COL=manifest.sub_tile_col,
        RA=center_ra,
        DEC=center_dec,
        G1=float(rng.normal(0, 0.03)),
        G2=float(rng.normal(0, 0.03)),
        G1_ERR=float(rng.uniform(0.005, 0.015)),
        G2_ERR=float(rng.uniform(0.005, 0.015)),
        N_SOURCES=len(manifest.core_source_ids),
        METHOD="mock",
        CONVERGENCE=float(rng.uniform(0.1, 0.5)),
    )
