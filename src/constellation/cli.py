"""CLI entry point for constellation."""

from __future__ import annotations

import logging
import sys

import click

from constellation.config import PipelineConfig


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def cli(verbose: bool) -> None:
    """constellation — survey-scale weak lensing shear inference."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )


@cli.command()
@click.option(
    "--config", "config_yaml", required=True, help="Path to pipeline config YAML."
)
def run(config_yaml: str) -> None:
    """Run the shear inference pipeline via Flyte.

    Submit the workflow using pyflyte or the Flyte console:

        pyflyte run src/constellation/workflows/pipeline.py shear_pipeline \\
            --config_yaml <config> --tile_ids '[...]'
    """
    click.echo(
        "Use pyflyte to submit the workflow:\n\n"
        "  pyflyte run src/constellation/workflows/pipeline.py shear_pipeline \\\n"
        f"    --config_yaml {config_yaml} --tile_ids '<tile_ids>'\n",
        err=True,
    )
    raise SystemExit(1)


@cli.command()
@click.option(
    "--config", "config_yaml", required=True, help="Path to pipeline config YAML."
)
def prepare(config_yaml: str) -> None:
    """Generate sub-tile manifests for all tiles."""
    from constellation.discovery import build_observation_index
    from constellation.manifest import write_manifests_for_tile

    config = PipelineConfig.from_yaml(config_yaml)
    obs_index = build_observation_index(
        vis_base_uri=config.data.vis_base_uri,
        s3_region=config.data.s3_region,
        s3_no_sign_request=config.data.s3_no_sign_request,
    )

    total = 0
    for tile_id in config.tile_ids:
        paths = write_manifests_for_tile(tile_id, config, obs_index)
        total += len(paths)

    click.echo(f"Generated {total} manifests for {len(config.tile_ids)} tiles.")


@cli.command()
@click.option(
    "--config", "config_yaml", required=True, help="Path to pipeline config YAML."
)
def extract(config_yaml: str) -> None:
    """Extract sub-tile data into self-contained directories."""
    from constellation.discovery import build_observation_index
    from constellation.extractor import extract_subtile
    from constellation.manifest import write_manifests_for_tile
    from constellation.quadrant_resolver import build_quadrant_index

    config = PipelineConfig.from_yaml(config_yaml)
    obs_index = build_observation_index(
        vis_base_uri=config.data.vis_base_uri,
        s3_region=config.data.s3_region,
        s3_no_sign_request=config.data.s3_no_sign_request,
    )

    click.echo("Building quadrant index from FITS headers...")
    quadrant_index = build_quadrant_index(
        obs_index,
        s3_anon=config.data.s3_no_sign_request,
    )
    click.echo(f"Found {len(quadrant_index)} quadrant footprints.")

    total_manifests = 0
    all_manifest_paths: list[str] = []
    for tile_id in config.tile_ids:
        paths = write_manifests_for_tile(
            tile_id, config, obs_index, quadrant_index=quadrant_index
        )
        all_manifest_paths.extend(paths)
        total_manifests += len(paths)

    click.echo(f"Generated {total_manifests} manifests. Extracting...")

    for manifest_path in all_manifest_paths:
        subtile_dir = extract_subtile(
            manifest_path,
            extraction_dir=config.output.extraction_dir,
            s3_anon=config.data.s3_no_sign_request,
        )
        click.echo(f"  {subtile_dir}")

    click.echo(
        f"Extracted {len(all_manifest_paths)} sub-tiles "
        f"for {len(config.tile_ids)} tiles."
    )


@cli.command()
@click.option(
    "--config", "config_yaml", required=True, help="Path to pipeline config YAML."
)
@click.argument("manifest_paths", nargs=-1, required=True)
def infer(config_yaml: str, manifest_paths: tuple[str, ...]) -> None:
    """Run inference on one or more sub-tile manifests."""
    config = PipelineConfig.from_yaml(config_yaml)

    if not config.mock_shine:
        click.echo("Real SHINE inference not yet integrated.", err=True)
        raise SystemExit(1)

    from constellation.mock_shine import run_mock_inference
    from constellation.result_writer import write_subtile_result

    for path in manifest_paths:
        result = run_mock_inference(path)
        out = write_subtile_result(result, config.output.result_dir)
        click.echo(f"  {out}")

    click.echo(f"Inferred {len(manifest_paths)} sub-tiles.")


@cli.command()
@click.option(
    "--config", "config_yaml", required=True, help="Path to pipeline config YAML."
)
@click.argument("result_paths", nargs=-1, required=True)
def assemble(config_yaml: str, result_paths: tuple[str, ...]) -> None:
    """Assemble sub-tile results into the Iceberg catalog."""
    from constellation.catalog_assembler import assemble_catalog

    config = PipelineConfig.from_yaml(config_yaml)
    n = assemble_catalog(
        list(result_paths),
        warehouse=config.output.catalog_warehouse,
        namespace=config.output.catalog_namespace,
        table_name=config.output.catalog_table,
    )
    click.echo(f"Assembled {n} rows into Iceberg catalog.")


@cli.command()
@click.option(
    "--config", "config_yaml", required=True, help="Path to pipeline config YAML."
)
@click.option("--expected", default=0, help="Expected number of sub-tiles.")
def validate(config_yaml: str, expected: int) -> None:
    """Validate the assembled shear catalog."""
    from constellation.catalog_assembler import validate_catalog

    config = PipelineConfig.from_yaml(config_yaml)

    if expected == 0:
        rows, cols = config.tiling.sub_tile_grid
        expected = len(config.tile_ids) * rows * cols

    stats = validate_catalog(
        warehouse=config.output.catalog_warehouse,
        namespace=config.output.catalog_namespace,
        table_name=config.output.catalog_table,
        expected_subtiles=expected,
    )

    click.echo(f"Rows:         {stats['row_count']}")
    click.echo(f"Tiles:        {stats['tile_count']}")
    click.echo(f"Completeness: {stats['completeness']:.2%}")
    click.echo(f"g1 mean:      {stats['g1_mean']:.6f}")
    click.echo(f"g2 mean:      {stats['g2_mean']:.6f}")
    click.echo(f"g1 std:       {stats['g1_std']:.6f}")
    click.echo(f"g2 std:       {stats['g2_std']:.6f}")
