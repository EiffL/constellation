# Constellation

A modern and scalable pipeline for survey-scale shear inference.

**constellation** is the orchestration layer that partners with [SHINE](https://github.com/CosmoStat/SHINE) (SHear INference Environment) to run weak lensing shear measurement across the Euclid Wide Survey. SHINE handles the probabilistic model, rendering, and inference for a single sky region; constellation handles everything else — tiling, data discovery, manifest generation, workflow orchestration, and catalog assembly.

## Architecture

```
constellation                          SHINE
─────────────                          ─────
Tiling · Quadrant discovery            Galaxy model · Rendering
Source partitioning · Manifests        Likelihood · Inference
Flyte workflows · SkyPilot compute     Data loading · PSF interpolation
Iceberg catalog                        Diagnostics
        │                                    ▲
        └── manifest.yaml ──────────────────┘
            (quadrant paths + source list + sky bounds)
```

The pipeline decomposes the sky into **MER tiles** (32'×32'), each subdivided into a **4×4 grid of sub-tiles** (8'×8' core with 1' overlap margin). Each sub-tile is one independent GPU job. For the full Euclid Wide Survey this produces ~960,000 sub-tiles.

### Pipeline stages

1. **Tile Preparation** (CPU) — Discover overlapping VIS quadrant images on S3, load the MER source catalog, partition sources into sub-tiles, and write per-sub-tile manifest YAML files.
2. **Sub-Tile Inference** (GPU) — Invoke SHINE on each manifest. Each job is fully independent and embarrassingly parallel.
3. **Catalog Assembly** (CPU) — Collect Parquet results, filter to core-area sources, and compact into an Apache Iceberg shear catalog.

## Quickstart

```bash
# Install with uv
uv sync

# Run the EDFF mock pipeline locally (no Flyte cluster required)
constellation run --config configs/edff_mock.yaml --local

# Validate the output catalog
constellation validate --config configs/edff_mock.yaml
```

This processes 48 EDFF tiles × 16 sub-tiles = 768 rows using a mock SHINE component that returns random shear values drawn from physically plausible distributions. The real S3 file discovery runs against the public `nasa-irsa-euclid-q1` bucket; only the inference step is mocked.

## Installation

Requires Python >= 3.12.

```bash
git clone <repo-url>
cd constellation
uv sync                     # install all dependencies
uv sync --extra test        # include test dependencies
```

## CLI

```
constellation [--verbose] <command>
```

| Command | Description |
|---------|-------------|
| `run --config CONFIG --local` | Run the full pipeline locally (sequential) |
| `prepare --config CONFIG` | Generate sub-tile manifests for all tiles |
| `infer --config CONFIG MANIFESTS...` | Run inference on specific manifests |
| `assemble --config CONFIG RESULTS...` | Merge Parquet results into Iceberg |
| `validate --config CONFIG [--expected N]` | Print catalog quality statistics |

## Configuration

Pipeline configuration is a single YAML file. See [`configs/edff_mock.yaml`](configs/edff_mock.yaml) for a complete example.

```yaml
field_name: EDFF
tile_ids: [102018211, 102018212, ...]    # MER tile IDs to process

tiling:
  sub_tile_grid: [4, 4]                  # sub-tiles per tile
  sub_tile_margin_arcmin: 1.0            # overlap margin

data:
  vis_base_uri: "s3://nasa-irsa-euclid-q1/q1/VIS/"
  catalog_base_uri: "s3://nasa-irsa-euclid-q1/q1/catalogs/MER_FINAL_CATALOG/"

output:
  catalog_warehouse: "/tmp/constellation-warehouse"
  result_dir: "/tmp/constellation-results/"
  manifest_dir: "/tmp/constellation-manifests/"

mock_shine: true        # use mock inference (no GPU required)

inference: { ... }      # pass-through to SHINE (opaque)
gal: { ... }            # pass-through to SHINE (opaque)
```

## Project structure

```
src/constellation/
├── config.py              # PipelineConfig (Pydantic)
├── schemas.py             # SubTileManifest, SubTileResult, PyArrow schema
├── tiling.py              # MER tile footprints, sub-tile grid generation
├── discovery.py           # Real S3 quadrant discovery via boto3
├── manifest.py            # Manifest writer
├── mock_shine.py          # Mock inference (random shear)
├── result_writer.py       # Parquet writer
├── iceberg_catalog.py     # PyIceberg table management
├── catalog_assembler.py   # Merge results → Iceberg
├── cli.py                 # Click CLI
└── workflows/
    ├── tasks.py           # Flyte @task definitions
    ├── pipeline.py        # Flyte @workflow
    └── local_runner.py    # Sequential local executor
```

## Testing

```bash
uv run pytest               # 71 tests, all offline (moto mocks S3)
uv run pytest -m integration # end-to-end only
uv run pytest -v             # verbose output
```

Tests use [moto](https://github.com/getmoto/moto) to mock S3 listing calls so the full suite runs without network access.

## Technology stack

| Layer | Tool |
|-------|------|
| Workflow orchestration | Flyte |
| GPU compute | SkyPilot (prod) / Modal (dev) |
| Data lakehouse | Apache Iceberg on S3 (PyIceberg + SQLite catalog locally) |
| Query engines | DuckDB / Polars / Athena |
| Input data | Euclid Q1 on `s3://nasa-irsa-euclid-q1` (public) |

## License

MIT
