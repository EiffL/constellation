# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**constellation** is the orchestration layer for survey-scale weak lensing shear inference. It partners with [SHINE](https://github.com/CosmoStat/SHINE) (SHear INference Environment), a JAX/NumPyro-powered inference library. SHINE handles the probabilistic model, rendering, and inference for a single sky region; constellation handles tiling, data preparation, manifest generation, workflow orchestration, and catalog assembly across the entire Euclid Wide Survey (~960,000 independent GPU jobs).

The architecture is fully specified in `survey_scale_architecture.md`.

**Organization:** CosmoStat Lab (CEA / CNRS)
**Status:** Early development — core pipeline implemented with mock SHINE, targeting EDFF.

## Two-Project Architecture

```
constellation                          SHINE
─────────────                          ─────
Tiling · Quadrant discovery            Galaxy model · Rendering
Source partitioning · Manifests        Likelihood · Inference
Flyte workflows · SkyPilot compute    Data loading · PSF interpolation
Iceberg catalog · Terraform           Diagnostics
        │                                    ▲
        └── manifest.yaml ──────────────────┘
            (quadrant paths + source list + sky bounds)
```

constellation produces per-sub-tile manifests (YAML); SHINE consumes them and produces per-sub-tile results (Parquet). SHINE is linked locally via `extern/SHINE` (symlink to `../../SHINE`).

## Key Concepts

- **MER Tile**: Euclid sky partition (32'×32'), the data preparation boundary. ~60,000 tiles cover the full survey.
- **Sub-tile**: Subdivision of a MER tile (default 4×4 = 8'×8' core, 10'×10' extended with 1' overlap margin). One sub-tile = one GPU job.
- **Core / Extended area**: Sources in the extended area are modeled during inference; results are reported only for core-area sources. The overlap margin prevents boundary bias.
- **Manifest**: YAML file defining one sub-tile's inputs — quadrant FITS paths, source catalog path, source IDs, and sky bounds. This is the interface contract with SHINE.
- **Quadrant**: A VIS CCD quadrant image (2048×2066 px), loaded as-is with no reprojection.

## Module Structure

```
src/constellation/
├── __init__.py
├── config.py              # PipelineConfig (Pydantic v2)
├── schemas.py             # SubTileManifest, SubTileResult, QuadrantRef, PyArrow schema
├── tiling.py              # MER tile footprints, sub-tile grid generation
├── discovery.py           # S3 quadrant discovery via boto3
├── manifest.py            # Per-sub-tile manifest YAML writer
├── mock_shine.py          # Mock SHINE inference (random shear)
├── result_writer.py       # Parquet result writer
├── iceberg_catalog.py     # PyIceberg table management (SQLite catalog)
├── catalog_assembler.py   # Merge sub-tile results → Iceberg table
├── cli.py                 # Click CLI entry point
└── workflows/
    ├── tasks.py           # Flyte @task definitions
    ├── pipeline.py        # Flyte @workflow with map_task
    └── local_runner.py    # Sequential executor (no Flyte)
```

## Technology Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Workflow orchestration | **Flyte** | Map tasks for embarrassingly parallel sub-tile jobs |
| GPU compute (production) | **SkyPilot** | Cross-cloud spot GPU provisioning (~$0.25/hr L4) |
| GPU compute (dev) | **Modal** | Serverless GPUs, fast iteration (~$0.80/hr L4) |
| Data lakehouse | **Apache Iceberg** on S3 | Versioned, queryable shear catalog (Parquet) |
| Query engines | **DuckDB** / **Polars** / **Athena** | Analysis of output catalogs |
| Infrastructure | **Terraform** | EKS, S3, Flyte control plane, monitoring |
| Monitoring | **Grafana** + Prometheus + Loki | Infrastructure metrics; Flyte handles job state |
| Data versioning | **DVC** | Content-addressable hashes of input FITS on S3 |
| CI/CD | **GitHub Actions** | Docker builds tagged with git SHA, tiered testing |

## Pipeline Stages

1. **Tile Preparation** (CPU): For each MER tile, discover overlapping VIS quadrants, load MER catalog, partition sources into sub-tiles, write per-sub-tile manifests.
2. **Sub-Tile Inference** (GPU): Invoke SHINE per sub-tile via `python -m shine.euclid.run --manifest ...`. Each job is fully independent.
3. **Catalog Assembly** (CPU): Collect results, filter to core-area sources, compact into Iceberg shear catalog, run quality checks.

## SHINE Interface

SHINE is invoked per sub-tile either as a CLI or Python API:
```bash
python -m shine.euclid.run --manifest subtile_manifest.yaml --config inference.yaml --output results/
```
```python
from shine.euclid.cli import run_subtile
result = run_subtile(manifest_path, config_path, output_dir)
```

The `inference` and `gal` config sections are passed through to SHINE unchanged — constellation does not interpret them.

## Development Commands

```bash
# Install (uses uv)
uv sync --extra test --extra dev

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_tiling.py -v

# Run the mock EDFF pipeline locally
uv run constellation run --config configs/edff_mock.yaml --local

# Validate the output catalog
uv run constellation validate --config configs/edff_mock.yaml

# Format code
uv run black src/ tests/
uv run isort src/ tests/
```

## Development Setup

- **Package manager:** uv (pyproject.toml with hatchling backend)
- **Python:** >=3.12 (venv at `.venv/`)
- **SHINE:** symlinked at `extern/SHINE`
- **Flyte:** local sandbox via `flytectl demo start`

## Code Standards (from SHINE)

- **Formatter:** Black (line-length 88)
- **Import sorting:** isort (black profile)
- **Type hints:** Full PEP 484 compliance
- **Docstrings:** Google-style
- **Testing:** pytest (with moto for S3 mocking)
- **Config:** Pydantic v2 models for all configuration

## Euclid Q1 Data on S3

**Bucket:** `nasa-irsa-euclid-q1` (us-east-1, public, `--no-sign-request`)

**353 MER tiles** across 3 deep fields + LDN 1641. Tile IDs are 9-digit integers.

**VIS observations** (`s3://nasa-irsa-euclid-q1/q1/VIS/{OBS_ID}/`):
- File pattern: `EUC_VIS_SWL-{TYPE}-{OBS_ID:06d}-{DITHER:02d}-{CCD}-0000000__{TS}.fits`
  - TYPE: `DET` (science ~7.3 GB), `BKG` (background ~2.4 GB), `WGT` (weight ~2.4 GB)
  - DITHER: `00`–`03`, CCD: `1` or `2` (focal plane halves)
- Each DET file is multi-extension FITS with ~72 quadrants × 3 HDUs (SCI/RMS/FLG)
- Quadrant HDU names: `{row}-{col}.{letter}.SCI` (e.g., `3-4.F.SCI`)
- 1 PSF grid per observation: `EUC_VIS_GRD-PSF-000-000000-0000000__{TS}.fits`

**MER catalogs** (`s3://nasa-irsa-euclid-q1/q1/catalogs/MER_FINAL_CATALOG/{TILE_ID}/`):
- `EUC_MER_FINAL-CAT_TILE{ID}-{HASH}_{TS}_00.00.fits` — main catalog (1–200 MB)
- Key columns: `object_id`, `right_ascension`, `declination`, `flux_detection_total`, `semimajor_axis`, `vis_det`, `spurious_flag`, `det_quality_flag`

**SHINE's data interface** (from `shine/euclid/data_loader.py`):
- `exposure_paths`: list of multi-extension FITS paths, accessed by quadrant HDU name
- `quadrant`: HDU name prefix (e.g., `"3-4.F"`) → `{quadrant}.SCI/RMS/FLG`
- `psf_path`: PSF grid FITS, accessed via `hdul[quadrant]`
