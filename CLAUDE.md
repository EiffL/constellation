# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**constellation** is the orchestration layer for survey-scale weak lensing shear inference. It partners with [SHINE](https://github.com/CosmoStat/SHINE) (SHear INference Environment), a JAX/NumPyro-powered inference library. SHINE handles the probabilistic model, rendering, and inference for a single sky region; constellation handles tiling, data preparation, manifest generation, workflow orchestration, and catalog assembly across the entire Euclid Wide Survey (~960,000 independent GPU jobs).

The architecture is fully specified in `survey_scale_architecture.md`.

**Organization:** CosmoStat Lab (CEA / CNRS)
**Status:** Early development — architecture defined, implementation not yet started.

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

## Planned Module Structure

```
constellation/
├── pipeline/
│   ├── tiling.py              # MER tile footprints, sub-tile grid generation
│   ├── discovery.py           # Query archive/S3 for overlapping VIS quadrants
│   ├── manifest.py            # Write per-sub-tile manifest YAML
│   ├── workflows.py           # Flyte workflow definitions (map tasks)
│   ├── catalog_assembler.py   # Merge sub-tile results → Iceberg table
│   └── config.py              # Pipeline-level Pydantic configuration
├── infra/
│   ├── terraform/             # VPC, EKS, GPU nodes, S3, Flyte, monitoring
│   └── docker/                # Dockerfile (JAX + CUDA + SHINE)
└── configs/
    ├── q1_campaign.yaml       # Q1 pilot (~4,200 sub-tiles, ~65 deg²)
    └── wide_survey.yaml       # Full survey (~960,000 sub-tiles)
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

## Development Setup

Python virtual environment is at `.venv/` (Python 3.14). SHINE is symlinked at `extern/SHINE`.

## Code Standards (from SHINE)

- **Formatter:** Black (line-length 88)
- **Import sorting:** isort (black profile)
- **Type hints:** Full PEP 484 compliance
- **Docstrings:** Google-style
- **Testing:** pytest
- **Config:** Pydantic models for all configuration
