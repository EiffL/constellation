# Constellation

Orchestration layer for survey-scale weak lensing shear inference.

**constellation** partners with [SHINE](https://github.com/CosmoStat/SHINE) (SHear INference Environment) to run weak lensing shear measurement across the Euclid Wide Survey. SHINE handles the probabilistic model, rendering, and inference for a single sky region; constellation handles everything else: sky tiling, VIS quadrant discovery and WCS resolution, FITS extraction, manifest generation, Flyte workflow orchestration, and catalog assembly.

**Organization:** CosmoStat Lab (CEA / CNRS)

## Architecture

```
constellation                          SHINE
─────────────                          ─────
Tiling · WCS quadrant resolution       Galaxy model · Rendering
FITS extraction · Catalog subsetting   Likelihood · Inference
Manifest generation · S3 upload        Data loading · PSF interpolation
Flyte workflows · Spot compute         Diagnostics
        │                                    ▲
        └── manifest_local.yaml ────────────┘
            (extracted FITS paths + source catalog + sky bounds)
```

The pipeline decomposes the Euclid sky into **MER tiles** (32'x32'), each subdivided into a configurable grid of **sub-tiles** (default 4x4 = 8'x8' core, 10'x10' extended with 1' overlap margin). Each sub-tile becomes one independent GPU job. For the full Euclid Wide Survey this produces ~960,000 sub-tiles across ~60,000 tiles.

## Pipeline stages

### Stage 1: Data Preparation (CPU, Flyte)

1. **Build observation index** — List VIS observation directories on S3, parse file naming conventions to locate DET/BKG/WGT/PSF files for each `(obs_id, dither, ccd)` combination.

2. **Build quadrant spatial index** — Fan out via `map_task` (one pod per DET file, ~840 tasks). Each task uses HTTP range requests to read only FITS WCS headers (~64 KB each, not the full ~7 GB file) and computes the sky footprint of every quadrant.

3. **Prepare and extract tile** — For each MER tile (one pod per tile, running in parallel):
   - Generate sub-tile manifests by intersecting the quadrant spatial index with each sub-tile's sky bounds.
   - **Stream-extract** FITS data: download one multi-GB source file at a time, extract all referenced quadrant HDUs into consolidated per-exposure FITS files, then delete the source file before downloading the next. This caps peak disk usage at ~1 source file (~7 GB) instead of the full observation set (~42 GB for deep-field tiles).
   - Subset the MER source catalog to each sub-tile's extended area.
   - Write a `manifest_local.yaml` with relative paths for SHINE consumption.
   - Upload the self-contained sub-tile directories to S3.

### Stage 2: Sub-Tile Inference (GPU, future)

Invoke SHINE on each sub-tile manifest. Each job is fully independent and embarrassingly parallel:

```bash
python -m shine.euclid.run --manifest manifest_local.yaml --config inference.yaml --output results/
```

Currently uses `mock_shine.py` which returns random shear values drawn from physically plausible distributions.

### Stage 3: Catalog Assembly (CPU, future)

Collect per-sub-tile Parquet results, filter to core-area sources (discarding overlap margins), and compact into an Apache Iceberg shear catalog.

## Output structure

Each tile's extracted output is uploaded to S3 under `{storage_base_uri}/{run_id}/{tile_id}/`:

```
{run_id}/{tile_id}/
├── 0_0/
│   ├── manifest.yaml             # Original manifest (S3 paths)
│   ├── manifest_local.yaml       # Rewritten manifest (relative paths)
│   ├── catalog.fits              # MER catalog subset for this sub-tile
│   ├── exposures/
│   │   ├── {obs}_{dither}_{ccd}_sci.fits   # Consolidated science quadrants
│   │   ├── {obs}_{dither}_{ccd}_bkg.fits   # Consolidated background quadrants
│   │   └── {obs}_{dither}_{ccd}_wgt.fits   # Consolidated weight quadrants
│   └── psf/
│       └── {obs}_{dither}_{ccd}_psf.fits   # Consolidated PSF quadrants
├── 0_1/
│   └── ...
├── ...
└── 3_3/
    └── ...
```

**Consolidated FITS files:** All quadrant HDUs from the same exposure `(obs_id, dither, ccd)` that overlap a given sub-tile are packed into a single FITS file per type (sci, bkg, wgt, psf). SHINE reads individual quadrants by HDU name (e.g., `"3-4.F.SCI"`). This reduces output file count by ~10x compared to one-file-per-quadrant.

## Quickstart

```bash
# Install with uv (Python >= 3.12)
uv sync

# Run tests (115 tests, all offline via moto S3 mocks)
uv run pytest

# Run the EDFF mock pipeline locally (no Flyte cluster required)
uv run constellation run --config configs/edff_mock.yaml --local

# Validate the output catalog
uv run constellation validate --config configs/edff_mock.yaml
```

## Flyte deployment

**Container image:** `696356228955.dkr.ecr.us-east-1.amazonaws.com/constellation:latest`

```bash
# Port-forward Flyte services
kubectl -n flyte port-forward svc/flyte-backend-flyte-binary-grpc 8089:8089 &
kubectl -n flyte port-forward svc/flyte-backend-flyte-binary-http 8088:8088 &

# Register + run (fast registration — no Docker build for code-only changes)
uv run pyflyte run --remote \
  --project constellation --domain development \
  --image 696356228955.dkr.ecr.us-east-1.amazonaws.com/constellation:latest \
  src/constellation/workflows/pipeline.py data_preparation_pipeline \
  --config_yaml configs/edff_single_tile.yaml \
  --tile_ids '[102019592]'
```

Fast registration zips your Python source and uploads it to S3; pods download and unpack it at runtime. Only rebuild the Docker image when dependencies change.

**Flyte console:** http://localhost:8088/console

## Configuration

Pipeline configuration is a single YAML file. See [`configs/edff_single_tile.yaml`](configs/edff_single_tile.yaml) for a working example.

```yaml
field_name: EDFF
tile_ids: [102019592]

tiling:
  sub_tile_grid: [4, 4]               # sub-tiles per tile (rows, cols)
  sub_tile_margin_arcmin: 1.0          # overlap margin

data:
  vis_base_uri: "s3://nasa-irsa-euclid-q1/q1/VIS/"
  catalog_base_uri: "s3://nasa-irsa-euclid-q1/q1/catalogs/MER_FINAL_CATALOG/"
  s3_no_sign_request: true

output:
  storage_base_uri: "s3://constellation-pipeline-dev-696356228955"
  extraction_dir: "/tmp/constellation-subtiles/"
  manifest_dir: "/tmp/constellation-manifests/"
  catalog_warehouse: "/tmp/constellation-warehouse"

mock_shine: true                       # use mock inference (no GPU required)

inference: { ... }                     # pass-through to SHINE (opaque)
gal: { ... }                          # pass-through to SHINE (opaque)
```

## Project structure

```
src/constellation/
├── config.py              # PipelineConfig (Pydantic v2)
├── schemas.py             # SubTileManifest, SubTileResult, PyArrow schema
├── tiling.py              # MER tile footprints, sub-tile grid generation
├── discovery.py           # S3 observation index (VIS file listing + parsing)
├── quadrant_resolver.py   # WCS-based quadrant footprints (HTTP range reads)
├── manifest.py            # Per-sub-tile manifest YAML writer
├── extractor.py           # Streaming FITS extraction + catalog subsetting
├── storage.py             # S3 upload (concurrent) + run ID resolution
├── mock_shine.py          # Mock inference (random shear)
├── result_writer.py       # Parquet result writer
├── iceberg_catalog.py     # PyIceberg table management (SQLite / Glue)
├── catalog_assembler.py   # Merge sub-tile results → Iceberg table
├── cli.py                 # Click CLI entry point
└── workflows/
    ├── tasks.py           # Flyte @task definitions
    ├── pipeline.py        # Flyte @workflow with map_task fan-out
    └── local_runner.py    # Sequential local executor (no Flyte)
```

## Infrastructure

```
infra/terraform/           # EKS cluster, S3 buckets, Flyte control plane
configs/                   # Pipeline YAML configs (baked into Docker image)
```

| Layer | Tool | Purpose |
|-------|------|---------|
| Workflow orchestration | **Flyte** | `map_task` for quadrant reads, `@dynamic` for per-tile fan-out |
| GPU compute (production) | **SkyPilot** | Cross-cloud spot GPU provisioning |
| GPU compute (dev) | **Modal** | Serverless GPUs for fast iteration |
| Data lakehouse | **Apache Iceberg** on S3 | Versioned shear catalog (Parquet) |
| Infrastructure | **Terraform** | EKS, S3, Flyte, monitoring |
| Input data | Euclid Q1 | `s3://nasa-irsa-euclid-q1` (public, 353 MER tiles) |

## Testing

```bash
uv run pytest                # 115 tests, all offline (moto mocks S3)
uv run pytest -v             # verbose output
uv run pytest tests/test_extractor.py  # just extractor tests
```

Tests use [moto](https://github.com/getmoto/moto) for S3 mocking and generate synthetic FITS files with valid WCS headers so the full suite runs without network access.

## License

MIT
