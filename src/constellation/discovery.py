"""S3 quadrant discovery for Euclid VIS observations.

Lists VIS observation directories and files on S3 (metadata only, no
downloads), parses file names into structured records, and matches
observations to sub-tile sky footprints.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from constellation.schemas import QuadrantRef

# Pattern: EUC_VIS_SWL-{TYPE}-{OBS_ID}-{DITHER}-{CCD}-0000000__{TS}.fits
_VIS_SWL_RE = re.compile(
    r"EUC_VIS_SWL-(DET|BKG|WGT)-(\d{6})-(\d{2})-(\d)-\d+__(.+)\.fits"
)

# Pattern: EUC_VIS_GRD-PSF-000-000000-0000000__{TS}.fits
_VIS_PSF_RE = re.compile(r"EUC_VIS_GRD-PSF-\d+-\d+-\d+__(.+)\.fits")

# Pattern for MER catalog: EUC_MER_FINAL-CAT_TILE{ID}-{HASH}_{TS}_{VER}.fits
_MER_CAT_RE = re.compile(r"EUC_MER_FINAL-CAT_TILE(\d+)-")


@dataclass
class VISFileRecord:
    """Parsed metadata from a VIS filename."""

    file_type: str  # DET, BKG, WGT, PSF
    obs_id: str
    dither: str
    ccd: str
    timestamp: str
    s3_key: str

    @property
    def s3_uri(self) -> str:
        return f"s3://{self.bucket}/{self.s3_key}" if hasattr(self, "bucket") else ""


@dataclass
class ObservationIndex:
    """Index of all VIS files grouped by observation ID.

    Structure: obs_id -> list of VISFileRecord
    """

    records: dict[str, list[VISFileRecord]] = field(default_factory=dict)
    bucket: str = ""

    def obs_ids(self) -> list[str]:
        return sorted(self.records.keys())

    def get_psf_key(self, obs_id: str) -> str | None:
        """Find the PSF grid key for an observation."""
        for rec in self.records.get(obs_id, []):
            if rec.file_type == "PSF":
                return rec.s3_key
        return None

    def get_dither_ccd_pairs(self, obs_id: str) -> list[tuple[str, str]]:
        """Return unique (dither, ccd) pairs that have DET files."""
        pairs = set()
        for rec in self.records.get(obs_id, []):
            if rec.file_type == "DET":
                pairs.add((rec.dither, rec.ccd))
        return sorted(pairs)

    def get_file(
        self, obs_id: str, file_type: str, dither: str, ccd: str
    ) -> str | None:
        """Find the S3 key for a specific file."""
        for rec in self.records.get(obs_id, []):
            if (
                rec.file_type == file_type
                and rec.dither == dither
                and rec.ccd == ccd
            ):
                return rec.s3_key
        return None

    def to_dict(self) -> dict:
        """Serialize to a plain dict for Flyte inter-task transport."""
        return {
            "bucket": self.bucket,
            "records": {
                obs_id: [
                    {
                        "file_type": r.file_type,
                        "obs_id": r.obs_id,
                        "dither": r.dither,
                        "ccd": r.ccd,
                        "timestamp": r.timestamp,
                        "s3_key": r.s3_key,
                    }
                    for r in recs
                ]
                for obs_id, recs in self.records.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> ObservationIndex:
        """Deserialize from a plain dict."""
        index = cls(bucket=data["bucket"])
        for obs_id, rec_dicts in data["records"].items():
            index.records[obs_id] = [
                VISFileRecord(**rd) for rd in rec_dicts
            ]
        return index


def _get_s3_client(region: str = "us-east-1", no_sign_request: bool = True):
    """Create a boto3 S3 client, optionally unsigned for public buckets."""
    config = Config(signature_version=UNSIGNED) if no_sign_request else Config()
    return boto3.client("s3", region_name=region, config=config)


def _parse_vis_filename(key: str) -> VISFileRecord | None:
    """Parse a VIS S3 key into a VISFileRecord, or None if unrecognized."""
    filename = key.rsplit("/", 1)[-1]

    m = _VIS_SWL_RE.match(filename)
    if m:
        return VISFileRecord(
            file_type=m.group(1),
            obs_id=m.group(2),
            dither=m.group(3),
            ccd=m.group(4),
            timestamp=m.group(5),
            s3_key=key,
        )

    m = _VIS_PSF_RE.match(filename)
    if m:
        # Extract obs_id from the directory path: q1/VIS/{obs_id}/...
        parts = key.split("/")
        obs_id = parts[2] if len(parts) >= 3 else "000000"
        return VISFileRecord(
            file_type="PSF",
            obs_id=obs_id.zfill(6),
            dither="00",
            ccd="0",
            timestamp=m.group(1),
            s3_key=key,
        )

    return None


def build_observation_index(
    vis_base_uri: str = "s3://nasa-irsa-euclid-q1/q1/VIS/",
    s3_region: str = "us-east-1",
    s3_no_sign_request: bool = True,
) -> ObservationIndex:
    """List all VIS observation directories and build a file index.

    Scans the VIS prefix on S3, parses each filename, and groups
    records by observation ID.

    Args:
        vis_base_uri: S3 URI prefix for VIS data.
        s3_region: AWS region.
        s3_no_sign_request: Use unsigned requests for public buckets.

    Returns:
        ObservationIndex with all parsed file records.
    """
    # Parse bucket and prefix from URI
    parts = vis_base_uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    client = _get_s3_client(s3_region, s3_no_sign_request)
    index = ObservationIndex(bucket=bucket)

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            record = _parse_vis_filename(obj["Key"])
            if record:
                # Normalize obs_id to the directory-level ID (without zero-padding)
                dir_obs_id = obj["Key"].split("/")[2] if "/" in obj["Key"] else ""
                if dir_obs_id not in index.records:
                    index.records[dir_obs_id] = []
                index.records[dir_obs_id].append(record)

    return index


def list_mer_catalog(
    tile_id: int,
    catalog_base_uri: str = "s3://nasa-irsa-euclid-q1/q1/catalogs/MER_FINAL_CATALOG/",
    s3_region: str = "us-east-1",
    s3_no_sign_request: bool = True,
) -> str | None:
    """Find the MER final catalog S3 path for a tile.

    Returns the full S3 URI, or None if not found.
    """
    parts = catalog_base_uri.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    prefix = f"{parts[1]}{tile_id}/" if len(parts) > 1 else f"{tile_id}/"

    client = _get_s3_client(s3_region, s3_no_sign_request)
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if _MER_CAT_RE.search(key.rsplit("/", 1)[-1]):
            return f"s3://{bucket}/{key}"
    return None


def build_quadrant_refs(
    obs_index: ObservationIndex,
    obs_id: str,
) -> list[QuadrantRef]:
    """Build QuadrantRef entries for all dither/CCD combos in an observation.

    Each (dither, ccd) pair that has a DET file produces one QuadrantRef.
    The quadrant HDU name is not known from S3 metadata alone — it defaults
    to a placeholder. Real usage requires reading FITS headers.
    """
    bucket = obs_index.bucket
    pairs = obs_index.get_dither_ccd_pairs(obs_id)
    psf_key = obs_index.get_psf_key(obs_id)

    refs = []
    for dither, ccd in pairs:
        det_key = obs_index.get_file(obs_id, "DET", dither, ccd)
        bkg_key = obs_index.get_file(obs_id, "BKG", dither, ccd)
        wgt_key = obs_index.get_file(obs_id, "WGT", dither, ccd)

        if not det_key:
            continue

        refs.append(
            QuadrantRef(
                sci_path=f"s3://{bucket}/{det_key}",
                bkg_path=f"s3://{bucket}/{bkg_key}" if bkg_key else "",
                wgt_path=f"s3://{bucket}/{wgt_key}" if wgt_key else "",
                psf_path=f"s3://{bucket}/{psf_key}" if psf_key else "",
                quadrant="TBD",  # Requires FITS header inspection
            )
        )

    return refs
