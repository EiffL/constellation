# -----------------------------------------------------------------------------
# S3 buckets
# -----------------------------------------------------------------------------

# Flyte metadata & raw output artifacts (workflow inputs/outputs, logs).
resource "aws_s3_bucket" "flyte" {
  bucket = "${var.project}-flyte-${var.environment}-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "flyte" {
  bucket = aws_s3_bucket.flyte.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "flyte" {
  bucket                  = aws_s3_bucket.flyte.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Pipeline working storage — manifests, sub-tile results, assembled catalog.
resource "aws_s3_bucket" "pipeline" {
  bucket = "${var.project}-pipeline-${var.environment}-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "pipeline" {
  bucket = aws_s3_bucket.pipeline.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "pipeline" {
  bucket                  = aws_s3_bucket.pipeline.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
