# -----------------------------------------------------------------------------
# Outputs — values needed to configure kubectl, Helm, and the Flyte starter.
# -----------------------------------------------------------------------------

output "region" {
  value = var.region
}

# myApplications -------------------------------------------------------------

output "aws_application_tag" {
  description = "The awsApplication tag value — set this as the aws_application_tag variable to tag all resources"
  value       = aws_servicecatalogappregistry_application.this.application_tag
}

# EKS -----------------------------------------------------------------------

output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "configure_kubectl" {
  description = "Run this to add the cluster to your kubeconfig"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

# RDS -----------------------------------------------------------------------

output "flyte_db_endpoint" {
  description = "PostgreSQL endpoint (host:port) for Flyte Helm values"
  value       = aws_db_instance.flyte.endpoint
}

output "flyte_db_host" {
  description = "PostgreSQL hostname only"
  value       = aws_db_instance.flyte.address
}

# S3 -------------------------------------------------------------------------

output "flyte_bucket" {
  value = aws_s3_bucket.flyte.id
}

output "pipeline_bucket" {
  value = aws_s3_bucket.pipeline.id
}

# IAM (IRSA) -----------------------------------------------------------------

output "flyte_backend_role_arn" {
  description = "Annotate the Flyte backend ServiceAccount with this ARN"
  value       = module.flyte_backend_irsa.iam_role_arn
}

output "flyte_tasks_role_arn" {
  description = "Annotate per-namespace default ServiceAccounts with this ARN"
  value       = module.flyte_tasks_irsa.iam_role_arn
}

# SSM ─────────────────────────────────────────────────────────────────────────

output "flyte_db_password_ssm_name" {
  description = "SSM parameter name for the Flyte DB password"
  value       = aws_ssm_parameter.flyte_db_password.name
}
