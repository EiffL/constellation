provider "aws" {
  region = var.region

  default_tags {
    tags = merge(
      {
        Project     = var.project
        Environment = var.environment
        ManagedBy   = "terraform"
      },
      var.aws_application_tag != "" ? { awsApplication = var.aws_application_tag } : {},
    )
  }
}

data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    }
  }
}

locals {
  cluster_name = "${var.project}-${var.environment}"
  account_id   = data.aws_caller_identity.current.account_id
  azs          = slice(data.aws_availability_zones.available.names, 0, 2)
}

# -----------------------------------------------------------------------------
# AWS myApplications — groups all project resources under a single application
# in the AWS console for cost tracking and operational visibility.
# -----------------------------------------------------------------------------

resource "aws_servicecatalogappregistry_application" "this" {
  name        = var.project
  description = "Orchestration layer for survey-scale weak lensing shear inference"
}

import {
  to = aws_servicecatalogappregistry_application.this
  id = var.project
}

