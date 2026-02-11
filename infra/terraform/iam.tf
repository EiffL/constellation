# -----------------------------------------------------------------------------
# IAM — IRSA roles so Flyte pods get fine-grained S3/RDS access without
# static credentials.
# -----------------------------------------------------------------------------

# ------------------------------------------------------------------
# 1. Flyte backend (flyteadmin + flytepropeller + datacatalog)
#    Needs read/write to the Flyte artifacts bucket.
# ------------------------------------------------------------------

resource "aws_iam_policy" "flyte_backend" {
  name = "${local.cluster_name}-flyte-backend"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "FlyteS3ReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject*",
          "s3:PutObject*",
          "s3:DeleteObject*",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.flyte.arn,
          "${aws_s3_bucket.flyte.arn}/*",
        ]
      },
      {
        Sid    = "PipelineBucketReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject*",
          "s3:PutObject*",
          "s3:DeleteObject*",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.pipeline.arn,
          "${aws_s3_bucket.pipeline.arn}/*",
        ]
      },
    ]
  })
}

module "flyte_backend_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.48"

  role_name = "${local.cluster_name}-flyte-backend"

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["flyte:flyte-backend-flyte-binary"]
    }
  }

  role_policy_arns = {
    s3 = aws_iam_policy.flyte_backend.arn
  }
}

# ------------------------------------------------------------------
# 2. Flyte task pods (SHINE inference + pipeline tasks)
#    Needs:
#    - Read-only access to the Euclid public bucket (belt-and-suspenders
#      with --no-sign-request; the role makes signed requests work too).
#    - Read/write to the pipeline working bucket (manifests, results).
#    - Read/write to the Flyte artifacts bucket (task I/O).
# ------------------------------------------------------------------

resource "aws_iam_policy" "flyte_tasks" {
  name = "${local.cluster_name}-flyte-tasks"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EuclidDataReadOnly"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::nasa-irsa-euclid-q1",
          "arn:aws:s3:::nasa-irsa-euclid-q1/*",
        ]
      },
      {
        Sid    = "PipelineBucketReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject*",
          "s3:PutObject*",
          "s3:DeleteObject*",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.pipeline.arn,
          "${aws_s3_bucket.pipeline.arn}/*",
        ]
      },
      {
        Sid    = "FlyteArtifactsReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject*",
          "s3:PutObject*",
          "s3:DeleteObject*",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.flyte.arn,
          "${aws_s3_bucket.flyte.arn}/*",
        ]
      },
    ]
  })
}

module "flyte_tasks_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.48"

  role_name = "${local.cluster_name}-flyte-tasks"

  # Flyte dynamically creates per-project-domain namespaces (e.g.
  # flytesnacks-development, constellation-production, ...).
  # Use StringLike so the wildcard in *:default actually matches.
  assume_role_condition_test = "StringLike"

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["*:default"]
    }
  }

  role_policy_arns = {
    s3 = aws_iam_policy.flyte_tasks.arn
  }
}

# ------------------------------------------------------------------
# 3. Cluster Autoscaler
# ------------------------------------------------------------------

resource "aws_iam_policy" "cluster_autoscaler" {
  name = "${local.cluster_name}-cluster-autoscaler"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Describe"
        Effect = "Allow"
        Action = [
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:DescribeAutoScalingInstances",
          "autoscaling:DescribeLaunchConfigurations",
          "autoscaling:DescribeScalingActivities",
          "autoscaling:DescribeTags",
          "ec2:DescribeImages",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeLaunchTemplateVersions",
          "ec2:GetInstanceTypesFromInstanceRequirements",
          "eks:DescribeNodegroup",
        ]
        Resource = ["*"]
      },
      {
        Sid    = "ScaleOwnNodeGroups"
        Effect = "Allow"
        Action = [
          "autoscaling:SetDesiredCapacity",
          "autoscaling:TerminateInstanceInAutoScalingGroup",
        ]
        Resource = ["*"]
        Condition = {
          StringEquals = {
            "autoscaling:ResourceTag/kubernetes.io/cluster/${local.cluster_name}" = "owned"
          }
        }
      },
    ]
  })
}

module "cluster_autoscaler_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.48"

  role_name = "${local.cluster_name}-cluster-autoscaler"

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:cluster-autoscaler"]
    }
  }

  role_policy_arns = {
    autoscaler = aws_iam_policy.cluster_autoscaler.arn
  }
}
