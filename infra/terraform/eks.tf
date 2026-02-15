# -----------------------------------------------------------------------------
# EKS cluster + managed node groups (system + GPU spot)
# -----------------------------------------------------------------------------

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.31"

  cluster_name    = local.cluster_name
  cluster_version = var.eks_cluster_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.public_subnets

  # Restrict API access to specific CIDRs (var.allowed_api_cidrs).
  cluster_endpoint_public_access       = true
  cluster_endpoint_public_access_cidrs = var.allowed_api_cidrs

  # Let the module manage the aws-auth ConfigMap so node groups can join.
  enable_cluster_creator_admin_permissions = true

  # Encrypt Kubernetes secrets at rest in etcd.
  cluster_encryption_config = {
    resources        = ["secrets"]
    provider_key_arn = aws_kms_key.eks.arn
  }

  # Control-plane audit logs — streams to CloudWatch.
  cluster_enabled_log_types = ["api", "audit", "authenticator"]

  # EKS add-ons — managed by AWS, no Helm required.
  cluster_addons = {
    coredns                = {}
    kube-proxy             = {}
    vpc-cni                = {}
    eks-pod-identity-agent = {}
  }

  eks_managed_node_groups = {
    # ------------------------------------------------------------------
    # System nodes — runs Flyte control plane, cluster autoscaler, etc.
    # ------------------------------------------------------------------
    system = {
      instance_types = var.system_instance_types
      min_size       = 1
      max_size       = 2
      desired_size   = 1

      labels = { role = "system" }

      # Enforce IMDSv2 — blocks SSRF-based credential theft from containers.
      metadata_options = {
        http_endpoint               = "enabled"
        http_tokens                 = "required"
        http_put_response_hop_limit = 1
      }
    }

    # ------------------------------------------------------------------
    # CPU worker nodes — cheap spot instances for data-prep tasks:
    # build_obs_index, build_quadrant_index, prepare_tile,
    # extract_tile, assemble_results, validate_results.
    #
    # t3a.medium spot ≈ $0.008/hr (~$6/mo if running 24/7).
    # Multiple instance types widen the spot pool for availability.
    # Scales 0→N; no cost when idle.
    # ------------------------------------------------------------------
    cpu-spot = {
      instance_types = var.cpu_worker_instance_types
      capacity_type  = "SPOT"

      min_size     = 0
      max_size     = var.cpu_worker_max_nodes
      desired_size = 0

      # 50 GB root volume — tile extraction caches multi-GB FITS files
      # locally before extracting sub-tile cutouts (see issue #1).
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = 50
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }

      labels = { role = "cpu-worker" }

      taints = {
        cpu_worker = {
          key    = "constellation.cosmostat.org/cpu-worker"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      metadata_options = {
        http_endpoint               = "enabled"
        http_tokens                 = "required"
        http_put_response_hop_limit = 1
      }
    }

    # ------------------------------------------------------------------
    # GPU nodes — spot L4 instances for SHINE inference tasks.
    # Starts at 0; the cluster autoscaler scales up when Flyte map-task
    # pods arrive with a gpu resource request.
    # ------------------------------------------------------------------
    gpu-spot = {
      instance_types = var.gpu_instance_types
      capacity_type  = "SPOT"
      ami_type       = "AL2023_x86_64_NVIDIA"

      min_size     = 0
      max_size     = var.gpu_max_nodes
      desired_size = 0

      labels = {
        role                     = "gpu-worker"
        "nvidia.com/gpu.present" = "true"
      }

      taints = {
        gpu = {
          key    = "nvidia.com/gpu"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      # Enforce IMDSv2 — blocks SSRF-based credential theft from containers.
      metadata_options = {
        http_endpoint               = "enabled"
        http_tokens                 = "required"
        http_put_response_hop_limit = 1
      }
    }
  }
}

# KMS key for EKS envelope encryption of Kubernetes secrets.
resource "aws_kms_key" "eks" {
  description             = "${local.cluster_name} EKS secrets encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}
