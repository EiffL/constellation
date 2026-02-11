variable "project" {
  description = "Project name, used as prefix for all resources"
  type        = string
  default     = "constellation"
}

variable "environment" {
  description = "Environment name (dev / prod)"
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region — must be us-east-1 to co-locate with Euclid Q1 S3 bucket"
  type        = string
  default     = "us-east-1"
}

# ------------------------------------------------------------------------------
# EKS
# ------------------------------------------------------------------------------

variable "eks_cluster_version" {
  description = "Kubernetes version for the EKS cluster"
  type        = string
  default     = "1.31"
}

variable "system_instance_types" {
  description = "Instance types for the system (Flyte control-plane) node group"
  type        = list(string)
  default     = ["t3.medium"]
}

variable "gpu_instance_types" {
  description = "Instance types for the GPU (inference) node group, in preference order"
  type        = list(string)
  default     = ["g6.xlarge", "g5.xlarge"]
}

variable "gpu_max_nodes" {
  description = "Maximum number of GPU spot nodes the autoscaler can provision"
  type        = number
  default     = 10
}

variable "cpu_worker_instance_types" {
  description = "Instance types for the CPU data-prep node group (spot fleet)"
  type        = list(string)
  default     = ["t3a.medium", "t3.medium", "t3a.large", "t3.large"]
}

variable "cpu_worker_max_nodes" {
  description = "Maximum number of CPU spot nodes for data-prep tasks"
  type        = number
  default     = 10
}

# ------------------------------------------------------------------------------
# Network access
# ------------------------------------------------------------------------------

variable "allowed_api_cidrs" {
  description = "CIDRs allowed to reach the EKS API endpoint (e.g. your IP or VPN)"
  type        = list(string)
  default     = ["0.0.0.0/0"] # CHANGE THIS — restrict to your IP / VPN CIDR
}

# ------------------------------------------------------------------------------
# RDS
# ------------------------------------------------------------------------------

variable "db_instance_class" {
  description = "RDS instance class for the Flyte PostgreSQL database"
  type        = string
  default     = "db.t4g.micro"
}
