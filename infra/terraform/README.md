# Constellation — AWS Infrastructure

Minimal EKS + RDS + S3 setup for running Flyte in `us-east-1` (co-located
with the Euclid Q1 data in `s3://nasa-irsa-euclid-q1`).

## What this creates

| Resource | Purpose | Approx. cost (dev) |
|----------|---------|---------------------|
| VPC (2 AZs, NAT gateway) | Networking | ~$35/mo |
| EKS cluster | Kubernetes control plane | ~$75/mo |
| System node group (2× t3.large) | Flyte control plane | ~$120/mo |
| GPU node group (0–N× g6.xlarge spot) | SHINE inference | ~$0.25/hr per node |
| RDS PostgreSQL (db.t4g.micro) | Flyte metadata | ~$15/mo |
| S3 buckets (×2) | Flyte artifacts + pipeline data | ~$1/mo |
| NVIDIA device plugin | GPU scheduling | — |
| Cluster Autoscaler | Scale GPU nodes 0→N | — |

**Baseline monthly cost (idle, no GPU jobs): ~$245/mo**

## Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform >= 1.5
- `kubectl`

## Deploy

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — at minimum set db_password

terraform init
terraform plan
terraform apply
```

## Configure kubectl

```bash
# Printed by terraform output:
aws eks update-kubeconfig --region us-east-1 --name constellation-dev
```

## Install Flyte

After `terraform apply`, install Flyte using the cloud-simple deployment:

```bash
# 1. Create namespace
kubectl create namespace flyte

# 2. Download and customise the starter values
curl -sL https://raw.githubusercontent.com/flyteorg/flyte/master/charts/flyte-binary/eks-starter.yaml \
  > eks-values.yaml

# 3. Patch eks-values.yaml with your Terraform outputs:
#
#   database.host     = terraform output -raw flyte_db_host
#   database.password = <your db_password>
#   storage.s3.bucket = terraform output -raw flyte_bucket
#   storage.s3.region = us-east-1
#
#   serviceAccount annotations:
#     eks.amazonaws.com/role-arn = terraform output -raw flyte_backend_role_arn
#
#   cluster_resources → per-namespace SA annotation:
#     eks.amazonaws.com/role-arn = terraform output -raw flyte_tasks_role_arn

# 4. Install
helm repo add flyteorg https://flyteorg.github.io/flyte
helm install flyte-backend flyteorg/flyte-binary \
  --namespace flyte \
  --values eks-values.yaml

# 5. Verify
kubectl -n flyte get pods
kubectl -n flyte port-forward svc/flyte-backend-flyte-binary-http 8088:8088
# Open http://localhost:8088/console
```

## Tear down

```bash
terraform destroy
```
