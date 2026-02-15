# Constellation — AWS Infrastructure

Minimal EKS + RDS + S3 setup for running Flyte in `us-east-1` (co-located
with the Euclid Q1 data in `s3://nasa-irsa-euclid-q1`).

## What this creates

| Resource | Purpose | Approx. cost (dev) |
|----------|---------|---------------------|
| VPC (2 AZs, public subnets) | Networking | ~$0 |
| S3 VPC gateway endpoint | Free S3 traffic | $0 |
| EKS cluster | Kubernetes control plane | ~$73/mo |
| System node group (1× t3.small) | Flyte control plane | ~$15/mo |
| CPU worker node group (0–N× t3a.medium spot, 50 GB gp3) | Data prep tasks | ~$0.014/hr per node |
| GPU node group (0–N× g6.xlarge spot, AL2023 NVIDIA) | SHINE inference | ~$0.25/hr per node |
| RDS PostgreSQL (db.t4g.micro) | Flyte metadata | $0 (free tier) |
| S3 buckets (×2) | Flyte artifacts + pipeline data | ~$0 |
| KMS key | EKS secrets encryption | ~$1/mo |
| AppRegistry application | AWS myApplications grouping | $0 |
| NVIDIA device plugin | GPU scheduling | — |
| Cluster Autoscaler | Scale worker nodes 0→N | — |

**Baseline monthly cost (idle, no GPU jobs): ~$90/mo**

## Prerequisites

- AWS CLI configured (`aws configure`) with credentials that can create
  EKS clusters, RDS instances, IAM roles, and S3 buckets
- Terraform >= 1.5
- `kubectl`
- `helm`

## Step 1 — Deploy infrastructure

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:
- Set `allowed_api_cidrs` to your IP (run `curl -s ifconfig.me` to find it)
- Adjust `gpu_max_nodes` / `cpu_worker_max_nodes` if needed

```bash
terraform init
terraform plan        # review what will be created
terraform apply       # ~15 min for EKS
```

## Step 1b — Enable myApplications tagging

The first apply creates an AWS AppRegistry application and outputs its tag
ARN. To tag all resources under this application in the AWS console:

```bash
# Grab the application tag ARN from the apply output
terraform output aws_application_tag
```

Copy the `awsApplication` ARN value and add it to `terraform.tfvars`:

```hcl
aws_application_tag = "arn:aws:resource-groups:us-east-1:<ACCOUNT_ID>:group/constellation/<APP_ID>"
```

Then apply again to propagate the tag to all resources:

```bash
terraform apply
```

## Step 2 — Configure kubectl

```bash
$(terraform output -raw configure_kubectl)
kubectl get nodes     # should show 1 system node
```

## Step 3 — Retrieve DB password

The password was auto-generated and stored in SSM Parameter Store:

```bash
aws ssm get-parameter \
  --name "$(terraform output -raw flyte_db_password_ssm_name)" \
  --with-decryption \
  --query Parameter.Value \
  --output text
```

## Step 4 — Install Flyte

```bash
# Add Helm repo
helm repo add flyteorg https://flyteorg.github.io/flyte
helm repo update

# Download the EKS starter values
curl -sL https://raw.githubusercontent.com/flyteorg/flyte/master/charts/flyte-binary/eks-starter.yaml \
  > eks-values.yaml
```

Edit `eks-values.yaml` — plug in your Terraform outputs:

| Helm value | Source |
|------------|--------|
| `configuration.database.host` | `terraform output -raw flyte_db_host` |
| `configuration.database.password` | Step 3 above |
| `configuration.storage.metadataContainer` | `terraform output -raw flyte_bucket` |
| `configuration.storage.userDataContainer` | `terraform output -raw flyte_bucket` |
| `configuration.storage.provider` | `s3` |
| `configuration.storage.providerConfig.s3.region` | `us-east-1` |
| `configuration.storage.providerConfig.s3.authType` | `iam` |
| `serviceAccount.annotations` | `eks.amazonaws.com/role-arn: <flyte_backend_role_arn>` |
| `clusterResourceTemplates.defaultIamRole` | `terraform output -raw flyte_tasks_role_arn` |

```bash
# Install
helm install flyte-backend flyteorg/flyte-binary \
  --namespace flyte --create-namespace \
  --values eks-values.yaml

# Wait for pod to be ready
kubectl -n flyte get pods -w

# Access the console
kubectl -n flyte port-forward svc/flyte-backend-flyte-binary-http 8088:8088
# Open http://localhost:8088/console
```

## Step 5 — Register workflows

Fast registration uploads a source tarball to S3 and overlays it on the pre-built base image at runtime. No Docker build needed for code changes — only rebuild the ECR image when dependencies change.

```bash
# Port-forward Flyte gRPC (if not already running)
kubectl -n flyte port-forward svc/flyte-backend-flyte-binary-grpc 8089:8089 &

cd ../..   # back to repo root
uv run pyflyte register src/constellation/workflows/ \
  --project constellation \
  --domain development \
  --image 696356228955.dkr.ecr.us-east-1.amazonaws.com/constellation:latest
```

To register and run a single-tile workflow in one shot:

```bash
uv run pyflyte run --remote \
  --project constellation --domain development \
  --image 696356228955.dkr.ecr.us-east-1.amazonaws.com/constellation:latest \
  src/constellation/workflows/pipeline.py data_preparation_pipeline \
  --config_yaml configs/edff_single_tile.yaml \
  --tile_ids '[102018211]'
```

## Tear down

```bash
# Uninstall Flyte first (cleans up namespaces Flyte created)
helm uninstall flyte-backend --namespace flyte

# Destroy infrastructure
cd infra/terraform
terraform destroy
```
