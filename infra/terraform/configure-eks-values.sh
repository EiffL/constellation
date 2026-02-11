#!/usr/bin/env bash
# Fills eks-values.yaml with values from terraform outputs + SSM.
# Run from infra/terraform/ after `terraform apply`.
set -euo pipefail

cd "$(dirname "$0")"

DB_HOST=$(terraform output -raw flyte_db_host)
DB_PASS=$(aws ssm get-parameter \
  --name "$(terraform output -raw flyte_db_password_ssm_name)" \
  --with-decryption --query Parameter.Value --output text)
BUCKET=$(terraform output -raw flyte_bucket)
BACKEND_ROLE=$(terraform output -raw flyte_backend_role_arn)
TASKS_ROLE=$(terraform output -raw flyte_tasks_role_arn)

echo "DB host:      $DB_HOST"
echo "S3 bucket:    $BUCKET"
echo "Backend role: $BACKEND_ROLE"
echo "Tasks role:   $TASKS_ROLE"
echo ""

sed -i \
  -e "s|password: FILL_ME|password: ${DB_PASS}|" \
  -e "s|host: FILL_ME|host: ${DB_HOST}|" \
  -e "s|metadataContainer: FILL_ME|metadataContainer: ${BUCKET}|" \
  -e "s|userDataContainer: FILL_ME|userDataContainer: ${BUCKET}|" \
  -e "s|value: FILL_ME|value: ${TASKS_ROLE}|g" \
  -e "s|eks.amazonaws.com/role-arn: \"FILL_ME\"|eks.amazonaws.com/role-arn: \"${BACKEND_ROLE}\"|" \
  eks-values.yaml

echo "eks-values.yaml configured. Review it, then run:"
echo ""
echo "  helm repo add flyteorg https://flyteorg.github.io/flyte"
echo "  helm install flyte-backend flyteorg/flyte-binary \\"
echo "    --namespace flyte --create-namespace \\"
echo "    --values eks-values.yaml"
