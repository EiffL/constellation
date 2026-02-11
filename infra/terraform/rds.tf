# -----------------------------------------------------------------------------
# PostgreSQL for Flyte (flyteadmin + datacatalog share one DB instance)
# -----------------------------------------------------------------------------

# Auto-generated password — never stored in tfvars or on disk.
# It is still present in Terraform state, so use an encrypted remote backend.
resource "random_password" "flyte_db" {
  length  = 32
  special = false # avoids shell-escaping issues when injecting into Helm values
}

# SSM Parameter Store (free) instead of Secrets Manager ($0.40/mo/secret).
resource "aws_ssm_parameter" "flyte_db_password" {
  name  = "/${local.cluster_name}/flyte-db-password"
  type  = "SecureString"
  value = random_password.flyte_db.result
}

resource "aws_db_subnet_group" "flyte" {
  name       = "${local.cluster_name}-flyte"
  subnet_ids = module.vpc.public_subnets
}

resource "aws_security_group" "rds" {
  name_prefix = "${local.cluster_name}-rds-"
  description = "Allow PostgreSQL from EKS nodes"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "PostgreSQL from EKS node security group"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  # No egress rule — RDS does not need to initiate outbound connections.

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_instance" "flyte" {
  identifier = "${local.cluster_name}-flyte"

  engine         = "postgres"
  engine_version = "16"
  instance_class = var.db_instance_class

  allocated_storage = 20
  storage_type      = "gp2" # free tier covers gp2, not gp3
  storage_encrypted = true

  db_name  = "flyteadmin"
  username = "flyte"
  password = random_password.flyte_db.result
  port     = 5432

  db_subnet_group_name   = aws_db_subnet_group.flyte.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  multi_az            = false
  publicly_accessible = false
  skip_final_snapshot = true
  deletion_protection = false # set true for production

  # Minimal backups for dev — increase for production.
  backup_retention_period = 1
}
