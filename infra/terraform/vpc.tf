# -----------------------------------------------------------------------------
# VPC — two AZs, public subnets only (no NAT gateway, saves ~$33/mo).
# EKS nodes get public IPs; security groups block all unsolicited inbound.
# For production, add private subnets + NAT or fck-nat.
# -----------------------------------------------------------------------------

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.16"

  name = local.cluster_name
  cidr = "10.0.0.0/16"

  azs            = local.azs
  public_subnets = ["10.0.1.0/24", "10.0.2.0/24"]

  # No private subnets, no NAT gateway.
  enable_nat_gateway   = false
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Auto-assign public IPs so nodes can reach the internet directly.
  map_public_ip_on_launch = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }
}

# S3 gateway endpoint — FREE. Keeps S3 traffic inside the AWS network
# instead of routing it through the public internet.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.public_route_table_ids
}
