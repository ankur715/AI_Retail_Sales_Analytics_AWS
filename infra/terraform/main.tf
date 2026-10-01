terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # Account id suffix keeps the bucket name globally unique.
  bucket_name = "${var.project}-lake-${data.aws_caller_identity.current.account_id}"
}

# ---------------------------------------------------------------------------
# S3 data lake. One bucket, one prefix per environment:
#   <env>/landing/<client>/<retailer>/   retailer files as received (S3 feeds land here)
#   <env>/parsed/.../<load_id>/          parser output.csv, what Redshift COPYs
#   <env>/rejects/.../<load_id>/         parse rejects + unmapped products, for the ops team
#   <env>/archive/<client>/<retailer>/   source files after a successful load
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = true # dummy data only -- lets `terraform destroy` clean up fully
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning: archiving is copy + delete, so a mistaken move is recoverable.
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_versioning.lake]

  dynamic "rule" {
    # parsed/ is reproducible from archive/; rejects only matter until someone fixes the STM.
    for_each = { for pair in setproduct(var.environments, ["parsed", "rejects"]) : "${pair[0]}-${pair[1]}" => pair }
    content {
      id     = "expire-${rule.key}"
      status = "Enabled"
      filter {
        prefix = "${rule.value[0]}/${rule.value[1]}/"
      }
      expiration {
        days = rule.value[1] == "parsed" ? 30 : 90
      }
    }
  }

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_public_access_block.lake]
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# ---------------------------------------------------------------------------
# IAM, least privilege:
#   - redshift_copy: assumed by Redshift to COPY parser output (parsed/ only)
#   - pipeline user: what Airflow runs as (landing/parsed/rejects/archive)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "redshift_copy" {
  name = "${var.project}-redshift-copy"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"] }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_copy" {
  name = "read-parsed-output"
  role = aws_iam_role.redshift_copy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = [for env in var.environments : "${aws_s3_bucket.lake.arn}/${env}/parsed/*"]
      },
      {
        Effect    = "Allow"
        Action    = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource  = aws_s3_bucket.lake.arn
        Condition = { StringLike = { "s3:prefix" = [for env in var.environments : "${env}/parsed/*"] } }
      }
    ]
  })
}

resource "aws_iam_user" "pipeline" {
  name = "${var.project}-pipeline"
}

resource "aws_iam_user_policy" "pipeline" {
  name = "pipeline-s3-access"
  user = aws_iam_user.pipeline.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [for env in var.environments : "${aws_s3_bucket.lake.arn}/${env}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      }
    ]
  })
}

# Access key for the local Airflow. The secret lands in terraform state,
# which is gitignored; a real deployment would use an instance/task role.
resource "aws_iam_access_key" "pipeline" {
  user = aws_iam_user.pipeline.name
}

# ---------------------------------------------------------------------------
# Redshift Serverless: bills per RPU-second only while queries run. Dev and
# prod are two databases (retail_dev, retail_prod) in one namespace.
# ---------------------------------------------------------------------------

data "aws_vpc" "default" {
  default = true
}

# Redshift Serverless needs subnets in >= 3 AZs and doesn't support every AZ.
data "aws_subnets" "redshift" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "availability-zone"
    values = var.redshift_azs
  }
}

resource "aws_security_group" "redshift" {
  name        = "${var.project}-redshift"
  description = "Redshift Serverless access from the developer IP only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Redshift from my IP"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name       = var.project
  db_name              = "retail_dev" # retail_prod is created by `RETAIL_ENV=prod python -m etl.migrate`
  admin_username       = var.redshift_admin_username
  admin_user_password  = var.redshift_admin_password
  iam_roles            = [aws_iam_role.redshift_copy.arn]
  default_iam_role_arn = aws_iam_role.redshift_copy.arn
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name = var.project
  base_capacity  = var.redshift_base_rpu
  # Public endpoint so the laptop-hosted Airflow can reach it; the security
  # group limits it to one /32. A real deployment keeps it private.
  publicly_accessible = true
  subnet_ids          = data.aws_subnets.redshift.ids
  security_group_ids  = [aws_security_group.redshift.id]
}

resource "aws_redshiftserverless_usage_limit" "daily_compute" {
  resource_arn  = aws_redshiftserverless_workgroup.this.arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_daily_rpu_hours
  period        = "daily"
  breach_action = "deactivate" # stop serving queries rather than keep billing
}
