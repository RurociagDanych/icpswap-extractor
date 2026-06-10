data "aws_caller_identity" "current" {}

locals {
  effective_bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.name_prefix}-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  common_tags = merge(
    {
      Project     = "icpswap-extractor"
      ManagedBy   = "terraform"
      Environment = "shared"
      Layer       = "storage"
    },
    var.tags
  )
}

resource "aws_s3_bucket" "raw_data" {
  bucket = local.effective_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "raw_data_tls_only" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.raw_data.arn,
      "${aws_s3_bucket.raw_data.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  policy = data.aws_iam_policy_document.raw_data_tls_only.json
}
