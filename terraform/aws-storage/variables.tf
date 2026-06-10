variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "icpswap-extractor"
}

variable "bucket_name" {
  type        = string
  default     = ""
  description = "Optional S3 bucket name. Leave empty to let Terraform create a deterministic name with account ID suffix."
}

variable "noncurrent_version_expiration_days" {
  type        = number
  default     = 30
  description = "Days to keep noncurrent object versions before expiring them. The bucket is versioned, so without this rule storage grows forever."
}

variable "tags" {
  type    = map(string)
  default = {}
}
