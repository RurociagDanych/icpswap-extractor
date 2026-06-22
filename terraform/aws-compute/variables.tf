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
  description = "Existing S3 bucket created by the aws-storage stack."
}

variable "vpc_id" {
  type    = string
  default = null

  validation {
    condition     = var.use_default_vpc || var.vpc_id != null
    error_message = "When use_default_vpc is false, provide vpc_id."
  }
}

variable "use_default_vpc" {
  type        = bool
  default     = true
  description = "For test and low-friction setups only: when true, the stack uses the account default VPC/subnets to avoid provisioning dedicated networking."
}

variable "subnet_ids" {
  type    = list(string)
  default = []

  validation {
    condition     = var.use_default_vpc || length(var.subnet_ids) > 0
    error_message = "When use_default_vpc is false, provide at least one subnet ID."
  }
}

variable "assign_public_ip" {
  type    = bool
  default = true
}

variable "container_image" {
  type        = string
  default     = ""
  description = "Optional image URI. Leave empty to use the managed ECR repository URL with :latest."
}

variable "container_cpu" {
  type    = number
  default = 1024
}

variable "container_memory" {
  type    = number
  default = 2048
}

variable "container_command" {
  type = list(string)
  # Scheduled runs use 'sync': backfill-if-needed then REST incremental. The
  # one-time canister archive is run manually (scripts/aws_build_push_and_run.sh --mode canister).
  default = ["node", "dist/index.js", "--mode", "sync"]
}

variable "s3_prefix" {
  type    = string
  default = "icpswap"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "schedule_expression" {
  type        = string
  default     = ""
  description = "Optional EventBridge Scheduler expression, for example rate(24 hour) or cron(0 * * * ? *)."
}

variable "task_timeout_seconds" {
  type    = number
  default = 3600
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "Optional email address subscribed to the failure alert SNS topic. Leave empty to skip the subscription (the topic and EventBridge rule are still created)."
}

variable "tags" {
  type    = map(string)
  default = {}
}
