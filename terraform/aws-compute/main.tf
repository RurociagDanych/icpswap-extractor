data "aws_partition" "current" {}

data "aws_vpc" "default" {
  count   = var.use_default_vpc ? 1 : 0
  default = true
}

data "aws_subnets" "default_vpc" {
  count = var.use_default_vpc ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
}

data "aws_s3_bucket" "raw_data" {
  bucket = var.bucket_name
}

locals {
  image_uri            = var.container_image != "" ? var.container_image : "${aws_ecr_repository.etl.repository_url}:latest"
  effective_vpc_id     = var.use_default_vpc ? data.aws_vpc.default[0].id : var.vpc_id
  effective_subnet_ids = var.use_default_vpc ? data.aws_subnets.default_vpc[0].ids : var.subnet_ids
  common_tags = merge(
    {
      Project     = "icpswap-extractor"
      ManagedBy   = "terraform"
      Environment = "shared"
      Layer       = "compute"
    },
    var.tags
  )
}

resource "aws_cloudwatch_log_group" "etl" {
  name              = "/aws/ecs/${var.name_prefix}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_ecr_repository" "etl" {
  name                 = var.name_prefix
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags
}

resource "aws_ecs_cluster" "etl" {
  name = "${var.name_prefix}-cluster"
  tags = local.common_tags
}

resource "aws_security_group" "etl_task" {
  name        = "${var.name_prefix}-task"
  description = "Network policy for ICP ETL Fargate tasks"
  vpc_id      = local.effective_vpc_id
  tags        = local.common_tags
}

resource "aws_vpc_security_group_egress_rule" "https" {
  security_group_id = aws_security_group.etl_task.id
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
  description       = "HTTPS egress for IC API, ECR, S3, and CloudWatch"
}

resource "aws_vpc_security_group_egress_rule" "dns_udp" {
  security_group_id = aws_security_group.etl_task.id
  ip_protocol       = "udp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = "0.0.0.0/0"
  description       = "DNS UDP egress"
}

resource "aws_vpc_security_group_egress_rule" "dns_tcp" {
  security_group_id = aws_security_group.etl_task.id
  ip_protocol       = "tcp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = "0.0.0.0/0"
  description       = "DNS TCP egress"
}

data "aws_iam_policy_document" "ecs_task_execution_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${var.name_prefix}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_managed" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name               = "${var.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "task_s3_access" {
  statement {
    sid    = "ListOnlyWithinPrefix"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [data.aws_s3_bucket.raw_data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.s3_prefix}",
        "${var.s3_prefix}/*",
      ]
    }
  }

  statement {
    sid    = "ReadWriteObjectsWithinPrefix"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${data.aws_s3_bucket.raw_data.arn}/${var.s3_prefix}/*"]
  }
}

resource "aws_iam_policy" "task_s3_access" {
  name   = "${var.name_prefix}-task-s3"
  policy = data.aws_iam_policy_document.task_s3_access.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "task_s3_access" {
  role       = aws_iam_role.ecs_task.name
  policy_arn = aws_iam_policy.task_s3_access.arn
}

resource "aws_ecs_task_definition" "etl" {
  family                   = var.name_prefix
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = tostring(var.container_cpu)
  memory                   = tostring(var.container_memory)
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "etl"
      image     = local.image_uri
      essential = true
      command   = var.container_command
      environment = [
        {
          name  = "S3_BUCKET"
          value = data.aws_s3_bucket.raw_data.bucket
        },
        {
          name  = "S3_PREFIX"
          value = var.s3_prefix
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.etl.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "etl"
        }
      }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  tags = local.common_tags
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
  tags = local.common_tags
}

data "aws_iam_policy_document" "alerts_topic" {
  statement {
    sid    = "AllowEventBridgePublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts_topic.json
}

resource "aws_sns_topic_subscription" "alert_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_event_rule" "task_failed" {
  name        = "${var.name_prefix}-task-failed"
  description = "ICP ETL Fargate task stopped abnormally (non-zero exit code or failed to start)"

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      clusterArn = [aws_ecs_cluster.etl.arn]
      lastStatus = ["STOPPED"]
      "$or" = [
        { containers = { exitCode = [{ anything-but = 0 }] } },
        { stopCode = ["TaskFailedToStart"] },
      ]
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "task_failed_sns" {
  rule = aws_cloudwatch_event_rule.task_failed.name
  arn  = aws_sns_topic.alerts.arn
}

data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  count              = var.schedule_expression != "" ? 1 : 0
  name               = "${var.name_prefix}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "scheduler_run_task" {
  count = var.schedule_expression != "" ? 1 : 0

  statement {
    sid    = "RunEcsTask"
    effect = "Allow"
    actions = [
      "ecs:RunTask",
    ]
    resources = [aws_ecs_task_definition.etl.arn]
  }

  statement {
    sid    = "PassTaskRoles"
    effect = "Allow"
    actions = [
      "iam:PassRole",
    ]
    resources = [
      aws_iam_role.ecs_task_execution.arn,
      aws_iam_role.ecs_task.arn,
    ]
  }
}

resource "aws_iam_policy" "scheduler_run_task" {
  count  = var.schedule_expression != "" ? 1 : 0
  name   = "${var.name_prefix}-scheduler-run-task"
  policy = data.aws_iam_policy_document.scheduler_run_task[0].json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "scheduler_run_task" {
  count      = var.schedule_expression != "" ? 1 : 0
  role       = aws_iam_role.scheduler[0].name
  policy_arn = aws_iam_policy.scheduler_run_task[0].arn
}

resource "aws_scheduler_schedule" "etl" {
  count       = var.schedule_expression != "" ? 1 : 0
  name        = "${var.name_prefix}-schedule"
  group_name  = "default"
  description = "Scheduled ICP ETL task"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"
  state                        = "ENABLED"

  target {
    arn      = aws_ecs_cluster.etl.arn
    role_arn = aws_iam_role.scheduler[0].arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.etl.arn
      launch_type         = "FARGATE"
      platform_version    = "LATEST"
      task_count          = 1

      network_configuration {
        assign_public_ip = var.assign_public_ip
        subnets          = local.effective_subnet_ids
        security_groups  = [aws_security_group.etl_task.id]
      }
    }

    retry_policy {
      maximum_event_age_in_seconds = var.task_timeout_seconds
      maximum_retry_attempts       = 2
    }
  }
}
