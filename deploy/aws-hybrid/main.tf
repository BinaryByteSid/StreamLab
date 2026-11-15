# StreamLab hybrid mode: AWS side only (S3 + CloudWatch + IAM).
# For AWS accounts that may not run servers (EC2/Lightsail/Lambda denied by an
# organization policy). The lab runs on your computer and uses these cloud services.
#
#   terraform init
#   terraform apply
#   terraform output -raw env_file      # paste into VirtualLab/.env.aws on your computer
#   terraform destroy                    # removes everything again
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 6.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

variable "region" {
  description = "AWS region; empty = the region CloudShell is using"
  type        = string
  default     = ""
}

variable "create_alarms" {
  description = "Create CloudWatch alarms (set false if the account's policy denies them)"
  type        = bool
  default     = true
}

provider "aws" {
  region = var.region == "" ? null : var.region
  default_tags {
    tags = { Project = "StreamLab", Course = "ACC" }
  }
}

data "aws_region" "current" {}

locals {
  region = data.aws_region.current.region
  buckets = {
    checkpoints = "streamlab-checkpoints-${random_id.suffix.hex}"
    archive     = "streamlab-archive-${random_id.suffix.hex}"
    logs        = "streamlab-logs-${random_id.suffix.hex}"
  }
}

resource "random_id" "suffix" {
  byte_length = 3
}

# -------------------------------------------------------------------- S3
resource "aws_s3_bucket" "lab" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lab" {
  for_each                = aws_s3_bucket.lab
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --------------------------------------------- IAM user used by the laptop
resource "aws_iam_user" "lab" {
  name = "streamlab-lab-${random_id.suffix.hex}"
}

# Least privilege: only the three buckets and the StreamLab metric namespace.
resource "aws_iam_user_policy" "lab" {
  name = "streamlab-runtime"
  user = aws_iam_user.lab.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"],
      Resource = [for b in aws_s3_bucket.lab : b.arn] },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      Resource = [for b in aws_s3_bucket.lab : "${b.arn}/*"] },
      { Effect = "Allow", Action = "cloudwatch:PutMetricData", Resource = "*",
      Condition = { StringEquals = { "cloudwatch:namespace" = "StreamLab" } } },
    ]
  })
}

resource "aws_iam_access_key" "lab" {
  user = aws_iam_user.lab.name
}

# ------------------------------------------------------------- CloudWatch
resource "aws_cloudwatch_dashboard" "lab" {
  dashboard_name = "StreamLab"
  dashboard_body = jsonencode({
    widgets = [
      { type = "text", x = 0, y = 0, width = 24, height = 2, properties = {
        markdown = "## StreamLab: Streaming Window Metrics Aggregator (live)\nPublished every minute by the StreamLab AWS bridge. The raw stream and window results also land in S3: `${local.buckets.archive}/stream/`."
      } },
      { type = "metric", x = 0, y = 2, width = 12, height = 6, properties = {
        title   = "Throughput (events/s): produced vs processed", region = local.region, view = "timeSeries",
        stat    = "Average", period = 60,
        metrics = [["StreamLab", "EventsProducedPerSec"], ["StreamLab", "EventsProcessedPerSec"], ["StreamLab", "DeadLettersPerSec"]]
      } },
      { type = "metric", x = 12, y = 2, width = 12, height = 6, properties = {
        title   = "Consumer lag (events) and workers alive", region = local.region, view = "timeSeries",
        stat    = "Maximum", period = 60,
        metrics = [["StreamLab", "ConsumerLag"], ["StreamLab", "WorkersAlive", { yAxis = "right" }]]
      } },
      { type = "metric", x = 0, y = 8, width = 8, height = 6, properties = {
        title = "p95 processing latency (ms)", region = local.region, view = "timeSeries",
        stat  = "Average", period = 60, metrics = [["StreamLab", "P95ProcessingLatencyMs"]]
      } },
      { type = "metric", x = 8, y = 8, width = 8, height = 6, properties = {
        title = "Window results emitted /s", region = local.region, view = "timeSeries",
        stat  = "Average", period = 60, metrics = [["StreamLab", "WindowResultsPerSec"]]
      } },
      { type = "metric", x = 16, y = 8, width = 8, height = 6, properties = {
        title = "API requests /s", region = local.region, view = "timeSeries",
        stat  = "Average", period = 60, metrics = [["StreamLab", "ApiRequestsPerSec"]]
      } },
      { type = "metric", x = 0, y = 14, width = 24, height = 4, properties = {
        title = "Right now", region = local.region, view = "singleValue", stat = "Average", period = 60,
        metrics = [["StreamLab", "EventsProducedPerSec"], ["StreamLab", "EventsProcessedPerSec"], ["StreamLab", "ConsumerLag"],
        ["StreamLab", "WorkersAlive"], ["StreamLab", "P95ProcessingLatencyMs"]]
      } },
    ]
  })
}

resource "aws_cloudwatch_metric_alarm" "lag" {
  count               = var.create_alarms ? 1 : 0
  alarm_name          = "streamlab-consumer-lag-high"
  alarm_description   = "Processors are falling behind the producer (backpressure)"
  namespace           = "StreamLab"
  metric_name         = "ConsumerLag"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 1000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "stream_stopped" {
  count               = var.create_alarms ? 1 : 0
  alarm_name          = "streamlab-stream-stopped"
  alarm_description   = "No events processed for 5 minutes"
  namespace           = "StreamLab"
  metric_name         = "EventsProcessedPerSec"
  statistic           = "Average"
  period              = 60
  evaluation_periods  = 5
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
}

# ----------------------------------------------------------------- outputs
output "region" {
  value = local.region
}

output "s3_buckets" {
  value = local.buckets
}

output "cloudwatch_dashboard" {
  value = "https://${local.region}.console.aws.amazon.com/cloudwatch/home?region=${local.region}#dashboards/dashboard/StreamLab"
}

output "s3_stream_folder" {
  value = "https://${local.region}.console.aws.amazon.com/s3/buckets/${local.buckets.archive}?region=${local.region}&prefix=stream/"
}

# Paste this into VirtualLab/.env.aws on your computer (it contains a secret key).
output "env_file" {
  sensitive = true
  value     = <<-EOT
    AWS_REGION=${local.region}
    AWS_ACCESS_KEY_ID=${aws_iam_access_key.lab.id}
    AWS_SECRET_ACCESS_KEY=${aws_iam_access_key.lab.secret}
    BUCKET_CHECKPOINTS=${local.buckets.checkpoints}
    BUCKET_ARCHIVE=${local.buckets.archive}
    BUCKET_LOGS=${local.buckets.logs}
  EOT
}
