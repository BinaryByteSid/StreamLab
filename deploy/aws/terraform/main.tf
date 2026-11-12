# StreamLab on AWS -- designed for the AWS Free plan (accounts created after 15 July 2025).
#
#   EC2 m7i-flex.large (free-plan eligible, 8 GB) runs the whole stack 24/7
#   Amazon S3          state checkpoints, cold-path archive, Loki log chunks
#   CloudWatch         live stream metrics + dashboard, raw-event / window-result logs, alarms
#   SNS                alarm e-mails          AWS Budgets   spending e-mails
#   Elastic IP         stable public address  SSM           shell access without SSH keys
#
#   terraform init
#   terraform apply -var="alert_email=you@example.com"
#   terraform destroy        # removes everything again
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 6.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = "StreamLab", Course = "ACC" }
  }
}

# ------------------------------------------------------------------ inputs
variable "region" {
  description = "AWS region (Mumbai by default)"
  type        = string
  default     = "ap-south-1"
}

variable "instance_type" {
  description = "m7i-flex.large is free-plan eligible and has the 8 GB the 23 containers need"
  type        = string
  default     = "m7i-flex.large"
}

variable "alert_email" {
  description = "Receives CloudWatch alarm and budget e-mails (confirm the SNS subscription mail)"
  type        = string
}

variable "viewer_cidr" {
  description = "Who may open the lab and Grafana. 0.0.0.0/0 = anyone with the link (e.g. your professor)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "repo_url" {
  description = "Git repository the instance clones"
  type        = string
  default     = "https://github.com/BinaryByteSid/StreamLab.git"
}

variable "budget_usd" {
  description = "Monthly budget (before credits) that triggers an e-mail"
  type        = number
  default     = 5
}

data "aws_caller_identity" "me" {}

data "aws_availability_zones" "azs" {
  state = "available"
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "random_id" "suffix" {
  byte_length = 3
}

resource "random_password" "admin" {
  length  = 16
  special = false
}

resource "random_password" "grafana" {
  length  = 16
  special = false
}

resource "random_password" "jwt" {
  length  = 40
  special = false
}

resource "random_password" "ops" {
  length  = 32
  special = false
}

locals {
  name = "streamlab"
  buckets = {
    checkpoints = "streamlab-checkpoints-${random_id.suffix.hex}"
    archive     = "streamlab-archive-${random_id.suffix.hex}"
    logs        = "streamlab-logs-${random_id.suffix.hex}"
  }
}

# ----------------------------------------------------------------- network
resource "aws_vpc" "lab" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "${local.name}-vpc" }
}

resource "aws_internet_gateway" "lab" {
  vpc_id = aws_vpc.lab.id
}

resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.lab.id
  cidr_block        = "10.20.1.0/24"
  availability_zone = data.aws_availability_zones.azs.names[0]
  tags              = { Name = "${local.name}-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.lab.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.lab.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Security group = firewall. Only the lab (80) and Grafana (3000) are public.
# Kafka, Redis, Consul etc. stay private; use SSM port forwarding to reach them.
resource "aws_security_group" "lab" {
  name        = "${local.name}-sg"
  description = "StreamLab: lab portal and Grafana only"
  vpc_id      = aws_vpc.lab.id

  ingress {
    description = "Virtual lab portal (nginx load balancer)"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = [var.viewer_cidr]
  }
  ingress {
    description = "Grafana (anonymous read-only viewer)"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = [var.viewer_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
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

# Keep storage well inside the 5 GB free allowance.
resource "aws_s3_bucket_lifecycle_configuration" "lab" {
  for_each = aws_s3_bucket.lab
  bucket   = each.value.id
  rule {
    id     = "expire-old-objects"
    status = "Enabled"
    filter {}
    expiration {
      days = each.key == "checkpoints" ? 3 : 14
    }
  }
}

# ------------------------------------------------------------- CloudWatch
resource "aws_cloudwatch_log_group" "containers" {
  name              = "/streamlab/containers"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "stream" {
  name              = "/streamlab/stream"
  retention_in_days = 3
}

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_metric_alarm" "lag" {
  alarm_name          = "${local.name}-consumer-lag-high"
  alarm_description   = "Processors are falling behind the producer (backpressure)"
  namespace           = "StreamLab"
  metric_name         = "ConsumerLag"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 1000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "stream_stopped" {
  alarm_name          = "${local.name}-stream-stopped"
  alarm_description   = "No events processed for 5 minutes (stream paused or pipeline down)"
  namespace           = "StreamLab"
  metric_name         = "EventsProcessedPerSec"
  statistic           = "Average"
  period              = 60
  evaluation_periods  = 5
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "workers" {
  alarm_name          = "${local.name}-no-processor-workers"
  alarm_description   = "No stream-processor worker is alive"
  namespace           = "StreamLab"
  metric_name         = "WorkersAlive"
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_dashboard" "lab" {
  dashboard_name = "StreamLab"
  dashboard_body = jsonencode({
    widgets = [
      { type = "text", x = 0, y = 0, width = 24, height = 2, properties = {
        markdown = "## StreamLab: Streaming Window Metrics Aggregator (live on AWS)\nEC2 runs the pipeline 24/7. Metrics are published every minute by the CloudWatch bridge. Lab: http://${aws_eip.lab.public_ip}  Grafana: http://${aws_eip.lab.public_ip}:3000"
      } },
      { type = "metric", x = 0, y = 2, width = 12, height = 6, properties = {
        title   = "Throughput (events/s): produced vs processed", region = var.region, view = "timeSeries", stat = "Average", period = 60,
        metrics = [["StreamLab", "EventsProducedPerSec"], ["StreamLab", "EventsProcessedPerSec"], ["StreamLab", "DeadLettersPerSec"]]
      } },
      { type = "metric", x = 12, y = 2, width = 12, height = 6, properties = {
        title   = "Consumer lag (events) and workers alive", region = var.region, view = "timeSeries", stat = "Maximum", period = 60,
        metrics = [["StreamLab", "ConsumerLag"], ["StreamLab", "WorkersAlive", { yAxis = "right" }]]
      } },
      { type = "metric", x = 0, y = 8, width = 8, height = 6, properties = {
        title   = "p95 processing latency (ms)", region = var.region, view = "timeSeries", stat = "Average", period = 60,
        metrics = [["StreamLab", "P95ProcessingLatencyMs"]]
      } },
      { type = "metric", x = 8, y = 8, width = 8, height = 6, properties = {
        title   = "Window results emitted /s", region = var.region, view = "timeSeries", stat = "Average", period = 60,
        metrics = [["StreamLab", "WindowResultsPerSec"]]
      } },
      { type = "metric", x = 16, y = 8, width = 8, height = 6, properties = {
        title   = "API requests /s", region = var.region, view = "timeSeries", stat = "Average", period = 60,
        metrics = [["StreamLab", "ApiRequestsPerSec"]]
      } },
      { type = "log", x = 0, y = 14, width = 24, height = 7, properties = {
        title = "Latest window results (from metrics.windows)", region = var.region, view = "table",
        query = "SOURCE '/streamlab/stream' | filter @logStream = 'window-results' | fields @timestamp, @message | sort @timestamp desc | limit 30"
      } },
      { type = "log", x = 0, y = 21, width = 24, height = 7, properties = {
        title = "Raw event sample (from telemetry.events, 1 per second)", region = var.region, view = "table",
        query = "SOURCE '/streamlab/stream' | filter @logStream = 'raw-events' | fields @timestamp, @message | sort @timestamp desc | limit 30"
      } },
      { type = "alarm", x = 0, y = 28, width = 24, height = 3, properties = {
        title = "Alarms", alarms = [aws_cloudwatch_metric_alarm.lag.arn, aws_cloudwatch_metric_alarm.stream_stopped.arn, aws_cloudwatch_metric_alarm.workers.arn]
      } },
    ]
  })
}

# ------------------------------------------------------------------ budget
resource "aws_budgets_budget" "monthly" {
  name         = "${local.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  cost_types {
    include_credit = false # show real usage even while free credits pay for it
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

# --------------------------------------------------------------------- IAM
resource "aws_iam_role" "ec2" {
  name = "${local.name}-ec2"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Least privilege: only our buckets, our log groups, our metric namespace.
resource "aws_iam_role_policy" "lab" {
  name = "${local.name}-runtime"
  role = aws_iam_role.ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"],
      Resource = [for b in aws_s3_bucket.lab : b.arn] },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      Resource = [for b in aws_s3_bucket.lab : "${b.arn}/*"] },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"],
      Resource = ["${aws_cloudwatch_log_group.containers.arn}:*", "${aws_cloudwatch_log_group.stream.arn}:*"] },
      { Effect = "Allow", Action = "cloudwatch:PutMetricData", Resource = "*",
      Condition = { StringEquals = { "cloudwatch:namespace" = "StreamLab" } } },
    ]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${local.name}-ec2"
  role = aws_iam_role.ec2.name
}

# --------------------------------------------------------------------- EC2
resource "aws_eip" "lab" {
  domain = "vpc"
  tags   = { Name = "${local.name}-ip" }
}

resource "aws_instance" "lab" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.lab.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  # Containers reach the instance role through IMDSv2, which needs a hop limit of 2.
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = 30 # the free allowance for EBS
    volume_type = "gp3"
  }

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/user-data.sh", {
    repo_url           = var.repo_url
    region             = var.region
    public_host        = aws_eip.lab.public_ip
    bucket_checkpoints = local.buckets.checkpoints
    bucket_archive     = local.buckets.archive
    bucket_logs        = local.buckets.logs
    admin_password     = random_password.admin.result
    grafana_password   = random_password.grafana.result
    jwt_secret         = random_password.jwt.result
    ops_token          = random_password.ops.result
  })

  tags = { Name = local.name }
  depends_on = [aws_iam_role_policy.lab, aws_cloudwatch_log_group.containers, aws_cloudwatch_log_group.stream,
  aws_s3_bucket.lab]
}

resource "aws_eip_association" "lab" {
  instance_id   = aws_instance.lab.id
  allocation_id = aws_eip.lab.id
}

# ----------------------------------------------------------------- outputs
output "lab_url" {
  value = "http://${aws_eip.lab.public_ip}"
}

output "grafana_url" {
  value = "http://${aws_eip.lab.public_ip}:3000"
}

output "cloudwatch_dashboard" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards/dashboard/StreamLab"
}

output "cloudwatch_live_tail" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#logsV2:live-tail (log group /streamlab/stream)"
}

output "s3_buckets" {
  value = local.buckets
}

output "shell" {
  value = "aws ssm start-session --target ${aws_instance.lab.id} --region ${var.region}"
}

output "lab_admin_password" {
  value     = random_password.admin.result
  sensitive = true
}

output "grafana_admin_password" {
  value     = random_password.grafana.result
  sensitive = true
}
