# Free-tier style deployment from the feasibility study: one EC2 instance runs the
# whole docker compose stack. Also creates the S3 bucket and ECR repositories.
#   terraform init && terraform apply -var="allowed_cidr=<your-ip>/32"
#   terraform destroy   # when you are done, to avoid charges
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  default = "ap-south-1"
}

variable "instance_type" {
  description = "t3.small or larger: 23 containers need ~3 GB RAM. t2.micro is too small."
  default     = "t3.medium"
}

variable "allowed_cidr" {
  description = "Who may open the lab (your public IP /32). Never leave 0.0.0.0/0 for a real system."
  type        = string
}

variable "repo_url" {
  description = "Git URL of this project"
  default     = "https://github.com/your-user/streamlab.git"
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_vpc" "lab" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "streamlab-vpc" }
}

resource "aws_internet_gateway" "lab" {
  vpc_id = aws_vpc.lab.id
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.lab.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
  tags                    = { Name = "streamlab-public" }
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

# Security group: only the lab entry point (nginx ALB stand-in) from your IP.
resource "aws_security_group" "lab" {
  name   = "streamlab-sg"
  vpc_id = aws_vpc.lab.id
  ingress {
    description = "Virtual lab via the nginx load balancer"
    from_port   = 80
    to_port     = 80
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

resource "aws_s3_bucket" "backups" {
  bucket_prefix = "streamlab-backups-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "backups" {
  bucket                  = aws_s3_bucket.backups.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_ecr_repository" "images" {
  for_each             = toset(["producer", "processor", "api", "ops-agent", "dashboard"])
  name                 = "swma/${each.key}"
  force_delete         = true
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
}

resource "aws_iam_role" "ec2" {
  name = "streamlab-ec2"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "ecr" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_instance_profile" "ec2" {
  name = "streamlab-ec2"
  role = aws_iam_role.ec2.name
}

resource "aws_instance" "lab" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.lab.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name
  user_data              = templatefile("${path.module}/user-data.sh", { repo_url = var.repo_url })
  root_block_device {
    volume_size = 30
  }
  tags = { Name = "streamlab" }
}

output "lab_url" {
  value = "http://${aws_instance.lab.public_dns}"
}

output "backup_bucket" {
  value = aws_s3_bucket.backups.bucket
}

output "ecr_repositories" {
  value = [for r in aws_ecr_repository.images : r.repository_url]
}
