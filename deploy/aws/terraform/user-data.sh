#!/bin/bash
# Runs once when the EC2 instance first boots: install Docker, fetch the
# project, write .env and start the whole lab. Progress: /var/log/streamlab-setup.log
set -eux
exec > >(tee -a /var/log/streamlab-setup.log) 2>&1

apt-get update
apt-get install -y ca-certificates curl git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu noble stable" > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin docker-buildx-plugin
systemctl enable --now docker

git clone ${repo_url} /opt/streamlab
cd /opt/streamlab

cat > .env <<'ENV'
AWS_REGION=${region}
PUBLIC_HOST=${public_host}
BUCKET_CHECKPOINTS=${bucket_checkpoints}
BUCKET_ARCHIVE=${bucket_archive}
BUCKET_LOGS=${bucket_logs}
LAB_ADMIN_PASSWORD=${admin_password}
GRAFANA_PASSWORD=${grafana_password}
JWT_SECRET=${jwt_secret}
OPS_AGENT_TOKEN=${ops_token}
ENV
chmod 600 .env

docker compose -f docker-compose.yml -f docker-compose.aws.yml up -d --build
echo "StreamLab is up: http://${public_host}"
