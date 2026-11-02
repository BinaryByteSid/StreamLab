#!/bin/bash
# Runs once when the EC2 instance boots: install Docker, fetch the project, start the lab.
set -eux
apt-get update
apt-get install -y ca-certificates curl git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu noble stable" > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
git clone ${repo_url} /opt/streamlab
cd /opt/streamlab
# The lab publishes its ports on 127.0.0.1 only; expose just the load balancer on port 80.
sed -i 's/"127.0.0.1:8080:80"/"80:80"/' docker-compose.yml
docker compose up -d --build
