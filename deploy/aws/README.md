# Run StreamLab on AWS (Free plan)

This puts the whole lab on AWS, running 24/7, so the event stream keeps flowing in the
background and can be shown from anywhere: the lab portal, Grafana, and the stream itself
inside **Amazon CloudWatch**.

## What gets created

| AWS service | Used for |
|---|---|
| **EC2** `m7i-flex.large` (2 vCPU, 8 GB) + Elastic IP | runs all lab containers; producer streams continuously |
| **Amazon S3** (3 buckets) | state checkpoints, cold-path archive of final windows, Loki log chunks |
| **CloudWatch Metrics** (namespace `StreamLab`) | events/s in and out, consumer lag, workers, p95 latency, DLQ, API rate (every minute) |
| **CloudWatch Logs** `/streamlab/stream` | live raw-event sample and every window result: watch with **Live Tail** |
| **CloudWatch Logs** `/streamlab/containers` | stdout of producer, processor, APIs, bootstrap, ops-agent |
| **CloudWatch Dashboard** `StreamLab` | all of the above on one page |
| **CloudWatch Alarms + SNS** | e-mails when lag is high, the stream stops, or no worker is alive |
| **AWS Budgets** | e-mail if usage passes 50% of $5/month (before credits) |
| **VPC, subnet, security group, IAM role, SSM** | network, firewall (only ports 80 and 3000 open), credentials without keys, shell without SSH |

## Cost

Use a **new AWS account on the Free plan** (accounts created after 15 July 2025).
`m7i-flex.large` is free-plan eligible, and the account gets $100–200 of credits for up to
6 months. On the Free plan AWS does not charge your card: when credits or the 6 months run
out, the account is paused until you choose to upgrade. The Elastic IP (about $3.60/month)
and CloudWatch usage are paid from the credits. Run `terraform destroy` when the evaluation
is over.

Check in the EC2 console that `m7i-flex.large` shows the **Free tier eligible** label in
your region before deploying. An account created before 15 July 2025 is on the old free
tier, where this instance type is **charged**.

## Step 1: push the latest code to GitHub

The EC2 instance clones `https://github.com/BinaryByteSid/StreamLab.git`, so push first
(the repository must be public, or change `repo_url`).

## Step 2: open AWS CloudShell

Sign in to the AWS console, select the **Asia Pacific (Mumbai) ap-south-1** region (top
right), and click the CloudShell icon (`>_`) in the top bar. CloudShell is already
signed in as you, so no access keys are needed.

## Step 3: install Terraform in CloudShell (once)

```bash
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo
sudo dnf install -y terraform
```

## Step 4: deploy

```bash
git clone https://github.com/BinaryByteSid/StreamLab.git
cd StreamLab/deploy/aws/terraform
export TF_DATA_DIR=/tmp/tfdata        # provider plugins are big; keep them out of the 1 GB home
terraform init
terraform apply -var="alert_email=YOUR_EMAIL"
```

Type `yes` when asked. It takes about 2 minutes, then the instance needs **another 10–15
minutes** to install Docker and build the containers.

**Confirm the e-mail** from "AWS Notifications" (SNS subscription), or alarms can't reach you.

## Step 5: open it

```bash
terraform output                       # lab_url, grafana_url, cloudwatch_dashboard, ...
terraform output -raw lab_admin_password
```

* **Lab portal:** `http://<elastic-ip>`. Sign in for admin actions with `admin` and the password above.
* **Grafana:** `http://<elastic-ip>:3000` (anonymous read-only).
* **CloudWatch dashboard:** the `cloudwatch_dashboard` link (AWS login needed).
* **Live stream in AWS:** CloudWatch → Logs → **Live Tail** → log group `/streamlab/stream`.
  You see one raw event per second and every window result as it is computed.
* **S3:** S3 console → the three `streamlab-*` buckets. Checkpoints appear every 30 s, archive files every 30 s.

## Check on the server

```bash
aws ssm start-session --target <instance-id>          # printed as "shell" in terraform output
sudo tail -f /var/log/streamlab-setup.log              # first-boot progress
cd /opt/streamlab && sudo docker compose -f docker-compose.yml -f docker-compose.aws.yml ps
```

To reach the private UIs (Redpanda Console, Consul, Prometheus) from your own PC, use SSM
port forwarding (needs the AWS CLI and Session Manager plugin installed locally):

```bash
aws ssm start-session --target <instance-id> --document-name AWS-StartPortForwardingSession --parameters "portNumber=8081,localPortNumber=8081"
```

## Update or remove

* New code: push to GitHub, then on the server:
  `cd /opt/streamlab && sudo git pull && sudo docker compose -f docker-compose.yml -f docker-compose.aws.yml up -d --build`
* Remove everything (buckets included): `terraform destroy -var="alert_email=YOUR_EMAIL"`

## Security notes

* Only ports 80 (lab) and 3000 (Grafana, read-only) are open. Restrict them to known IPs
  with `-var="viewer_cidr=203.0.113.7/32"`.
* Admin, Grafana, JWT and internal tokens are random per deployment (in the Terraform
  state and in `/opt/streamlab/.env` on the server). The Terraform state therefore contains
  secrets, so keep the CloudShell home private.
* The server holds no AWS keys: S3, CloudWatch and logs use the EC2 instance role,
  limited to the StreamLab buckets, log groups and metric namespace.
