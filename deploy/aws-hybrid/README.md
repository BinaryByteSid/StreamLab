# StreamLab hybrid mode: your laptop + AWS S3 and CloudWatch

Use this when your AWS account **cannot run servers**. Some college/event sandbox accounts
block EC2, Lightsail, Lambda and ECS with an organization policy (an error such as
`... with an explicit deny in a service control policy`). Such accounts often still allow
**S3, CloudWatch and IAM**, and that's all this mode needs.

* The full lab (23 containers) runs on your laptop as usual.
* **Amazon S3** stores the processor's state checkpoints (every 30 s), the archive of final
  windows, Loki's log chunks, and a copy of the **live stream**: every minute a new JSON-lines
  file of raw events and window results appears under `stream/`.
* **Amazon CloudWatch** shows the **StreamLab** dashboard: events/s in and out, consumer lag,
  workers alive, p95 latency, dead letters, window results, API requests, updated every minute,
  plus two alarms.

(If you have a normal AWS account where EC2 works, use `deploy/aws` instead: the whole lab
then runs in the cloud 24/7.)

## 1. Create the AWS side (CloudShell, about 2 minutes)

In the AWS console, open **CloudShell** (`>_`). Install Terraform once:

```bash
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo
sudo dnf install -y terraform
```

Then:

```bash
cd ~ && rm -rf StreamLab && git clone https://github.com/BinaryByteSid/StreamLab.git
cd StreamLab/deploy/aws-hybrid
export TF_DATA_DIR=/tmp/tfdata
terraform init
terraform apply
```

If `apply` fails only on the alarms, run `terraform apply -var="create_alarms=false"`.

## 2. Copy the keys to your laptop

```bash
terraform output -raw env_file
```

This prints six lines (region, an access key, a secret key and three bucket names). Create
a file named **`.env.aws`** in the `VirtualLab` folder on your laptop and paste them in. The
file is git-ignored; keep it private. The key belongs to an IAM user that may only use the
StreamLab buckets and metrics.

## 3. Start the lab in hybrid mode

Double-click **`START-LAB-AWS.bat`** (or run
`docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.hybrid.yml up -d --build`).

The lab opens on the **AWS deployment** page, which now shows "Connected to AWS (hybrid mode)"
with buttons to the CloudWatch dashboard and the live stream folder in S3.

## 4. What to show

* **CloudWatch → Dashboards → StreamLab**: graphs fill within 2–3 minutes and update every minute.
* **S3 → `streamlab-archive-…` → `stream/raw-events/…` and `stream/window-results/…`**:
  open the newest file to see the stream that just passed through (a new file every minute).
* **S3 → `streamlab-checkpoints-…` → `partition-N/latest.json`**: the processor's state,
  refreshed every 30 s.
* The lab itself at http://localhost:8080 (Kafka page, dashboard, Grafana, experiments).

## Remove it

In CloudShell, in `StreamLab/deploy/aws-hybrid`: `terraform destroy`. This deletes the buckets,
the IAM user and its key, the dashboard and the alarms. On the laptop, delete `.env.aws` and
start with `START-LAB.bat` again for the purely local lab.

If CloudShell was restarted in between, run `export TF_DATA_DIR=/tmp/tfdata && terraform init`
again first. The Terraform state stays in the CloudShell home folder.
