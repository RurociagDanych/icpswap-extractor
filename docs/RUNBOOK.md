# Runbook

Operational guide for the ICPSwap extractor: running locally, testing, deploying to AWS with Terraform, day-2 operations, and teardown.

## Prerequisites

| Tool | Needed for | Version |
| --- | --- | --- |
| Node.js + npm | local runs, tests, the helper script | Node 22+ |
| Terraform | AWS infrastructure | >= 1.5 |
| AWS CLI v2 | deploy + verification | any recent |
| Docker | building/pushing the container image | any recent |

AWS commands use the normal credential chain (`AWS_PROFILE`, env vars, or SSO). The IAM principal applying Terraform needs rights to manage S3, ECR, ECS, IAM, CloudWatch, EventBridge, and SNS.

---

## 1. Run locally

Local mode needs no AWS account — output goes to `./out/`.

```bash
npm install

# incremental: fetch only new records on the active canister
npm run dev -- --mode incremental --overlap 50

# full: fetch every storage canister (long; resumable if interrupted)
npm run dev -- --mode full --page-size 1000 --concurrency 5
```

Or run the compiled build (what the container runs):

```bash
npm run build
npm start -- --mode incremental --overlap 50
```

### What you get in `./out/`

| File | Meaning |
| --- | --- |
| `incremental_<canisterId>.csv` or `NNNN_<canisterId>.csv` | extracted swap rows (28-column CSV) |
| `manifest_<mode>_<runId>.json` | per-file rows/bytes/sha256 + totals for the run |
| `state.json` | resume state (per-canister progress, recent hashes) |
| `etl.log` | the same JSON log lines that went to stdout |
| `run.lock` | present only while a run is active |

### Useful flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--mode` | `full` | `full` or `incremental` |
| `--page-size` | `1000` | 1..1000 |
| `--concurrency` | `5` | 1..20, full mode only |
| `--overlap` | `50` | incremental re-fetch window for dedupe |
| `--out-dir` / `--state-file` / `--log-file` | under `./out` | local paths |

### Local gotchas

- **Interrupted full load**: just rerun the same command — completed canisters are skipped via `state.json`.
- **Stale lock after a kill**: if a run was killed (Ctrl-C is handled, `kill -9` is not), `./out/run.lock` may remain. A new run within 6 hours will log `another run holds the lock` and exit. Delete it manually: `rm ./out/run.lock`.
- **Fresh start**: `rm -rf ./out` wipes data, state, and lock.

---

## 2. Run tests

```bash
npm test          # node:test suites in tests/ (config, csv, logger, state, storageTarget)
npm run build     # strict TypeScript compile — treat failures as test failures
```

Optional, what CI also enforces (run via Docker if terraform isn't installed):

```bash
docker run --rm -v "$PWD/terraform:/tf" -w /tf hashicorp/terraform:1.9 fmt -check -recursive
docker run --rm -v "$PWD/terraform:/tf" -w /tf/aws-storage hashicorp/terraform:1.9 init -backend=false -input=false
docker run --rm -v "$PWD/terraform:/tf" -w /tf/aws-storage hashicorp/terraform:1.9 validate
# repeat init+validate with -w /tf/aws-compute
```

All of the above runs automatically in GitHub Actions on every push/PR (`.github/workflows/ci.yml`).

---

## 3. Deploy to AWS

The infrastructure is two independent Terraform roots, applied in order:

1. `terraform/aws-storage` — the persistent raw-data bucket (survives compute teardown).
2. `terraform/aws-compute` — ECR, ECS/Fargate, IAM, CloudWatch, scheduler, alerting.

### Step 3.1 — storage root

```bash
cd terraform/aws-storage
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars:
#   aws_region                          - target region
#   bucket_name                         - leave "" for an auto-generated unique name
#   noncurrent_version_expiration_days  - default 30
terraform init
terraform apply
terraform output bucket_name        # <- needed in the next step
```

### Step 3.2 — compute root

```bash
cd ../aws-compute
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars:
#   bucket_name         - REQUIRED: the output from step 3.1
#   aws_region          - same region as storage
#   use_default_vpc     - true for evaluation; for production set false and fill vpc_id/subnet_ids
#   schedule_expression - e.g. "rate(24 hour)"; set "" to disable scheduled runs
#   alert_email         - your email for failure alerts (recommended)
#   container_command   - default runs incremental mode
terraform init
terraform apply
```

If you set `alert_email`, **confirm the subscription** via the email AWS sends, or alerts go nowhere.

### Step 3.3 — build, push, and first run

The helper script reads everything (region, ECR URL, cluster, task definition, networking) from the compute root's Terraform outputs:

```bash
cd ../..   # repo root

# first load: full backfill of all canisters
scripts/aws_build_push_and_run.sh --mode full

# subsequent ad hoc runs (the scheduler does this automatically)
scripts/aws_build_push_and_run.sh --mode incremental

# push a new image without triggering a run
scripts/aws_build_push_and_run.sh --build-only
```

The script: logs into ECR, `docker build`s from the repo root, pushes `:latest`, and (unless `--build-only`) calls `aws ecs run-task` with the right network configuration and command overrides.

### Step 3.4 — verify the deployment

```bash
REGION=$(terraform -chdir=terraform/aws-compute output -raw aws_region)
BUCKET=$(terraform -chdir=terraform/aws-compute output -raw bucket_name)

# data landed? newest entries should show <runId>/...csv and manifest.json
aws s3 ls "s3://${BUCKET}/icpswap/full/" --recursive | tail -5
aws s3 ls "s3://${BUCKET}/icpswap/incremental/" --recursive | tail -5

# state object exists?
aws s3 ls "s3://${BUCKET}/icpswap/state/"

# logs (JSON lines; filter in CloudWatch Logs Insights with: fields ts, level, msg)
aws logs tail /aws/ecs/icpswap-extractor --region "$REGION" --since 1h
```

A healthy run ends with a `full load done` or `incremental done` log line, and the run's `manifest.json` totals match the CSV files next to it.

---

## 4. Day-2 operations

- **A run failed**: you get an SNS email (if `alert_email` is set). Check CloudWatch logs for the `error` line; the run's prefix will have no `manifest.json` — that's the marker of an incomplete run. Re-running is safe: state was only advanced for canisters that completed.
- **Stale S3 lock** (task was killed mid-run): new runs exit with `another run holds the lock` until the 6-hour TTL expires. To clear immediately:
  ```bash
  aws s3 rm "s3://${BUCKET}/icpswap/locks/run.lock"
  ```
- **Deploy a code change**: `scripts/aws_build_push_and_run.sh --build-only` pushes a new `:latest`; the next scheduled run picks it up automatically (the task definition references `:latest`).
- **Change the schedule / alerting / resources**: edit `terraform/aws-compute/terraform.tfvars`, `terraform apply`.
- **Pause scheduled runs**: set `schedule_expression = ""` and apply.

---

## 5. Teardown

Remove compute but **keep all collected data**:

```bash
cd terraform/aws-compute
terraform destroy
```

Full teardown including data (irreversible — the bucket must be emptied first because it is versioned):

```bash
BUCKET=$(terraform -chdir=terraform/aws-storage output -raw bucket_name)
aws s3 rm "s3://${BUCKET}" --recursive
# versioned buckets also need delete markers/versions removed; simplest via console "Empty bucket",
# or: aws s3api delete-objects with the output of list-object-versions
cd terraform/aws-storage
terraform destroy
```
