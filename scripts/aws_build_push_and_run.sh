#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/aws_build_push_and_run.sh [options]

Options:
  --mode <sync|canister|backfill|incremental|full>
                              ETL mode for the one-off ECS run. Default: sync.
                              'canister' (alias 'full') runs the one-time deep-history
                              archive; 'backfill'/'incremental' use the REST API;
                              'sync' runs backfill-if-needed then REST incremental.
  --tag <tag>                 Image tag to build and push. Default: latest
  --terraform-dir <path>      Terraform AWS compute root. Default: terraform/aws-compute
  --page-size <n>             Override page size for the one-off ECS run. Default: 1000
  --concurrency <n>           Override concurrency for the one-off ECS run. Default: 5
  --overlap <n>               Override overlap for incremental runs. Default: 50
  --build-only                Build and push the image, but do not run ECS task
  --push-only                 Push the already-built local image tag, but do not build or run
  --run-only                  Skip build/push and only run the ECS task
  --help                      Show this help

Environment:
  AWS_PROFILE / AWS_REGION / normal AWS credential chain are used by the AWS CLI.

Notes:
  - The script reads cluster, task definition, security group, subnet IDs, and region
    from Terraform outputs in the AWS compute stack.
  - The safest/default path is to use tag 'latest', because the Terraform task definition
    points at ':latest' unless you override container_image in Terraform.
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

MODE="sync"
IMAGE_TAG="latest"
TF_DIR="${REPO_ROOT}/terraform/aws-compute"
PAGE_SIZE="1000"
CONCURRENCY="5"
OVERLAP="50"
DO_BUILD=1
DO_PUSH=1
DO_RUN=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:?missing value for --mode}"
      shift 2
      ;;
    --tag)
      IMAGE_TAG="${2:?missing value for --tag}"
      shift 2
      ;;
    --terraform-dir)
      TF_DIR_INPUT="${2:?missing value for --terraform-dir}"
      if [[ "${TF_DIR_INPUT}" = /* ]]; then
        TF_DIR="${TF_DIR_INPUT}"
      else
        TF_DIR="${REPO_ROOT}/${TF_DIR_INPUT}"
      fi
      shift 2
      ;;
    --page-size)
      PAGE_SIZE="${2:?missing value for --page-size}"
      shift 2
      ;;
    --concurrency)
      CONCURRENCY="${2:?missing value for --concurrency}"
      shift 2
      ;;
    --overlap)
      OVERLAP="${2:?missing value for --overlap}"
      shift 2
      ;;
    --build-only)
      DO_RUN=0
      shift
      ;;
    --push-only)
      DO_BUILD=0
      DO_RUN=0
      shift
      ;;
    --run-only)
      DO_BUILD=0
      DO_PUSH=0
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

case "${MODE}" in
  sync|canister|backfill|incremental|full) ;;
  *)
    echo "Invalid --mode '${MODE}'. Expected one of: sync, canister, backfill, incremental, full." >&2
    exit 1
    ;;
esac

require_cmd aws
require_cmd docker
require_cmd terraform
require_cmd node

if [[ ! -d "${TF_DIR}" ]]; then
  echo "Terraform directory not found: ${TF_DIR}" >&2
  exit 1
fi

tf_output_raw() {
  terraform -chdir="${TF_DIR}" output -raw "$1"
}

tf_output_json() {
  terraform -chdir="${TF_DIR}" output -json "$1"
}

AWS_REGION="$(tf_output_raw aws_region)"
ECR_REPO_URL="$(tf_output_raw ecr_repository_url)"
ECS_CLUSTER_NAME="$(tf_output_raw ecs_cluster_name)"
TASK_DEFINITION_ARN="$(tf_output_raw task_definition_arn)"
TASK_SECURITY_GROUP_ID="$(tf_output_raw task_security_group_id)"
ASSIGN_PUBLIC_IP_RAW="$(tf_output_raw assign_public_ip)"
SUBNET_IDS_JSON="$(tf_output_json effective_subnet_ids)"

if [[ "${ASSIGN_PUBLIC_IP_RAW}" == "true" ]]; then
  ASSIGN_PUBLIC_IP="ENABLED"
else
  ASSIGN_PUBLIC_IP="DISABLED"
fi

NETWORK_CONFIGURATION="$(
  SUBNET_IDS_JSON="${SUBNET_IDS_JSON}" \
  TASK_SECURITY_GROUP_ID="${TASK_SECURITY_GROUP_ID}" \
  ASSIGN_PUBLIC_IP="${ASSIGN_PUBLIC_IP}" \
  node --input-type=module -e '
    const subnets = JSON.parse(process.env.SUBNET_IDS_JSON);
    const sg = process.env.TASK_SECURITY_GROUP_ID;
    const assign = process.env.ASSIGN_PUBLIC_IP;
    const quoted = (values) => values.map((value) => `"${value}"`).join(",");
    process.stdout.write(`awsvpcConfiguration={subnets=[${quoted(subnets)}],securityGroups=["${sg}"],assignPublicIp=${assign}}`);
  '
)"

IMAGE_URI="${ECR_REPO_URL}:${IMAGE_TAG}"
LOCAL_IMAGE_TAG="icpswap-extractor:${IMAGE_TAG}"

echo "AWS region: ${AWS_REGION}"
echo "ECR image: ${IMAGE_URI}"
echo "ECS cluster: ${ECS_CLUSTER_NAME}"
echo "Task definition: ${TASK_DEFINITION_ARN}"

if (( DO_BUILD || DO_PUSH )); then
  aws ecr get-login-password --region "${AWS_REGION}" \
    | docker login --username AWS --password-stdin "${ECR_REPO_URL}"
fi

if (( DO_BUILD )); then
  docker build -t "${LOCAL_IMAGE_TAG}" "${REPO_ROOT}"
fi

if (( DO_PUSH )); then
  docker tag "${LOCAL_IMAGE_TAG}" "${IMAGE_URI}"
  docker push "${IMAGE_URI}"
fi

if (( ! DO_RUN )); then
  exit 0
fi

OVERRIDES_JSON="$(
  MODE="${MODE}" \
  PAGE_SIZE="${PAGE_SIZE}" \
  CONCURRENCY="${CONCURRENCY}" \
  OVERLAP="${OVERLAP}" \
  node --input-type=module -e '
    const mode = process.env.MODE;
    const command = ["node", "dist/index.js", "--mode", mode];
    // Canister archive (one-time) uses page-size/concurrency/overlap; REST modes
    // (sync/backfill/incremental) rely on their own configured defaults.
    if (mode === "canister" || mode === "full") {
      command.push("--page-size", process.env.PAGE_SIZE, "--concurrency", process.env.CONCURRENCY, "--overlap", process.env.OVERLAP);
    }
    process.stdout.write(JSON.stringify({
      containerOverrides: [{ name: "etl", command }]
    }));
  '
)"

if [[ "${IMAGE_TAG}" != "latest" ]]; then
  echo "Warning: task definition still references ':latest' unless Terraform was updated separately." >&2
fi

aws ecs run-task \
  --region "${AWS_REGION}" \
  --cluster "${ECS_CLUSTER_NAME}" \
  --launch-type FARGATE \
  --task-definition "${TASK_DEFINITION_ARN}" \
  --network-configuration "${NETWORK_CONFIGURATION}" \
  --overrides "${OVERRIDES_JSON}"
