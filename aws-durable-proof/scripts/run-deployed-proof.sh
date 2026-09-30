#!/usr/bin/env bash
set -euo pipefail

region="us-east-2"
role_arn=""
stack_name=""
keep_success=false
keep_failure=false
promotion_only=false
history_only=false

usage() {
  echo "usage: $0 --stack-name NAME [--region REGION] [--role-arn ARN] [--keep-on-success] [--keep-on-failure] [--promotion-only] [--history-only]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) region="$2"; shift 2 ;;
    --role-arn) role_arn="$2"; shift 2 ;;
    --stack-name) stack_name="$2"; shift 2 ;;
    --keep-on-success) keep_success=true; shift ;;
    --keep-on-failure) keep_failure=true; shift ;;
    --promotion-only) promotion_only=true; shift ;;
    --history-only) history_only=true; shift ;;
    *) usage; exit 2 ;;
  esac
done

if [[ -z "$stack_name" || ! "$stack_name" =~ ^[a-z0-9-]+$ ]]; then
  usage
  echo "stack name must contain only lowercase letters, digits, and hyphens" >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"
maven_repo="$PWD/.m2/repository"

caller_arn=$(aws sts get-caller-identity --region "$region" --query Arn --output text)
if [[ -n "$role_arn" ]]; then
  read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN < <(
    aws sts assume-role \
      --region "$region" \
      --role-arn "$role_arn" \
      --role-session-name "tpf-issue-976-${stack_name}" \
      --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
      --output text
  )
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  caller_arn=$(aws sts get-caller-identity --region "$region" --query Arn --output text)
fi

if [[ "$caller_arn" == *":root" || "$caller_arn" != arn:* ]]; then
  echo "proof requires a non-root AWS role session" >&2
  exit 3
fi

account_id=$(aws sts get-caller-identity --region "$region" --query Account --output text)
artifact_bucket="${stack_name}-artifacts-${account_id}-${region}"
proof_tmp=$(mktemp -d "${TMPDIR:-/tmp}/tpf-issue-976.XXXXXX")
run_succeeded=false
packaged_template="$proof_tmp/packaged.yaml"
driver_jar="$PWD/aws-durable-proof/durable-driver/target/pipelineframework-aws-durable-proof-driver-26.9.4-SNAPSHOT-lambda.jar"
driver_key="${stack_name}/durable-driver.jar"
action_host_zip="$PWD/aws-durable-proof/action-host/target/function.zip"
report_path="$PWD/aws-durable-proof/proof-report.json"
test_arguments=()
if [[ "$promotion_only" == true ]]; then
  report_path="$PWD/aws-durable-proof/promotion-evidence.json"
  random_seed=$(date +%s)
  test_arguments+=(
    '-Dit.test=DeployedAwsDurableProofIT#randomizedCallbackAndBindingRacesConverge+providerHistoryLossRecoversFromTheTpfSemanticCheckpoint'
    '-Dtpf.proof.random-repetitions=10'
    "-Dtpf.proof.random-seed=$random_seed"
  )
fi
if [[ "$history_only" == true ]]; then
  report_path="$PWD/aws-durable-proof/history-loss-evidence.json"
  test_arguments=(
    '-Dit.test=DeployedAwsDurableProofIT#providerHistoryLossRecoversFromTheTpfSemanticCheckpoint'
  )
fi

stop_running_durable_executions() {
  local function_name="${stack_name}-durable"
  local running=""
  local attempt
  for attempt in {1..12}; do
    running=$(aws lambda list-durable-executions-by-function \
      --function-name "$function_name" \
      --region "$region" \
      --query 'DurableExecutions[?Status==`RUNNING`].DurableExecutionArn' \
      --output text)
    if [[ -z "$running" || "$running" == "None" ]]; then
      return 0
    fi
    for execution_arn in $running; do
      aws lambda stop-durable-execution \
        --durable-execution-arn "$execution_arn" \
        --region "$region" >/dev/null
    done
    sleep 5
  done
  echo "timed out stopping active durable executions for $function_name" >&2
  return 1
}

cleanup() {
  local exit_code=$?
  trap - EXIT
  local preserve=false
  local cleanup_failed=false
  if [[ "$run_succeeded" == true && "$keep_success" == true ]]; then
    preserve=true
  elif [[ "$run_succeeded" == false && "$keep_failure" == true ]]; then
    preserve=true
  fi

  if [[ "$preserve" == true ]]; then
    echo "proof resources retained: $stack_name"
  else
    stop_running_durable_executions || cleanup_failed=true
    if aws cloudformation describe-stacks \
        --stack-name "$stack_name" --region "$region" >/dev/null 2>&1; then
      sam delete --stack-name "$stack_name" --region "$region" --no-prompts || cleanup_failed=true
    fi
    if aws s3api head-bucket --bucket "$artifact_bucket" --region "$region" 2>/dev/null; then
      aws s3 rm "s3://${artifact_bucket}" --recursive --region "$region" --only-show-errors \
        || cleanup_failed=true
      aws s3api delete-bucket --bucket "$artifact_bucket" --region "$region" \
        || cleanup_failed=true
    fi
  fi

  if [[ -d "$proof_tmp" ]]; then
    find "$proof_tmp" -type f -delete
    rmdir "$proof_tmp"
  fi
  if [[ "$exit_code" -eq 0 && "$cleanup_failed" == true ]]; then
    echo "proof completed, but resource cleanup failed" >&2
    exit_code=1
  fi
  exit "$exit_code"
}

trap cleanup EXIT

./mvnw \
  -pl :pipelineframework-aws-durable-proof-driver,:pipelineframework-aws-durable-proof-action-host \
  -am package -DskipTests -Dgpg.skip \
  -Dmaven.repo.local="$maven_repo" --no-transfer-progress

if ! aws s3api head-bucket --bucket "$artifact_bucket" --region "$region" 2>/dev/null; then
  if [[ "$region" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "$artifact_bucket" --region "$region" >/dev/null
  else
    aws s3api create-bucket \
      --bucket "$artifact_bucket" \
      --region "$region" \
      --create-bucket-configuration "LocationConstraint=$region" >/dev/null
  fi
fi
aws s3api put-public-access-block \
  --bucket "$artifact_bucket" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true >/dev/null
aws s3 cp "$driver_jar" "s3://${artifact_bucket}/${driver_key}" --region "$region" --only-show-errors
driver_sha=$(openssl dgst -sha256 -binary "$driver_jar" | openssl base64 -A)
action_artifact_digest="sha256:$(openssl dgst -sha256 "$action_host_zip" | awk '{print $NF}')"

sam validate --template-file aws-durable-proof/template.yaml --region "$region"
(
  cd aws-durable-proof
  sam validate --lint --template-file template.yaml --region "$region"
)
sam package \
  --template-file aws-durable-proof/template.yaml \
  --output-template-file "$packaged_template" \
  --s3-bucket "$artifact_bucket" \
  --s3-prefix "${stack_name}/sam" \
  --region "$region"

resume_secret=$(openssl rand -hex 32)
worker_secret=$(openssl rand -hex 32)
sam deploy \
  --template-file "$packaged_template" \
  --stack-name "$stack_name" \
  --region "$region" \
  --capabilities CAPABILITY_IAM \
  --no-confirm-changeset \
  --no-fail-on-empty-changeset \
  --tags issue=976 purpose=aws-durable-proof \
  --parameter-overrides \
    ProofName="$stack_name" \
    ResumeTokenSecret="$resume_secret" \
    WorkerSharedSecret="$worker_secret" \
    ArtifactBucket="$artifact_bucket" \
    DurableArtifactKey="$driver_key" \
    DurableCodeSha256="$driver_sha" \
    ActionArtifactDigest="$action_artifact_digest"

./mvnw -N install -DskipTests -Dgpg.skip \
  -Dmaven.repo.local="$maven_repo" --no-transfer-progress
./mvnw -f aws-durable-proof/pom.xml -N install -DskipTests -Dgpg.skip \
  -Dmaven.repo.local="$maven_repo" --no-transfer-progress
./mvnw -pl :pipelineframework-aws-durable-proof-shared install -DskipTests -Dgpg.skip \
  -Dmaven.repo.local="$maven_repo" --no-transfer-progress
./mvnw -pl :pipelineframework-aws-durable-proof-fault-tests verify \
  -Dmaven.repo.local="$maven_repo" \
  -Dtpf.proof.stack="$stack_name" \
  -Dtpf.proof.region="$region" \
  -Dtpf.proof.report="$report_path" \
  "${test_arguments[@]}" \
  --no-transfer-progress

echo "proof report: $report_path"
run_succeeded=true
