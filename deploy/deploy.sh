#!/usr/bin/env bash
# ==============================================================================
# 生产化部署脚本 (deploy/deploy.sh) - 高性能 Python 多线程引擎
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

source ./config.env

JOB_NAME="mf-reconcile"
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "==> 1. 检查运维 SA 与顶级 Managed Folder 授权"
gcloud iam service-accounts describe "${OPS_SA}" --project="${PROJECT_ID}" >/dev/null
gcloud storage managed-folders get-iam-policy "${BUCKET}/${DATA_ROOT_PREFIX}/" --format=json \
  | grep -q "${OPS_SA}" || { 
    echo "错误：${OPS_SA} 未绑定在 ${BUCKET}/${DATA_ROOT_PREFIX}/"; 
    exit 1; 
  }

echo "==> 2. 启用所需 API"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com cloudscheduler.googleapis.com \
  --project="${PROJECT_ID}"

echo "==> 3. 检查/创建 Artifact Registry 镜像仓库"
gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" \
  --project="${PROJECT_ID}" &>/dev/null || \
  gcloud artifacts repositories create "${REPO_NAME}" --location="${REGION}" \
    --project="${PROJECT_ID}" --repository-format=docker

echo "==> 4. 构建并推送 Python 高性能调和镜像"
build_ctx=$(mktemp -d)
cp deploy/Dockerfile config.env reconcile_fast.py "${build_ctx}/"
gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" "${build_ctx}"
rm -rf "${build_ctx}"

echo "==> 5. 部署 Cloud Run Job (${JOB_NAME})"
gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --max-retries=3 \
  --task-timeout=1h \
  --memory=1Gi \
  --cpu=2 \
  --set-env-vars="^#^PROJECT_ID=${PROJECT_ID}#REGION=${REGION}#BUCKET=${BUCKET}#DATA_ROOT_PREFIX=${DATA_ROOT_PREFIX}#FOLDER_DATE_FORMAT=${FOLDER_DATE_FORMAT}#HOT_DAYS=${HOT_DAYS}#HOT_SA=${HOT_SA}#COLD_SA=${COLD_SA}#MAX_WORKERS=30#EXCLUDE_DBS=tmp.db,kafka_test.db,udf.db,bench,test_batch_1,test_batch_2,test_batch_3,test_p"

echo "==> 6. 配置 Cloud Scheduler（每日 UTC 00:30 自动执行）"
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" --role="roles/run.invoker" >/dev/null

gcloud scheduler jobs describe "${JOB_NAME}-daily" --location="${REGION}" \
  --project="${PROJECT_ID}" &>/dev/null || \
gcloud scheduler jobs create http "${JOB_NAME}-daily" \
  --location="${REGION}" --project="${PROJECT_ID}" \
  --schedule="30 0 * * *" --time-zone="Etc/UTC" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
  --http-method=POST \
  --oauth-service-account-email="${OPS_SA}"

echo "========================================================================="
echo "==> 高性能多线程调和引擎部署成功！✔"
echo "    - 镜像已自动打包 reconcile_fast.py (30 线程并发)"
echo "    - 调度计划: 每日 UTC 00:30 自动执行"
echo "    - 手动立即触发测试命令:"
echo "      gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID} --wait"
echo "========================================================================="
