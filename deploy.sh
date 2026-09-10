#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 调和引擎一键云端部署脚本 (deploy.sh)
# 作用：
#   1. 自动启用所需的 GCP API 服务
#   2. 创建 Artifact Registry Docker 仓库
#   3. 通过 Cloud Build 构建并推送 Python 高性能调和引擎镜像
#   4. 部署/更新 Cloud Run Job (mf-reconcile)，配置生产增量滑动窗口参数
#   5. 授权并配置 Cloud Scheduler 每日定时触发作业
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"

# 1. 加载环境变量
if [[ -f ./config.env ]]; then
  source ./config.env
else
  echo "❌ 错误: 未找到 config.env 配置文件，请先配置环境变量！"
  exit 1
fi

JOB_NAME="mf-reconcile"
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "========================================================================="
echo "🚀 启动 GCS Managed Folder 调和引擎自动化部署 (deploy.sh)"
echo "   项目 ID:     ${PROJECT_ID}"
echo "   所在地域:    ${REGION}"
echo "   目标存储桶:  ${BUCKET}"
echo "   执行 SA:     ${OPS_SA}"
echo "========================================================================="

# 2. 启用必须的 GCP API 服务
echo -e "\n==> [1/5] 检查并启用 GCP 基础 API 服务..."
gcloud services enable \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"

# 3. 创建 Artifact Registry 仓库
echo -e "\n==> [2/5] 检查/创建 Artifact Registry 代码库 [${REPO_NAME}]..."
if ! gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="Docker repository for Managed Folder reconcile engine"
  echo "    ✔ 代码库 ${REPO_NAME} 创建完成"
else
  echo "    ✔ 代码库 ${REPO_NAME} 已存在，跳过创建"
fi

# 4. 构建并推送容器镜像
echo -e "\n==> [3/5] 使用 Cloud Build 构建并推送镜像..."
echo "    镜像标签: ${IMAGE}"
gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" .

# 5. 部署 Cloud Run Job
echo -e "\n==> [4/5] 部署/更新 Cloud Run Job [${JOB_NAME}]..."
cat > env_vars.yaml <<EOF
PROJECT_ID: "${PROJECT_ID}"
REGION: "${REGION}"
BUCKET: "${BUCKET}"
DATA_ROOT_PREFIX: "${DATA_ROOT_PREFIX}"
HOT_DAYS: "${HOT_DAYS}"
HOT_SA: "${HOT_SA}"
COLD_SA: "${COLD_SA}"
MAX_WORKERS: "30"
RECONCILE_MODE: "incremental"
SLIDING_LOOKBACK_DAYS: "3"
EXCLUDE_DBS: "tmp.db,kafka_test.db"
EOF

gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --cpu=2 \
  --memory=1Gi \
  --max-retries=0 \
  --task-timeout=10m \
  --env-vars-file="env_vars.yaml"

# 6. 配置 Cloud Scheduler 每日定时作业
echo -e "\n==> [5/5] 配置 Cloud Scheduler 每日定时作业 [${JOB_NAME}-daily]..."

# 授予 Reconciler 运行自身 Cloud Run Job 的权限
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" \
  --role="roles/run.invoker" &>/dev/null || true

SCHEDULER_JOB="${JOB_NAME}-daily"
SCHEDULER_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "${SCHEDULER_JOB}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    更新已存在的定时作业..."
  gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
else
  echo "    创建全新定时作业..."
  gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
fi

echo "========================================================================="
echo "🎉 全流程部署成功！"
echo "   Cloud Run Job:      ${JOB_NAME} (${REGION})"
echo "   Cloud Scheduler:    ${SCHEDULER_JOB} (每日 UTC 00:30 执行)"
echo "   运行模式:           增量滑动窗口 (RECONCILE_MODE=incremental)"
echo "-------------------------------------------------------------------------"
echo "💡 可通过以下命令立即手动测试触发一次云端执行："
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID} --wait"
echo "========================================================================="
