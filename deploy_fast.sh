#!/usr/bin/env bash
# ==============================================================================
# deploy_fast.sh - 生产级自动化部署脚本
# 包含：构建推送镜像 + 部署 Cloud Run Job + 配置 Cloud Scheduler 每日定时触发
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"
source ./config.env

JOB_NAME="mf-reconcile"
SCHEDULER_NAME="mf-reconcile-daily"
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "========================================================================="
echo "🚀 开始执行生产环境自动化部署 (Deployment)"
echo "   项目: ${PROJECT_ID} | 区域: ${REGION}"
echo "   镜像: ${IMAGE}"
echo "   Cloud Run Job: ${JOB_NAME}"
echo "========================================================================="

echo "==> 1. 确保 Artifact Registry Docker 仓库存在"
if ! gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}"
  echo "    ✔ Docker 仓库 ${REPO_NAME} 创建完成"
else
  echo "    ✔ Docker 仓库 ${REPO_NAME} 已存在"
fi

echo "==> 2. 使用 Cloud Build 构建并推送高性能调和引擎镜像"
gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" .
echo "    ✔ 容器镜像构建并推送成功"

echo "==> 3. 部署 / 更新 Cloud Run Job (${JOB_NAME})"
# 采用 2 vCPU / 1 GiB / 30 并发，毫秒级内网调用 GCS 控制面
gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --cpu=2 \
  --memory=1Gi \
  --max-retries=0 \
  --task-timeout=10m \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},REGION=${REGION},BUCKET=${BUCKET},DATA_ROOT_PREFIX=${DATA_ROOT_PREFIX},HOT_DAYS=${HOT_DAYS},HOT_SA=${HOT_SA},COLD_SA=${COLD_SA},MAX_WORKERS=30,RECONCILE_MODE=incremental,SLIDING_LOOKBACK_DAYS=3,EXCLUDE_DBS=tmp.db\,kafka_test.db"
echo "    ✔ Cloud Run Job 部署成功"

echo "==> 4. 配置 Cloud Scheduler 每日凌晨定时调度作业 (${SCHEDULER_NAME})"
# 授予 Reconciler SA 触发自身 Cloud Run Job 的权限
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" \
  --role="roles/run.invoker" &>/dev/null || true

# 每天 UTC 00:05 (对应北京时间 08:05) 自动触发增量滑动调和
RUN_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if ! gcloud scheduler jobs describe "${SCHEDULER_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud scheduler jobs create http "${SCHEDULER_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="5 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${RUN_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
  echo "    ✔ Cloud Scheduler 定时作业创建成功"
else
  gcloud scheduler jobs update http "${SCHEDULER_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="5 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${RUN_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
  echo "    ✔ Cloud Scheduler 定时作业更新成功"
fi

echo "========================================================================="
echo "🎉 生产部署全部完成！"
echo "   手动触发测试命令: gcloud run jobs execute ${JOB_NAME} --region=${REGION} --wait"
echo "   查看实时运行日志: gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}' --limit=30"
echo "========================================================================="
