#!/usr/bin/env bash
# ==============================================================================
# Dockerfile 配套的高性能调和镜像打包部署
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"
source ./config.env

JOB_NAME="mf-reconcile"
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "==> 1. 构建并推送多线程 Python 镜像到 Artifact Registry..."
cat << 'EOF' > Dockerfile
FROM python:3.11-slim
RUN pip install --no-cache-dir requests
WORKDIR /app
COPY config.env reconcile_fast.py ./
RUN chmod +x reconcile_fast.py
ENTRYPOINT ["python3", "/app/reconcile_fast.py"]
EOF

gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" .

echo "==> 2. 更新 Cloud Run Job (${JOB_NAME})..."
gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --max-retries=3 \
  --task-timeout=1h \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},REGION=${REGION},BUCKET=${BUCKET},DATA_ROOT_PREFIX=${DATA_ROOT_PREFIX},HOT_DAYS=${HOT_DAYS},HOT_SA=${HOT_SA},COLD_SA=${COLD_SA},MAX_WORKERS=20,EXCLUDE_DBS=tmp.db,kafka_test.db"

echo "==> 部署完成！可使用以下命令手动触发一次运行："
echo "    gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
