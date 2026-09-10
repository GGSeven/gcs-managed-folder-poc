#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 生产一键部署与自检脚本 (deploy.sh)
# 特性：
#   1. 完全无 .env 文件依赖，优先读取当前 Shell export 的环境变量
#   2. 前置自动幂等自检（UBLA、4 个 SA、自定义角色、顶级 Managed Folder IAM）
#      已存在的资源自动打印 [✔ 已配置，跳过]，防止任何遗漏同时避免重复报错
#   3. 构建上下文严格隔离 (<30KB)，避免打包 Cloud Shell 家目录 1GB+ 文件
#   4. 部署完成后自动触发一次云端试运行自检并输出状态日志
# ==============================================================================
set -euo pipefail

# ==============================================================================
# 0. 环境变量解析（直接读取当前 shell export，或自动推导默认值）
# ==============================================================================
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
if [[ -z "${PROJECT_ID}" ]]; then
  echo "❌ 错误: 未检测到 PROJECT_ID，请先执行: export PROJECT_ID=您的项目ID"
  exit 1
fi

REGION="${REGION:-asia-east1}"
BUCKET="${BUCKET:-gs://${PROJECT_ID}-mf-poc}"
DATA_ROOT_PREFIX="${DATA_ROOT_PREFIX:-datasets}"
HOT_DAYS="${HOT_DAYS:-60}"
FOLDER_DATE_FORMAT="${FOLDER_DATE_FORMAT:-dt=%Y-%m-%d}"

HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
WRITER_SA="${WRITER_SA:-iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com}"
OPS_SA="${OPS_SA:-mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com}"

ROLE_ID="mfReconciler"
JOB_NAME="mf-reconcile"
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "========================================================================="
echo "🚀 启动 GCS Managed Folder 生产全自动部署与环境自检"
echo "   项目 ID:     ${PROJECT_ID}"
echo "   所在地域:    ${REGION}"
echo "   目标存储桶:  ${BUCKET}"
echo "   数据前缀:    ${DATA_ROOT_PREFIX}/"
echo "   执行 SA:     ${OPS_SA}"
echo "========================================================================="

# ==============================================================================
# 1. 检查并启用基础 GCP API
# ==============================================================================
echo -e "\n==> [1/7] 检查并启用 GCP 基础 API 服务..."
gcloud services enable \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"
echo "    ✔ API 服务就绪"

# ==============================================================================
# 2. 检查存储桶 UBLA 状态（必须为 True）
# ==============================================================================
echo -e "\n==> [2/7] 检查存储桶 UBLA (Uniform Bucket-Level Access) 状态..."
UBLA_VAL=$(gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access)" 2>/dev/null || echo "False")

if [[ "${UBLA_VAL}" != "True" ]]; then
  echo "    ⚠️ 检测到存储桶未开启 UBLA，正在自动开启..."
  gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
  echo "    ✔ 存储桶 UBLA 开启成功"
else
  echo "    ✔ 存储桶 UBLA 已开启 (True)，跳过"
fi

# ==============================================================================
# 3. 幂等检查并补齐 4 个专用 Service Accounts
# ==============================================================================
echo -e "\n==> [3/7] 幂等自检 4 个专用 Service Account..."
declare -A SAS=(
  ["iceberg-writer"]="Iceberg Pipeline Writer (Spark/Flink)"
  ["iceberg-hot-reader"]="Hot Data Reader (Hue/BI Analyst)"
  ["iceberg-cold-reader"]="Cold Data Reader (Audit/Archive Query)"
  ["mf-reconciler"]="Managed Folder Daily Reconciler (Cloud Run)"
)

for sa in "${!SAS[@]}"; do
  sa_email="${sa}@${PROJECT_ID}.iam.gserviceaccount.com"
  if ! gcloud iam service-accounts describe "${sa_email}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud iam service-accounts create "${sa}" \
      --project="${PROJECT_ID}" \
      --display-name="${SAS[$sa]}"
    echo "    ✔ 补齐创建成功: ${sa_email}"
  else
    echo "    ✔ 已存在，跳过: ${sa_email}"
  fi
done

# ==============================================================================
# 4. 幂等检查自定义角色与顶级 Managed Folder 授权
# ==============================================================================
echo -e "\n==> [4/7] 幂等自检 Reconciler 自定义角色与顶级托管目录 IAM..."
if ! gcloud iam roles describe "${ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud iam roles create "${ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
  echo "    ✔ 自定义角色 ${ROLE_ID} 补齐创建成功"
else
  echo "    ✔ 自定义角色 ${ROLE_ID} 已存在，跳过"
fi

# 确保存储桶顶级 managed folder 存在
TOP_MF="${BUCKET}/${DATA_ROOT_PREFIX}/"
gcloud storage managed-folders create "${TOP_MF}" &>/dev/null || true

# 检查顶级 managed folder 是否已挂载 IAM 策略
EXISTING_POLICY=$(gcloud storage managed-folders get-iam-policy "${TOP_MF}" --format="json" 2>/dev/null || echo "{}")
if [[ "${EXISTING_POLICY}" != *"serviceAccount:${WRITER_SA}"* || "${EXISTING_POLICY}" != *"serviceAccount:${OPS_SA}"* ]]; then
  echo "    正在补齐顶级托管文件夹 [${TOP_MF}] IAM 授权策略..."
  POLICY_TMP=$(mktemp)
  cat > "${POLICY_TMP}" <<EOF
{
  "bindings": [
    {
      "role": "roles/storage.objectUser",
      "members": ["serviceAccount:${WRITER_SA}"]
    },
    {
      "role": "projects/${PROJECT_ID}/roles/${ROLE_ID}",
      "members": ["serviceAccount:${OPS_SA}"]
    }
  ]
}
EOF
  gcloud storage managed-folders set-iam-policy "${TOP_MF}" "${POLICY_TMP}"
  rm -f "${POLICY_TMP}"
  echo "    ✔ 顶级 Managed Folder 策略挂载完成"
else
  echo "    ✔ 顶级 Managed Folder IAM 策略已就绪，跳过"
fi

# ==============================================================================
# 5. 检查并创建 Artifact Registry，隔离构建上下文打包镜像
# ==============================================================================
echo -e "\n==> [5/7] 检查 Artifact Registry 并构建轻量镜像..."
if ! gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="Docker repository for Managed Folder reconcile engine"
  echo "    ✔ 代码库 ${REPO_NAME} 创建完成"
else
  echo "    ✔ 代码库 ${REPO_NAME} 已存在，跳过"
fi

# 确保 Dockerfile 存在
if [[ ! -f Dockerfile ]]; then
  cat << 'EOF' > Dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY reconcile_fast.py /app/
ENTRYPOINT ["python3", "reconcile_fast.py"]
EOF
fi

# 隔离构建目录：仅打包 Dockerfile 和 reconcile_fast.py (<30KB)，杜绝打包 Cloud Shell 家目录 1GB+
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "${BUILD_DIR}"' EXIT

cp Dockerfile reconcile_fast.py "${BUILD_DIR}/"
cat << 'EOF_IGNORE' > "${BUILD_DIR}/.gcloudignore"
.git
.gitignore
*.env
*.sh
*.md
EOF_IGNORE

echo "    ✔ 构建上下文已隔离，准备上传核心脚本 (大小: <30KB)..."
gcloud builds submit "${BUILD_DIR}" --tag "${IMAGE}" --project="${PROJECT_ID}"

# ==============================================================================
# 6. 部署/更新 Cloud Run Job 与 Cloud Scheduler
# ==============================================================================
echo -e "\n==> [6/7] 部署/平滑更新 Cloud Run Job [${JOB_NAME}] 与 定时器..."
ENV_FILE="${BUILD_DIR}/env_vars.yaml"
cat > "${ENV_FILE}" <<EOF_YAML
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
EOF_YAML

# 平滑部署/更新 Cloud Run Job（已有 job 时将自动覆盖为新镜像与新环境变量）
gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --cpu=2 \
  --memory=1Gi \
  --max-retries=0 \
  --task-timeout=10m \
  --env-vars-file="${ENV_FILE}"

# 赋予 Reconciler SA 调用自身作业的权限
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" \
  --role="roles/run.invoker" &>/dev/null || true

# 配置 Cloud Scheduler 每日触发
SCHEDULER_JOB="${JOB_NAME}-daily"
SCHEDULER_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "${SCHEDULER_JOB}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    更新已存在的 Cloud Scheduler 定时作业..."
  gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
else
  echo "    创建全新 Cloud Scheduler 定时作业..."
  gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
fi

# ==============================================================================
# 7. 自动触发一次云端试运行自检（无须人工等待测试）
# ==============================================================================
echo -e "\n==> [7/7] 正在自动触发一次云端试运行自检..."
if gcloud run jobs execute "${JOB_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --wait; then
  echo "    ✔ 云端作业自检执行成功！"
else
  echo "    ⚠️ 云端执行异常，请查看日志排查！"
  exit 1
fi

echo "========================================================================="
echo "🎉 全流程部署并自检完成！"
echo "   Cloud Run Job:      ${JOB_NAME} (${REGION})"
echo "   Cloud Scheduler:    ${SCHEDULER_JOB} (每日 UTC 00:30 自动执行)"
echo "   运行模式:           增量滑动窗口 (RECONCILE_MODE=incremental)"
echo "   最新执行状态:       成功 (Succeeded)"
echo "========================================================================="
