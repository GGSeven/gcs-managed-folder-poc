#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 生产一键自愈部署脚本 (deploy.sh)
# 特性：
#   1. 全自动化与防御性核验：自动核验 UBLA、SA 账号、自定义角色及顶级托管文件夹，
#      已配置的直接跳过，遗漏项自动补充自愈，杜绝人工配置疏漏。
#   2. 直接支持 Shell export 环境变量（无需 .env 文件）。
#   3. 构建上下文自动隔离（<30KB），绝不误传家目录大文件。
#   4. 一键打通 API -> 镜像仓库 -> 镜像构建 -> Cloud Run Job -> Cloud Scheduler。
# ==============================================================================
set -euo pipefail

# 1. 加载环境变量（兼容直接 export 或通过 config.env 加载）
if [[ -f ./config.env ]]; then
  source ./config.env
fi

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
if [[ -z "${PROJECT_ID}" ]]; then
  echo "❌ 错误: 未检测到 PROJECT_ID，请先执行: export PROJECT_ID=您的项目ID"
  exit 1
fi

REGION="${REGION:-asia-east1}"
BUCKET="${BUCKET:-gs://${PROJECT_ID}-mf-poc}"
DATA_ROOT_PREFIX="${DATA_ROOT_PREFIX:-datasets}"
HOT_DAYS="${HOT_DAYS:-60}"

HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
WRITER_SA="${WRITER_SA:-iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com}"
OPS_SA="${OPS_SA:-mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com}"
ROLE_ID="mfReconciler"

JOB_NAME="mf-reconcile"
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "========================================================================="
echo "🚀 启动 GCS Managed Folder 生产一键自愈部署 (deploy.sh)"
echo "   项目 ID:     ${PROJECT_ID}"
echo "   所在地域:    ${REGION}"
echo "   目标存储桶:  ${BUCKET}"
echo "   根目录前缀:  ${DATA_ROOT_PREFIX}"
echo "   执行 SA:     ${OPS_SA}"
echo "========================================================================="

# 2. 前置防御性核验与自动自愈 (Self-Healing IAM & Infrastructure Check)
echo -e "\n==> [1/6] 防御性核验前置 IAM 与基础存储配置（已配自动跳过，遗漏项自动补充）..."

# A. 检查存储桶 UBLA
UBLA_STATUS=$(gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access.enabled)" 2>/dev/null || echo "False")
if [[ "${UBLA_STATUS}" != "True" ]]; then
  echo "    ⚠️ 存储桶未开启 UBLA，正在自动补全开启..."
  gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
  echo "    ✔ UBLA 开启成功"
else
  echo "    ✔ [核验通过] 存储桶已开启 UBLA"
fi

# B. 检查 4 个专用 SA 账号
declare -A SAS=(
  ["iceberg-writer"]="Iceberg Pipeline Writer (Spark/Flink)"
  ["iceberg-hot-reader"]="Hot Data Reader (Hue/BI Analyst)"
  ["iceberg-cold-reader"]="Cold Data Reader (Audit/Archive Query)"
  ["mf-reconciler"]="Managed Folder Daily Reconciler (Cloud Run)"
)
for sa in "${!SAS[@]}"; do
  sa_email="${sa}@${PROJECT_ID}.iam.gserviceaccount.com"
  if ! gcloud iam service-accounts describe "${sa_email}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "    ⚠️ 缺少 SA 账号 [${sa_email}]，正在自动创建..."
    gcloud iam service-accounts create "${sa}" \
      --project="${PROJECT_ID}" \
      --display-name="${SAS[$sa]}"
    echo "    ✔ SA [${sa_email}] 创建成功"
  else
    echo "    ✔ [核验通过] SA [${sa_email}] 已就绪"
  fi
done

# C. 检查 Reconciler 最小权限自定义角色
if ! gcloud iam roles describe "${ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    ⚠️ 缺少自定义角色 [${ROLE_ID}]，正在自动创建..."
  gcloud iam roles create "${ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
  echo "    ✔ 自定义角色 ${ROLE_ID} 创建成功"
else
  echo "    ✔ [核验通过] 自定义角色 [${ROLE_ID}] 已就绪"
fi

# D. 检查顶级 Managed Folder (datasets/) 及其权限
TOP_MF="${BUCKET}/${DATA_ROOT_PREFIX}/"
gcloud storage managed-folders create "${TOP_MF}" &>/dev/null || true

CURRENT_POLICY=$(gcloud storage managed-folders get-iam-policy "${TOP_MF}" --format=json 2>/dev/null || echo "{}")
HAS_WRITER=$(echo "${CURRENT_POLICY}" | grep -c "${WRITER_SA}" || true)
HAS_OPS=$(echo "${CURRENT_POLICY}" | grep -c "${OPS_SA}" || true)

if (( HAS_WRITER == 0 || HAS_OPS == 0 )); then
  echo "    ⚠️ 顶级 Managed Folder [${TOP_MF}] 缺少必要权限绑定，正在自动挂载策略..."
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
  echo "    ✔ 顶级 Managed Folder 策略绑定完成"
else
  echo "    ✔ [核验通过] 顶级 Managed Folder [${TOP_MF}] 权限策略已就绪"
fi

# 3. 启用必须的 GCP API 服务
echo -e "\n==> [2/6] 检查并启用 GCP 基础 API 服务..."
gcloud services enable \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"

# 4. 创建 Artifact Registry 仓库
echo -e "\n==> [3/6] 检查/创建 Artifact Registry 代码库 [${REPO_NAME}]..."
if ! gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="Docker repository for Managed Folder reconcile engine"
  echo "    ✔ 代码库 ${REPO_NAME} 创建完成"
else
  echo "    ✔ [核验通过] 代码库 ${REPO_NAME} 已就绪"
fi

# 5. 构建并推送容器镜像（极速隔离模式：仅打包 Dockerfile 和 Python 脚本）
echo -e "\n==> [4/6] 使用 Cloud Build 构建并推送轻量镜像..."
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

# 6. 部署 Cloud Run Job
echo -e "\n==> [5/6] 部署/更新 Cloud Run Job [${JOB_NAME}]..."
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

# 7. 配置 Cloud Scheduler 每日定时作业
echo -e "\n==> [6/6] 配置 Cloud Scheduler 每日定时作业 [${JOB_NAME}-daily]..."

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
echo "💡 建议操作："
echo "   1. 验证定时调度：gcloud scheduler jobs run ${SCHEDULER_JOB} --location=${REGION} --project=${PROJECT_ID}"
echo "   2. 存量首次全量调和（如需立即处理存量历史数据）："
echo "      gcloud run jobs execute ${JOB_NAME} --region=${REGION} --update-env-vars=RECONCILE_MODE=full --wait"
echo "========================================================================="
