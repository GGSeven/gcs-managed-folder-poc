#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 生产一键自愈与云端部署脚本 (deploy.sh)
# 特性：
#   1. 预检防遗漏（Pre-flight IAM Auto-Remediation）：
#      - 自动检查并修复存储桶 UBLA 状态
#      - 自动检查并补齐 4 个专用 Service Accounts
#      - 自动检查并补齐 Reconciler 自定义最小权限角色
#      - 自动检查并创建顶级 Managed Folder 及基准授权策略
#   2. 上下文隔离极速构建（<30KB），绝不上传 Cloud Shell 冗余文件
#   3. 一键部署 Cloud Run Job 与 Cloud Scheduler 定时触发器
#   4. 部署后自动触发单次校验执行，确保全链路 100% 成功闭环
# ==============================================================================
set -euo pipefail

# ------------------------------------------------------------------------------
# 1. 加载环境变量（兼容直接 export 或通过 config.env 加载）
# ------------------------------------------------------------------------------
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
ROLE_ID="mfReconciler"

HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
WRITER_SA="${WRITER_SA:-iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com}"
OPS_SA="${OPS_SA:-mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com}"

JOB_NAME="mf-reconcile"
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "========================================================================="
echo "🚀 启动 GCS Managed Folder 自动化部署与前置自愈引擎"
echo "   项目 ID:     ${PROJECT_ID}"
echo "   所在地域:    ${REGION}"
echo "   目标存储桶:  ${BUCKET}"
echo "   根目录前缀:  ${DATA_ROOT_PREFIX}/"
echo "   隔离天数:    ${HOT_DAYS} 天"
echo "========================================================================="

# ------------------------------------------------------------------------------
# 2. 前置 IAM 与基础架构自愈预检（Pre-flight Auto-Remediation）
# ------------------------------------------------------------------------------
echo -e "\n==> [1/6] 检查必须的 GCP API 服务启用状态..."
gcloud services enable \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"

echo -e "\n==> [2/6] 检查并自愈 IAM 基础环境配置（防遗漏校验）..."

# 2.1 检查存储桶 UBLA
UBLA_ENABLED=$(gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access.enabled)" 2>/dev/null || echo "False")
if [[ "${UBLA_ENABLED}" != "True" ]]; then
  echo "    ⚠️ 检测到存储桶未开启 UBLA，正在自动开启..."
  gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
  echo "    ✔ UBLA 开启成功"
else
  echo "    ✔ [PASS] 存储桶已开启 UBLA"
fi

# 2.2 检查 4 个专用 Service Account
declare -A SAS=(
  ["iceberg-writer"]="Iceberg Pipeline Writer (Spark/Flink)"
  ["iceberg-hot-reader"]="Hot Data Reader (Hue/BI Analyst)"
  ["iceberg-cold-reader"]="Cold Data Reader (Audit/Archive Query)"
  ["mf-reconciler"]="Managed Folder Daily Reconciler (Cloud Run)"
)

for sa in "${!SAS[@]}"; do
  sa_email="${sa}@${PROJECT_ID}.iam.gserviceaccount.com"
  if ! gcloud iam service-accounts describe "${sa_email}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "    ⚠️ 检测到缺失服务账号 [${sa_email}]，正在自动创建..."
    gcloud iam service-accounts create "${sa}" \
      --project="${PROJECT_ID}" \
      --display-name="${SAS[$sa]}"
    echo "    ✔ 创建成功: ${sa_email}"
  else
    echo "    ✔ [PASS] 服务账号已存在: ${sa_email}"
  fi
done

# 2.3 检查 Reconciler 自定义角色
if ! gcloud iam roles describe "${ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    ⚠️ 检测到缺失自定义角色 [${ROLE_ID}]，正在自动创建..."
  gcloud iam roles create "${ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
  echo "    ✔ 自定义角色 ${ROLE_ID} 创建成功"
else
  echo "    ✔ [PASS] 自定义角色已存在: ${ROLE_ID}"
fi

# 2.4 检查顶级 Managed Folder 及其策略
TOP_MF="${BUCKET}/${DATA_ROOT_PREFIX}/"
if ! gcloud storage managed-folders describe "${TOP_MF}" &>/dev/null; then
  echo "    ⚠️ 正在创建顶级 Managed Folder: ${TOP_MF} ..."
  gcloud storage managed-folders create "${TOP_MF}" &>/dev/null || true
fi

# 检查顶级 Managed Folder 是否挂载了 Writer 和 Reconciler 的策略
CURRENT_POLICY=$(gcloud storage managed-folders get-iam-policy "${TOP_MF}" --format="json" 2>/dev/null || echo "{}")
if [[ "${CURRENT_POLICY}" != *"serviceAccount:${WRITER_SA}"* || "${CURRENT_POLICY}" != *"serviceAccount:${OPS_SA}"* ]]; then
  echo "    ⚠️ 正在自动绑定顶级 Managed Folder 基础权限 (Writer + Reconciler)..."
  POLICY_FILE=$(mktemp)
  cat > "${POLICY_FILE}" <<EOF
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
  gcloud storage managed-folders set-iam-policy "${TOP_MF}" "${POLICY_FILE}"
  rm -f "${POLICY_FILE}"
  echo "    ✔ 顶级 Managed Folder 基准策略挂载完成"
else
  echo "    ✔ [PASS] 顶级 Managed Folder 基础策略已健全"
fi

# ------------------------------------------------------------------------------
# 3. 创建 Artifact Registry 仓库
# ------------------------------------------------------------------------------
echo -e "\n==> [3/6] 检查/创建 Artifact Registry 代码库 [${REPO_NAME}]..."
if ! gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="Docker repository for Managed Folder reconcile engine"
  echo "    ✔ 代码库 ${REPO_NAME} 创建完成"
else
  echo "    ✔ [PASS] 代码库已存在: ${REPO_NAME}"
fi

# ------------------------------------------------------------------------------
# 4. 构建并推送容器镜像（极速隔离模式）
# ------------------------------------------------------------------------------
echo -e "\n==> [4/6] 隔离上下文并提交 Cloud Build 构建镜像..."
echo "    镜像目标: ${IMAGE}"

BUILD_DIR=$(mktemp -d)
trap 'rm -rf "${BUILD_DIR}"' EXIT

# 仅打包真正需要的 Dockerfile 与 Python 脚本
cp Dockerfile reconcile_fast.py "${BUILD_DIR}/"
cat << 'EOF' > "${BUILD_DIR}/.gcloudignore"
.git
.gitignore
*.env
*.sh
*.md
EOF

echo "    ✔ 构建上下文已隔离 (大小: <30KB)，正在极速上传构建..."
gcloud builds submit "${BUILD_DIR}" --tag "${IMAGE}" --project="${PROJECT_ID}"

# ------------------------------------------------------------------------------
# 5. 部署 Cloud Run Job
# ------------------------------------------------------------------------------
echo -e "\n==> [5/6] 部署/更新 Cloud Run Job [${JOB_NAME}]..."
ENV_FILE="${BUILD_DIR}/env_vars.yaml"
cat > "${ENV_FILE}" <<EOF
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
  --env-vars-file="${ENV_FILE}"

# ------------------------------------------------------------------------------
# 6. 配置 Cloud Scheduler 定时作业
# ------------------------------------------------------------------------------
echo -e "\n==> [6/6] 配置 Cloud Scheduler 每日定时触发 [${JOB_NAME}-daily]..."

# 授予 Reconciler 运行自身 Cloud Run Job 的权限
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" \
  --role="roles/run.invoker" &>/dev/null || true

SCHEDULER_JOB="${JOB_NAME}-daily"
SCHEDULER_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "${SCHEDULER_JOB}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    更新已存在的定时作业 (每日 UTC 00:30)..."
  gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
else
  echo "    创建全新定时作业 (每日 UTC 00:30)..."
  gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
fi

echo -e "\n-------------------------------------------------------------------------"
echo "🔄 正在自动执行一次云端测试以验证全链路 (耗时约 2~3 秒)..."
gcloud run jobs execute "${JOB_NAME}" --region="${REGION}" --project="${PROJECT_ID}" --wait

echo "========================================================================="
echo "🎉 全流程部署并验证成功！"
echo "   1. 前置 IAM 权限:  全部检查并通过（防遗漏自愈完毕）"
echo "   2. Cloud Run Job:  ${JOB_NAME} (${REGION})"
echo "   3. 调度触发器:     ${SCHEDULER_JOB} (每日 UTC 00:30 执行)"
echo "   4. 增量调和自检:   首次运行成功 ✔"
echo "========================================================================="
