#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 热/冷数据隔离方案 - 生产环境一键全自动化部署脚本 (deploy.sh)
# ==============================================================================
# 功能：从零到一自动完成所有 GCP 基础服务、权限配置、容器构建、Cloud Run Job 部署与定时调度
# 适用：直接交付给客户或百道架构师在客户环境一键执行
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"

echo "========================================================================="
echo "  🚀 启动 GCS Managed Folder 热/冷数据隔离方案 一键自动化部署"
echo "========================================================================="

# ------------------------------------------------------------------------------
# 阶段 0: 前置检查与环境依赖校验
# ------------------------------------------------------------------------------
echo "==> [0/8] 检查本地环境依赖与同级目录文件..."

if ! command -v gcloud &>/dev/null; then
  echo "❌ 错误: 本机未安装 Google Cloud SDK (gcloud CLI)，请先安装并完成 gcloud auth login。"
  exit 1
fi

REQUIRED_FILES=("config.env" "Dockerfile" "reconcile_fast.py")
for f in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "❌ 错误: 缺少关键同级依赖文件: $f"
    echo "   请确保 deploy.sh、config.env、Dockerfile、reconcile_fast.py 存放在同一目录下！"
    exit 1
  fi
done

source ./config.env

echo "    ✔ 加载配置文件: config.env"
echo "    - 目标项目 (PROJECT_ID) : ${PROJECT_ID}"
echo "    - 目标区域 (REGION)     : ${REGION}"
echo "    - 目标存储桶 (BUCKET)   : ${BUCKET}"
echo "    - 根数据路径 (PREFIX)   : ${DATA_ROOT_PREFIX}"
echo "    - 隔离阈值天数 (HOT_DAYS): ${HOT_DAYS} 天"

# 检查当前 gcloud 认证身份
CURRENT_USER=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null || true)
if [[ -z "${CURRENT_USER}" ]]; then
  echo "❌ 错误: 未检测到有效的 gcloud 登录凭据，请先执行: gcloud auth login"
  exit 1
fi
echo "    ✔ 当前执行账号: ${CURRENT_USER}"

# 检查目标存储桶是否存在并确认开启 UBLA
echo "    --> 检查存储桶与 UBLA 配置..."
if ! gcloud storage buckets describe "${BUCKET}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    ⚠️ 存储桶 ${BUCKET} 不存在，正在自动创建并启用 UBLA..."
  gcloud storage buckets create "${BUCKET}" \
    --project="${PROJECT_ID}" --location="${REGION}" \
    --uniform-bucket-level-access
else
  # 确保存储桶开启 UBLA（Managed Folder 的硬性前提）
  UBLA_STATUS=$(gcloud storage buckets describe "${BUCKET}" --format="value(uniformBucketLevelAccess.enabled)" 2>/dev/null || echo "False")
  if [[ "${UBLA_STATUS}" != "True" ]]; then
    echo "    ⚠️ 存储桶未开启统一存储桶级访问权限 (UBLA)，正在启用..."
    gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
  fi
fi
echo "    ✔ 存储桶 UBLA 校验通过"

# ------------------------------------------------------------------------------
# 阶段 1: 启用必要的 GCP API 服务
# ------------------------------------------------------------------------------
echo "==> [1/8] 启用所需 GCP 服务 API..."
gcloud services enable \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"
echo "    ✔ API 服务已就绪"

# ------------------------------------------------------------------------------
# 阶段 2: 创建自定义最小权限角色 (mfReconciler)
# ------------------------------------------------------------------------------
echo "==> [2/8] 检查/创建调和引擎最小权限自定义角色 (${OPS_ROLE_ID})..."
if ! gcloud iam roles describe "${OPS_ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud iam roles create "${OPS_ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data contents" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get" >/dev/null
  echo "    ✔ 自定义角色 ${OPS_ROLE_ID} 创建成功 (剥离 storage.objects.get，零数据访问)"
else
  echo "    ✔ 自定义角色 ${OPS_ROLE_ID} 已存在，跳过"
fi

# ------------------------------------------------------------------------------
# 阶段 3: 创建四大专用服务账号 (Service Accounts)
# ------------------------------------------------------------------------------
echo "==> [3/8] 检查/创建四大专用服务账号..."
SA_ARRAY=("${HOT_SA}" "${COLD_SA}" "${WRITER_SA}" "${OPS_SA}")
for sa_email in "${SA_ARRAY[@]}"; do
  sa_name="${sa_email%%@*}"
  if ! gcloud iam service-accounts describe "${sa_email}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud iam service-accounts create "${sa_name}" --project="${PROJECT_ID}" \
      --display-name="SA for ${sa_name}" >/dev/null
    echo "    ✔ 创建服务账号: ${sa_email}"
  else
    echo "    ✔ 服务账号已存在: ${sa_email}"
  fi
done

# ------------------------------------------------------------------------------
# 阶段 4: 初始化数据根目录 Managed Folder 与基线权限
# ------------------------------------------------------------------------------
echo "==> [4/8] 初始化根目录 Managed Folder 与基础权限..."
ROOT_MF="${BUCKET}/${DATA_ROOT_PREFIX}/"
# 创建根 Managed Folder（若已存在忽略错误）
gcloud storage managed-folders create "${ROOT_MF}" &>/dev/null || true

# 绑定写入方与调和方的基础权限
ROOT_POLICY_TMP=$(mktemp)
cat > "${ROOT_POLICY_TMP}" <<EOF
{
  "bindings": [
    {
      "role": "${WRITER_ROLE}",
      "members": ["serviceAccount:${WRITER_SA}"]
    },
    {
      "role": "projects/${PROJECT_ID}/roles/${OPS_ROLE_ID}",
      "members": ["serviceAccount:${OPS_SA}"]
    }
  ]
}
EOF
gcloud storage managed-folders set-iam-policy "${ROOT_MF}" "${ROOT_POLICY_TMP}" >/dev/null
rm -f "${ROOT_POLICY_TMP}"
echo "    ✔ ${ROOT_MF} -> 写入方: ${WRITER_SA} (${WRITER_ROLE})"
echo "    ✔ ${ROOT_MF} -> 调和方: ${OPS_SA} (projects/${PROJECT_ID}/roles/${OPS_ROLE_ID})"

# ------------------------------------------------------------------------------
# 阶段 5: 创建 Artifact Registry 仓库并构建容器镜像
# ------------------------------------------------------------------------------
echo "==> [5/8] 构建并推送 Python 高性能调和引擎镜像..."
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" \
  --project="${PROJECT_ID}" &>/dev/null || \
  gcloud artifacts repositories create "${REPO_NAME}" --location="${REGION}" \
    --project="${PROJECT_ID}" --repository-format=docker

build_ctx=$(mktemp -d)
cp Dockerfile config.env reconcile_fast.py "${build_ctx}/"
gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" "${build_ctx}"
rm -rf "${build_ctx}"
echo "    ✔ 镜像构建并推送成功: ${IMAGE}"

# ------------------------------------------------------------------------------
# 阶段 6: 部署 Cloud Run Job (计算型调和引擎)
# ------------------------------------------------------------------------------
echo "==> [6/8] 部署 Cloud Run Job (mf-reconcile)..."
JOB_NAME="mf-reconcile"
EXCLUDE_DBS_LIST="${EXCLUDE_DBS:-tmp.db,kafka_test.db,udf.db,bench,test_batch_1,test_batch_2,test_batch_3,test_p}"

gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --max-retries=3 \
  --task-timeout=1h \
  --memory=1Gi \
  --cpu=2 \
  --set-env-vars="^#^PROJECT_ID=${PROJECT_ID}#REGION=${REGION}#BUCKET=${BUCKET}#DATA_ROOT_PREFIX=${DATA_ROOT_PREFIX}#FOLDER_DATE_FORMAT=${FOLDER_DATE_FORMAT}#HOT_DAYS=${HOT_DAYS}#HOT_SA=${HOT_SA}#COLD_SA=${COLD_SA}#MAX_WORKERS=30#EXCLUDE_DBS=${EXCLUDE_DBS_LIST}"
echo "    ✔ Cloud Run Job ${JOB_NAME} 部署成功 (规格: 2 vCPU / 1 GiB / 30 并发 Worker)"

# ------------------------------------------------------------------------------
# 阶段 7: 配置 Cloud Scheduler 每日自动调度
# ------------------------------------------------------------------------------
echo "==> [7/8] 配置 Cloud Scheduler 每日定时任务..."
# 授予调和 SA 触发 Cloud Run Job 的调用权限
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" --role="roles/run.invoker" >/dev/null

SCHEDULER_JOB_NAME="${JOB_NAME}-daily"
if gcloud scheduler jobs describe "${SCHEDULER_JOB_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    ✔ Cloud Scheduler 任务 ${SCHEDULER_JOB_NAME} 已存在，正在更新..."
  gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
    --location="${REGION}" --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" --time-zone="Etc/UTC" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
else
  echo "    ✔ 创建 Cloud Scheduler 定时任务: 每日 00:30 UTC..."
  gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
    --location="${REGION}" --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" --time-zone="Etc/UTC" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
fi

# ------------------------------------------------------------------------------
# 阶段 8: 完成与使用说明
# ------------------------------------------------------------------------------
echo "========================================================================="
echo "🎉 GCS Managed Folder 生产环境一键部署全部顺利完成！"
echo "========================================================================="
echo "  📌 部署汇总:"
echo "     - GCP 项目 ID   : ${PROJECT_ID}"
echo "     - 存储桶        : ${BUCKET}"
echo "     - Cloud Run 任务: ${JOB_NAME} (Region: ${REGION})"
echo "     - 定时调度计划  : 每日 UTC 00:30 (北京时间 08:30) 自动触发"
echo "     - 运行身份      : ${OPS_SA} (无数据读取权限，零信任合规)"
echo ""
echo "  👉 建议下一步验证操作:"
echo "     1. 立即手动触发一次全量调和测试:"
echo "        gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID} --wait"
echo ""
echo "     2. 运行验收断言测试 (需要当前账号拥有 SA Token Creator 权限):"
echo "        ./verify.sh"
echo "========================================================================="
