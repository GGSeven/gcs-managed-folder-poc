#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 基础环境初始化脚本 (setup.sh)
# 作用：
#   1. 检查/开启 Bucket UBLA（统一存储桶级访问）
#   2. 创建 4 个专用 Service Accounts
#   3. 创建 Reconciler 最小权限自定义角色 (mfReconciler)
#   4. 初始化顶级 Managed Folder (datasets/) 并绑定基础 IAM 策略
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"

# 加载配置变量（兼容直接 export 或通过 config.env 加载）
if [[ -f ./config.env ]]; then
  source ./config.env
fi

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
if [[ -z "${PROJECT_ID}" ]]; then
  echo "❌ 错误: 未检测到 PROJECT_ID，请先执行: export PROJECT_ID=您的项目ID"
  exit 1
fi

REGION="${REGION:-asia-east1}"
BUCKET="${BUCKET:-gs://${PROJECT_ID}-mf-poc}}"
DATA_ROOT_PREFIX="${DATA_ROOT_PREFIX:-datasets}"
HOT_DAYS="${HOT_DAYS:-60}"
FOLDER_DATE_FORMAT="${FOLDER_DATE_FORMAT:-dt=%Y-%m-%d}"

HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
WRITER_SA="${WRITER_SA:-iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com}"
OPS_SA="${OPS_SA:-mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com}"

WRITER_ROLE="${WRITER_ROLE:-roles/storage.objectUser}"
OPS_ROLE_ID="${OPS_ROLE_ID:-mfReconciler}"

echo "========================================================================="
echo "🚀 开始初始化 GCS Managed Folder 基础环境与 IAM 配置"
echo "   项目 ID:     ${PROJECT_ID}"
echo "   所在地域:    ${REGION}"
echo "   目标存储桶:  ${BUCKET}"
echo "   根目录前缀:  ${DATA_ROOT_PREFIX}"
echo "========================================================================="

# 1. 检查并确保存储桶开启 UBLA
echo -e "\n==> [1/4] 检查存储桶 UBLA (Uniform Bucket-Level Access) 状态..."
if ! gcloud storage buckets describe "${BUCKET}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    存储桶不存在，正在创建并直接开启 UBLA..."
  gcloud storage buckets create "${BUCKET}" \
    --project="${PROJECT_ID}" --location="${REGION}" \
    --uniform-bucket-level-access
  echo "    ✔ 存储桶 ${BUCKET} 创建成功且已开启 UBLA"
else
  UBLA_ENABLED=$(gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access.enabled)" 2>/dev/null || echo "False")
  if [[ "${UBLA_ENABLED}" != "True" ]]; then
    echo "    存储桶未开启 UBLA，正在开启..."
    gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
    echo "    ✔ 存储桶已成功更新为 UBLA 模式"
  else
    echo "    ✔ 存储桶 UBLA 状态正常 (Enabled)"
  fi
fi

# 2. 创建 4 个专用 Service Account
echo -e "\n==> [2/4] 创建 4 个生产专用服务账号 (Service Accounts)..."
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
    echo "    ✔ 创建成功: ${sa_email}"
  else
    echo "    ✔ 已存在，跳过创建: ${sa_email}"
  fi
done

# 3. 创建 Reconciler 最小权限自定义角色
echo -e "\n==> [3/4] 创建 Reconciler 最小权限自定义角色 [${OPS_ROLE_ID}]..."
if ! gcloud iam roles describe "${OPS_ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud iam roles create "${OPS_ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
  echo "    ✔ 自定义角色 ${OPS_ROLE_ID} 创建成功"
else
  echo "    ✔ 自定义角色 ${OPS_ROLE_ID} 已存在，跳过创建"
fi

# 4. 创建顶级 Managed Folder 并下发基础策略
echo -e "\n==> [4/4] 初始化顶级 Managed Folder [${DATA_ROOT_PREFIX}/] 并挂载 IAM 策略..."
TOP_MF="${BUCKET}/${DATA_ROOT_PREFIX}/"

# 创建顶级 Managed Folder (若已存在则忽略)
gcloud storage managed-folders create "${TOP_MF}" &>/dev/null || true

# 构造策略文件
POLICY_FILE=$(mktemp)
cat > "${POLICY_FILE}" <<EOF
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

gcloud storage managed-folders set-iam-policy "${TOP_MF}" "${POLICY_FILE}"
rm -f "${POLICY_FILE}"

echo "    ✔ 顶级 Managed Folder [${TOP_MF}] 策略绑定完成："
echo "       - ${WRITER_SA} -> ${WRITER_ROLE} (全库读写删)"
echo "       - ${OPS_SA}    -> ${OPS_ROLE_ID} (调和管理权)"

echo "========================================================================="
echo "🎉 基础环境初始化完成！请继续执行阶段三 (调和存量数据) 或阶段四 (一键部署 deploy.sh)"
echo "========================================================================="
