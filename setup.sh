#!/usr/bin/env bash
# ==============================================================================
# setup.sh - 一次性环境初始化脚本
# 包含：Bucket 创建 (UBLA) + 生命周期规则 + 4个专属服务账号 + 自定义角色 + 根 Managed Folder 授权 + 模拟验证数据
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"
source ./config.env

echo "========================================================================="
echo "🚀 开始执行环境配置 (Environment Setup)"
echo "   项目 ID: ${PROJECT_ID} | 区域: ${REGION}"
echo "   存储桶: ${BUCKET} | 数据根路径: ${DATA_ROOT_PREFIX}/"
echo "========================================================================="

echo "==> 1. 创建 Bucket 并开启 UBLA（Managed Folder 的硬性前置要求）"
if ! gcloud storage buckets describe "${BUCKET}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud storage buckets create "${BUCKET}" \
    --project="${PROJECT_ID}" --location="${REGION}" \
    --uniform-bucket-level-access
  echo "    ✔ Bucket ${BUCKET} 创建成功并已开启 UBLA"
else
  echo "    ✔ Bucket 已存在，确保开启 UBLA"
  gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
fi

echo "==> 2. 应用 Lifecycle 规则（60 天转 Coldline / 365 天转 Archive）"
if [[ -f "lifecycle.json" ]]; then
  gcloud storage buckets update "${BUCKET}" --lifecycle-file=lifecycle.json
  echo "    ✔ 存储桶生命周期规则已成功应用"
fi

echo "==> 3. 创建 4 大专属服务账号（Service Accounts）"
for sa in "${HOT_SA_NAME}" "${COLD_SA_NAME}" "${WRITER_SA_NAME}" "${OPS_SA_NAME}"; do
  if ! gcloud iam service-accounts describe "${sa}@${PROJECT_ID}.iam.gserviceaccount.com" \
       --project="${PROJECT_ID}" &>/dev/null; then
    gcloud iam service-accounts create "${sa}" --project="${PROJECT_ID}" \
      --display-name="SA for ${sa}"
    echo "    ✔ 服务账号 ${sa} 创建成功"
  else
    echo "    ✔ 服务账号 ${sa} 已存在，跳过"
  fi
done

echo "==> 4. 创建 Reconciler 专用最小权限自定义角色 (${OPS_ROLE_ID})"
# 仅授予控制面操作，无 storage.objects.get，无法窥探或读取任何业务数据内容
if ! gcloud iam roles describe "${OPS_ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud iam roles create "${OPS_ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and their IAM; list prefixes; cannot read object data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get" >/dev/null
  echo "    ✔ 自定义角色 ${OPS_ROLE_ID} 创建成功"
else
  echo "    ✔ 自定义角色 ${OPS_ROLE_ID} 已存在，跳过"
fi

echo "==> 5. 创建数据根目录 Managed Folder 并注入基础权限"
# 权限严格圈定在 datasets/ 根目录内，绝不在 Bucket 级赋予读取权限
prefix_mf="${BUCKET}/${DATA_ROOT_PREFIX}/"
gcloud storage managed-folders create "${prefix_mf}" &>/dev/null || true

prefix_policy=$(mktemp)
cat > "${prefix_policy}" <<EOF
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
gcloud storage managed-folders set-iam-policy "${prefix_mf}" "${prefix_policy}" >/dev/null
rm -f "${prefix_policy}"
echo "    ✔ ${prefix_mf} -> 写入方 ${WRITER_SA} (${WRITER_ROLE})"
echo "    ✔ ${prefix_mf} -> 调和方 ${OPS_SA} (${OPS_ROLE_ID})"

echo "==> 6. 为当前管理员账号授予 TokenCreator 权限（供本地 impersonate 验证）"
CURRENT_USER=$(gcloud config get-value account 2>/dev/null || true)
if [[ -n "${CURRENT_USER}" ]]; then
  MEMBER_TYPE="user"
  if [[ "${CURRENT_USER}" == *"gserviceaccount.com"* ]]; then
    MEMBER_TYPE="serviceAccount"
  fi
  for sa in "${HOT_SA}" "${COLD_SA}" "${WRITER_SA}" "${OPS_SA}"; do
    gcloud iam service-accounts add-iam-policy-binding "${sa}" \
      --project="${PROJECT_ID}" \
      --member="${MEMBER_TYPE}:${CURRENT_USER}" \
      --role="roles/iam.serviceAccountTokenCreator" &>/dev/null || true
  done
  echo "    ✔ 已为 ${CURRENT_USER} 授予 4 个 SA 的 TokenCreator 权限"
fi

echo "==> 7. 生成基础验证表结构与测试数据 (demo.db/sales_order)"
DEMO_TABLE="${BUCKET}/${DATA_ROOT_PREFIX}/demo.db/sales_order"
# 写入 Iceberg 元数据文件
echo '{"format-version": 2, "table-uuid": "demo-sales-order-001"}' | \
  gcloud storage cp - "${DEMO_TABLE}/metadata/v1.metadata.json" &>/dev/null || true

# 生成跨越 60 天边界的天级数据文件
for offset in 0 1 30 58 59 60 61 65; do
  d=$(date -u -d "-${offset} days" +"${FOLDER_DATE_FORMAT}")
  obj="${DEMO_TABLE}/data/${d}/data-00000.parquet"
  if ! gcloud storage objects describe "${obj}" &>/dev/null; then
    echo "iceberg-mock-data-${d}" | gcloud storage cp - "${obj}" &>/dev/null || true
  fi
done
echo "    ✔ 基础验证数据生成完毕: 覆盖 metadata/、今日增量及 60 天前后冷热边界"

echo "========================================================================="
echo "✔ 环境配置全部完成！下一步请执行调和脚本: python3 reconcile_fast.py"
echo "========================================================================="
