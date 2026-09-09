#!/usr/bin/env bash
# 一次性初始化：bucket(UBLA) + lifecycle + 两个 SA + 模拟的天级 Iceberg 目录
set -euo pipefail
cd "$(dirname "$0")"
source ./config.env

echo "==> 1. 创建 bucket（uniform bucket-level access 是 managed folder 的硬性前提）"
if ! gcloud storage buckets describe "${BUCKET}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud storage buckets create "${BUCKET}" \
    --project="${PROJECT_ID}" --location="${REGION}" \
    --uniform-bucket-level-access
else
  echo "    bucket 已存在，跳过"
fi

echo "==> 2. 应用 lifecycle 规则（60 天 Coldline / 365 天 Archive）"
gcloud storage buckets update "${BUCKET}" --lifecycle-file=lifecycle.json

echo "==> 3. 创建 service account（热/冷 reader、写入方、自动化调和）及自定义角色"
for sa in "${HOT_SA_NAME}" "${COLD_SA_NAME}" "${WRITER_SA_NAME}" "${OPS_SA_NAME}"; do
  if ! gcloud iam service-accounts describe "${sa}@${PROJECT_ID}.iam.gserviceaccount.com" \
       --project="${PROJECT_ID}" &>/dev/null; then
    gcloud iam service-accounts create "${sa}" --project="${PROJECT_ID}" \
      --display-name="POC ${sa}"
  else
    echo "    ${sa} 已存在，跳过"
  fi
done

# reconciler 的最小权限自定义角色：只能管理 managed folder + 列目录名，
# 不含 storage.objects.get，读不到任何数据内容
# （storage.admin / storage.folderAdmin 都含 objects.get，不要用）
if ! gcloud iam roles describe "${OPS_ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud iam roles create "${OPS_ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and their IAM; list prefixes; cannot read object data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get" >/dev/null
  echo "    自定义角色 ${OPS_ROLE_ID} 已创建"
fi

# 注意：任何 SA（业务或自动化）都【不要】授予 bucket 级或 project 级的对象权限。
# managed folder 的 IAM 是叠加(additive)的，且不支持 deny，
# bucket 级授权会让 folder 级隔离完全失效。
# 写入方和 reconciler 的权限都授在表前缀级 managed folder 上（见第 4 步），
# 范围被圈定在 DATA_PREFIX 内，bucket 中其他业务数据碰不到。

echo "==> 4. 表前缀级 managed folder：授予写入方(Spark/Flink)整表读写删 + reconciler 管理权限"
echo "     （Iceberg 提交要读写 metadata/，compaction/expire 要跨天操作历史文件，"
echo "      故写入方不参与热冷切换；嵌套的天级 managed folder 权限叠加，互不干扰；"
echo "      reconciler 的自定义角色对嵌套 managed folder 同样生效，无需 bucket 级绑定）"
prefix_mf="${BUCKET}/${DATA_PREFIX}/"
gcloud storage managed-folders create "${prefix_mf}" &>/dev/null || true
prefix_policy=$(mktemp)
cat > "${prefix_policy}" <<EOF
{"bindings": [
  {"role": "${WRITER_ROLE}", "members": ["serviceAccount:${WRITER_SA}"]},
  {"role": "projects/${PROJECT_ID}/roles/${OPS_ROLE_ID}", "members": ["serviceAccount:${OPS_SA}"]}
]}
EOF
gcloud storage managed-folders set-iam-policy "${prefix_mf}" "${prefix_policy}" >/dev/null
rm -f "${prefix_policy}"
echo "    ${prefix_mf} -> ${WRITER_SA} (${WRITER_ROLE})"
echo "    ${prefix_mf} -> ${OPS_SA} (${OPS_ROLE_ID})"

echo "==> 5. 模拟客户目录：生成最近 65 天的天级 folder，每个放一个数据文件"
echo "     （覆盖 60 天阈值两侧，便于验证热/冷切换）"
for offset in 0 1 30 58 59 60 61 65; do
  d=$(date -u -d "-${offset} days" +"${FOLDER_DATE_FORMAT}")
  obj="${BUCKET}/${DATA_PREFIX}/${d}/data-00000.parquet"
  if ! gcloud storage objects describe "${obj}" &>/dev/null; then
    echo "iceberg-data-placeholder ${d}" | gcloud storage cp - "${obj}"
  fi
done

echo "==> 完成。下一步执行 ./reconcile.sh"
