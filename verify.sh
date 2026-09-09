#!/usr/bin/env bash
# 验证热/冷隔离是否生效：分别模拟(impersonate)两个 SA 读取热、冷 folder 中的对象。
# 前提：当前登录账号对两个 SA 具有 roles/iam.serviceAccountTokenCreator。
# 授权命令（只需一次，注意 IAM 传播可能需要 1-2 分钟；
# 若当前身份是用户账号，把 member 前缀 serviceAccount: 换成 user:）：
#   for sa in $HOT_SA $COLD_SA $WRITER_SA $OPS_SA; do
#     gcloud iam service-accounts add-iam-policy-binding $sa \
#       --member="serviceAccount:$(gcloud config get-value account)" \
#       --role="roles/iam.serviceAccountTokenCreator"
#   done
set -uo pipefail
cd "$(dirname "$0")"
source ./config.env

HOT_OBJ="${BUCKET}/${DATA_PREFIX}/$(date -u -d '-1 days' +"${FOLDER_DATE_FORMAT}")/data-00000.parquet"
COLD_OBJ="${BUCKET}/${DATA_PREFIX}/$(date -u -d '-65 days' +"${FOLDER_DATE_FORMAT}")/data-00000.parquet"

check() {
  local sa="$1" op="$2" obj="$3" expect="$4" actual
  case "${op}" in
    read)  gcloud storage cat "${obj}" --impersonate-service-account="${sa}" &>/dev/null \
             && actual="ALLOW" || actual="DENY" ;;
    write) echo "poc-write-test" | gcloud storage cp - "${obj}" \
             --impersonate-service-account="${sa}" &>/dev/null \
             && actual="ALLOW" || actual="DENY" ;;
  esac
  if [[ "${actual}" == "${expect}" ]]; then
    echo "  [PASS] ${sa%%@*} ${op} ${obj#${BUCKET}/} => ${actual}（符合预期）"
  else
    echo "  [FAIL] ${sa%%@*} ${op} ${obj#${BUCKET}/} => ${actual}（预期 ${expect}）"
    FAILED=1
  fi
}

FAILED=0
echo "==> 验证 reader 隔离（人工/查询任务：只能读热，碰不到冷）"
check "${HOT_SA}"  read "${HOT_OBJ}"  "ALLOW"   # 热 reader 读热数据：应成功
check "${HOT_SA}"  read "${COLD_OBJ}" "DENY"    # 热 reader 读冷数据：应被拒（误查冷数据 = 0 费用）
check "${COLD_SA}" read "${COLD_OBJ}" "ALLOW"   # 冷 reader 读冷数据：应成功
check "${COLD_SA}" read "${HOT_OBJ}"  "DENY"    # 冷 reader 读热数据：应被拒

echo "==> 验证写入方（Spark/Flink：冷热均可读写，不受热冷切换影响）"
check "${WRITER_SA}" read  "${HOT_OBJ}"  "ALLOW"                                        # 读热
check "${WRITER_SA}" read  "${COLD_OBJ}" "ALLOW"                                        # 读冷（compaction 场景）
check "${WRITER_SA}" write "${HOT_OBJ%/*}/writer-test.txt"  "ALLOW"                     # 写热
check "${WRITER_SA}" write "${COLD_OBJ%/*}/writer-test.txt" "ALLOW"                     # 写冷（重写历史文件场景）

echo "==> 验证自动化身份（mf-reconciler：只管权限，读不到数据内容）"
check "${OPS_SA}" read "${HOT_OBJ}"  "DENY"    # 自定义角色无 objects.get
check "${OPS_SA}" read "${COLD_OBJ}" "DENY"

(( FAILED == 0 )) && echo "==> 全部通过 ✔" || { echo "==> 存在失败项 ✘"; exit 1; }
