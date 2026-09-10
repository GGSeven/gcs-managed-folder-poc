#!/usr/bin/env bash
# ==============================================================================
# verify.sh - 权限隔离自动化验证脚本
# 通过 impersonate 模拟各个业务角色，断言 403 强拦截与透明放行
# ==============================================================================
set -uo pipefail
cd "$(dirname "$0")"
source ./config.env

DEMO_TABLE="${BUCKET}/${DATA_ROOT_PREFIX}/demo.db/sales_order"
HOT_DATE=$(date -u -d '-1 days' +"${FOLDER_DATE_FORMAT}")
COLD_DATE=$(date -u -d '-65 days' +"${FOLDER_DATE_FORMAT}")

META_OBJ="${DEMO_TABLE}/metadata/v1.metadata.json"
HOT_OBJ="${DEMO_TABLE}/data/${HOT_DATE}/data-00000.parquet"
COLD_OBJ="${DEMO_TABLE}/data/${COLD_DATE}/data-00000.parquet"

echo "========================================================================="
echo "🧪 开始执行权限隔离自动化断言测试 (Verification)"
echo "   热分区测试路径: ${HOT_OBJ#${BUCKET}/}"
echo "   冷分区测试路径: ${COLD_OBJ#${BUCKET}/}"
echo "   元数据测试路径: ${META_OBJ#${BUCKET}/}"
echo "========================================================================="

check() {
  local sa="$1" op="$2" obj="$3" expect="$4" desc="$5" actual
  case "${op}" in
    read)
      if gcloud storage cat "${obj}" --impersonate-service-account="${sa}" &>/dev/null; then
        actual="ALLOW"
      else
        actual="DENY"
      fi
      ;;
    write)
      if echo "poc-write-test" | gcloud storage cp - "${obj}" --impersonate-service-account="${sa}" &>/dev/null; then
        actual="ALLOW"
      else
        actual="DENY"
      fi
      ;;
  esac

  if [[ "${actual}" == "${expect}" ]]; then
    echo "  [PASS ✔] ${desc} => 实际: ${actual} (符合预期)"
  else
    echo "  [FAIL ✘] ${desc} => 实际: ${actual} (预期: ${expect})"
    FAILED=1
  fi
}

FAILED=0

echo -e "\n--> [1/4] 验证 Iceberg 元数据放行 (Hot / Cold Reader 均必须可读)"
check "${HOT_SA}"  read "${META_OBJ}" "ALLOW" "Hot Reader 读取 Iceberg metadata"
check "${COLD_SA}" read "${META_OBJ}" "ALLOW" "Cold Reader 读取 Iceberg metadata"

echo -e "\n--> [2/4] 验证查询分析端隔离 (Hue / 交互式查询：热数据放行，冷数据 403 强拦截)"
check "${HOT_SA}"  read "${HOT_OBJ}"  "ALLOW" "Hot Reader 正常读取热分区"
check "${HOT_SA}"  read "${COLD_OBJ}" "DENY"  "Hot Reader 误查冷分区 (403 强拦截，0元检索费)"
check "${COLD_SA}" read "${COLD_OBJ}" "ALLOW" "Cold Reader 正常调阅冷分区"
check "${COLD_SA}" read "${HOT_OBJ}"  "DENY"  "Cold Reader 越权访问热分区 (403 拦截)"

echo -e "\n--> [3/4] 验证写入端全生命周期权限 (Spark / Flink：冷热皆可读写与 Compaction)"
check "${WRITER_SA}" read  "${HOT_OBJ}"  "ALLOW" "Writer SA 读取热数据"
check "${WRITER_SA}" read  "${COLD_OBJ}" "ALLOW" "Writer SA 读取冷数据 (Compaction 规划)"
check "${WRITER_SA}" write "${HOT_OBJ%/*}/writer-test.txt"  "ALLOW" "Writer SA 写入新热分区"
check "${WRITER_SA}" write "${COLD_OBJ%/*}/writer-test.txt" "ALLOW" "Writer SA 重写历史冷数据"

echo -e "\n--> [4/4] 验证调和引擎零特权原则 (Reconciler SA：只控权限，读不到数据)"
check "${OPS_SA}" read "${HOT_OBJ}"  "DENY" "Reconciler SA 读取热数据 (无 objects.get)"
check "${OPS_SA}" read "${COLD_OBJ}" "DENY" "Reconciler SA 读取冷数据 (无 objects.get)"

echo "========================================================================="
if (( FAILED == 0 )); then
  echo "🎉 恭喜！全部权限隔离与安全断言 100% 通过验证 ✔"
  exit 0
else
  echo "⚠️ 存在未通过项，请排查调和脚本运行状态及 IAM 策略 ✘"
  exit 1
fi
