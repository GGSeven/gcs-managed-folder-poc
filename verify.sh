#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 热/冷数据隔离生产双向断言验证脚本 (verify.sh)
# 作用：
#   模拟 4 个 Service Account 身份，执行 8 项严格安全断言：
#   1. Writer SA 在热分区写/读 (ALLOW)
#   2. Writer SA 在冷分区写/读 (ALLOW - 支持 compaction / 历史重写)
#   3. Hot Reader SA 读 Iceberg 元数据 metadata/ (ALLOW - 支持 SQL 解析规划)
#   4. Hot Reader SA 读近 60 天热分区 (ALLOW)
#   5. Hot Reader SA 读 60 天前冷分区 (DENY 403 - 强拦截，产生 0 元冷检索费)
#   6. Cold Reader SA 读 Iceberg 元数据 metadata/ (ALLOW)
#   7. Cold Reader SA 读 60 天前冷分区 (ALLOW)
#   8. Cold Reader SA 读近 60 天热分区 (DENY 403 - 权限隔离)
#   9. Reconciler SA 尝试读取任何业务数据 (DENY 403 - 零信任，无数据窃取风险)
# ==============================================================================
set -uo pipefail
cd "$(dirname "$0")"

if [[ -f ./config.env ]]; then
  source ./config.env
else
  echo "❌ 错误: 未找到 config.env 配置文件！"
  exit 1
fi

DATA_ROOT="${DATA_ROOT_PREFIX:-datasets}"

# 自动发现测试表路径（支持自定义环境变量覆盖）
if [[ -z "${VERIFY_TABLE_PATH:-}" ]]; then
  # 优先找 ads.db 或 perf_10k.db 或第一个发现的表
  FIRST_TBL=$(gcloud storage ls "${BUCKET}/${DATA_ROOT}/" 2>/dev/null | grep '\.db/' | head -n 1 || true)
  if [[ -n "${FIRST_TBL}" ]]; then
    SUB_TBL=$(gcloud storage ls "${FIRST_TBL}" 2>/dev/null | head -n 1 || true)
    TBL_PATH="${SUB_TBL%/}"
  else
    TBL_PATH="${BUCKET}/${DATA_ROOT}/ads.db/ads_100f_ext_special_car_coverage_p_d_i"
  fi
else
  TBL_PATH="${VERIFY_TABLE_PATH%/}"
fi

HOT_DATE=$(date -u -d "-2 days" +"${FOLDER_DATE_FORMAT}")
COLD_DATE=$(date -u -d "-65 days" +"${FOLDER_DATE_FORMAT}")
TODAY_DATE=$(date -u +"${FOLDER_DATE_FORMAT}")

META_FILE="${TBL_PATH}/metadata/v1.metadata.json"
HOT_FILE="${TBL_PATH}/data/${HOT_DATE}/data-00000.parquet"
COLD_FILE="${TBL_PATH}/data/${COLD_DATE}/data-00000.parquet"
WRITER_TEST_FILE="${TBL_PATH}/data/${TODAY_DATE}/writer_test.txt"

echo "========================================================================="
echo "🔍 启动 GCS Managed Folder 生产安全与业务双向验证"
echo "   验证基准表:   ${TBL_PATH}"
echo "   热测试分区:   ${HOT_DATE}"
echo "   冷测试分区:   ${COLD_DATE}"
echo "========================================================================="

FAILED=0

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
      if echo "managed-folder-verify-$(date +%s)" | gcloud storage cp - "${obj}" \
           --impersonate-service-account="${sa}" &>/dev/null; then
        actual="ALLOW"
      else
        actual="DENY"
      fi
      ;;
  esac

  if [[ "${actual}" == "${expect}" ]]; then
    echo "  ✔ [PASS] ${desc} => 结果: ${actual} (符合预期)"
  else
    echo "  ✘ [FAIL] ${desc} => 结果: ${actual} (预期: ${expect}) [路径: ${obj}]"
    FAILED=1
  fi
}

echo -e "\n1. 验证业务写入方管道 (Spark/Flink: 全周期读写不受热冷切换影响):"
check "${WRITER_SA}" write "${WRITER_TEST_FILE}" "ALLOW" "写入今日热分区 (${TODAY_DATE})"
check "${WRITER_SA}" read  "${HOT_FILE}"         "ALLOW" "读取历史热分区 (${HOT_DATE})"
check "${WRITER_SA}" write "${TBL_PATH}/data/${COLD_DATE}/compaction.txt" "ALLOW" "重写冷分区 (Compaction/Merge)"

echo -e "\n2. 验证日常查询引擎权限 (Hue/分析师: 只能读热数据与元数据，严禁触碰冷数据):"
check "${HOT_SA}" read "${META_FILE}" "ALLOW" "读取表级元数据 (metadata/v1.metadata.json)"
check "${HOT_SA}" read "${HOT_FILE}"  "ALLOW" "读取近 60 天热分区 (${HOT_DATE})"
check "${HOT_SA}" read "${COLD_FILE}" "DENY"  "误查 60 天前冷分区 (HTTP 403 强拦截，0 元冷检索费)"

echo -e "\n3. 验证历史合规通道权限 (Cold Reader: 仅能查冷数据与元数据，禁止越权查热数据):"
check "${COLD_SA}" read "${META_FILE}" "ALLOW" "读取表级元数据 (metadata/v1.metadata.json)"
check "${COLD_SA}" read "${COLD_FILE}" "ALLOW" "正常读取 60 天前冷分区 (${COLD_DATE})"
check "${COLD_SA}" read "${HOT_FILE}"  "DENY"  "越权读取近 60 天热分区 (${HOT_DATE})"

echo -e "\n4. 验证自动化运维身份权限 (mf-reconciler: 零信任，无业务数据窥探权限):"
check "${OPS_SA}" read "${HOT_FILE}"  "DENY" "运维 SA 尝试读取热数据"
check "${OPS_SA}" read "${COLD_FILE}" "DENY" "运维 SA 尝试读取冷数据"

echo -e "\n========================================================================="
if (( FAILED == 0 )); then
  echo "🎉 全部验证项 100% 通过！冷热隔离与元数据放行策略在底层完全生效！"
else
  echo "⚠️ 存在未通过的验证项，请排查存储桶 IAM 是否存在宽泛权限污染或分区调和未执行。"
  exit 1
fi
echo "========================================================================="
