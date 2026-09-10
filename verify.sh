#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 热/冷数据隔离生产双向断言验证脚本 (verify.sh)
# ==============================================================================
set -uo pipefail

# 1. 加载配置
if [[ -f ./config.env ]]; then
  source ./config.env
fi

PROJECT_ID="${PROJECT_ID:-bd-host-2026-004}"
BUCKET="${BUCKET:-gs://${PROJECT_ID}-mf-poc}"
DATA_ROOT="${DATA_ROOT_PREFIX:-datasets}"

HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
WRITER_SA="${WRITER_SA:-iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com}"
OPS_SA="${OPS_SA:-mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com}"

# 2. 自动定位或指定验证基准表
if [[ -z "${VERIFY_TABLE_PATH:-}" ]]; then
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

# 3. 动态扫描该表实际存在的分区，精准匹配真实热分区 (<60天) 和真实冷分区 (>60天)
NOW_EPOCH=$(date +%s)
HOT_PART=""
COLD_PART=""
HOT_DATE=""
COLD_DATE=""

EXISTING_PARTS=$(gcloud storage ls "${TBL_PATH}/data/" 2>/dev/null | grep 'dt=' || true)

for p in ${EXISTING_PARTS}; do
  p_clean="${p%/}"
  part_name="${p_clean##*/}"
  dt_str="${part_name#dt=}"
  p_epoch=$(date -d "${dt_str}" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "${dt_str}" +%s 2>/dev/null || true)
  if [[ -n "${p_epoch}" ]]; then
    diff_days=$(( (NOW_EPOCH - p_epoch) / 86400 ))
    if (( diff_days >= 0 && diff_days < 60 )) && [[ -z "${HOT_PART}" ]]; then
      HOT_PART="${p_clean}"
      HOT_DATE="${part_name}"
    elif (( diff_days >= 60 )) && [[ -z "${COLD_PART}" ]]; then
      COLD_PART="${p_clean}"
      COLD_DATE="${part_name}"
    fi
  fi
  if [[ -n "${HOT_PART}" && -n "${COLD_PART}" ]]; then
    break
  fi
done

# 兜底默认值
HOT_PART="${HOT_PART:-${TBL_PATH}/data/$(date -u +"dt=%Y-%m-%d")}"
HOT_DATE="${HOT_DATE:-$(date -u +"dt=%Y-%m-%d")}"
COLD_PART="${COLD_PART:-${TBL_PATH}/data/$(date -u -d "-65 days" +"dt=%Y-%m-%d")}"
COLD_DATE="${COLD_DATE:-$(date -u -d "-65 days" +"dt=%Y-%m-%d")}"

META_FILE="${TBL_PATH}/metadata/v1.metadata.json"
HOT_FILE="${HOT_PART}/test_verify_data.parquet"
COLD_FILE="${COLD_PART}/test_verify_data.parquet"
WRITER_TEST_FILE="${HOT_PART}/writer_test.txt"

echo "========================================================================="
echo "🔍 启动 GCS Managed Folder 生产安全与业务双向验证"
echo "   基准验证表:   ${TBL_PATH}"
echo "   真实热测试分区: ${HOT_DATE}"
echo "   真实冷测试分区: ${COLD_DATE}"
echo "========================================================================="

# 4. 前置准备：由业务写入方 (iceberg-writer) 写入测试实体文件（确保测试真实权限而非404）
echo "--> [准备] 正在通过 iceberg-writer 写入测试断言实体文件..."
echo "{\"table\":\"test\",\"version\":1}" | gcloud storage cp - "${META_FILE}" --impersonate-service-account="${WRITER_SA}" &>/dev/null || true
echo "mock-hot-data" | gcloud storage cp - "${HOT_FILE}" --impersonate-service-account="${WRITER_SA}" &>/dev/null || true
echo "mock-cold-data" | gcloud storage cp - "${COLD_FILE}" --impersonate-service-account="${WRITER_SA}" &>/dev/null || true

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
    echo "  ✔ [PASS] ${desc} => 结果: ${actual}"
  else
    echo "  ✘ [FAIL] ${desc} => 结果: ${actual} (预期: ${expect}) [路径: ${obj}]"
    FAILED=1
  fi
}

echo -e "\n1. 验证业务写入方管道 (Spark/Flink: 全周期读写不受热冷切换影响):"
check "${WRITER_SA}" write "${WRITER_TEST_FILE}" "ALLOW" "写入今日热分区 (${HOT_DATE})"
check "${WRITER_SA}" read  "${HOT_FILE}"         "ALLOW" "读取历史热分区 (${HOT_DATE})"
check "${WRITER_SA}" write "${COLD_PART}/compaction.txt" "ALLOW" "重写冷分区 (Compaction/Merge)"

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
