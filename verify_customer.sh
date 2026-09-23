#!/usr/bin/env bash
# ==============================================================================
# 客户定制场景全链路自动化验证脚本 (适用于 Cloud Shell / Linux 环境)
# ==============================================================================
set -e

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
BUCKET="${BUCKET:-gs://${PROJECT_ID}-mf-poc}"
HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================================================="
echo "🔍 开始自动化验证客户定制场景隔离策略"
echo "   项目: ${PROJECT_ID} | 存储桶: ${BUCKET}"
echo "   Hot Reader:  ${HOT_SA}"
echo "   Cold Reader: ${COLD_SA}"
echo "========================================================================="

ALL_PASS=true

test_action() {
  local desc="$1"
  local sa="$2"
  local expect="$3"
  local cmd="$4"

  echo -n "  测试: ${desc} ... "
  local out
  if out=$(eval "${cmd}" 2>&1); then
    actual="ALLOW"
  else
    actual="DENY (403)"
  fi

  if [ "${actual}" == "${expect}" ]; then
    echo -e "${GREEN}✔ [PASS]${NC} (预期: ${expect} | 实际: ${actual})"
  else
    echo -e "${RED}✘ [FAIL]${NC} (预期: ${expect} | 实际: ${actual})"
    ALL_PASS=false
  fi
}

echo -e "\n【测试项 1】验证 Spark/Kyuubi 辅助目录放行 (解决启动 403 阻碍)"
test_action "读取 user/spark/ 运行依赖" "${HOT_SA}" "ALLOW" \
  "gcloud storage cat '${BUCKET}/user/spark/kyuubi-spark-sql-engine_2.12-1.9.3.jar' --impersonate-service-account='${HOT_SA}'"

test_action "读取 flink_jar/ Iceberg 依赖" "${HOT_SA}" "ALLOW" \
  "gcloud storage cat '${BUCKET}/flink_jar/iceberg-hive-runtime-1.4.2.jar' --impersonate-service-account='${HOT_SA}'"

test_action "写入 spark-job-history/ 事件日志" "${HOT_SA}" "ALLOW" \
  "echo 'LOG_TEST' | gcloud storage cp - '${BUCKET}/spark-job-history/test_verify.log' --impersonate-service-account='${HOT_SA}'"

test_action "写入 spark-tmp/ 资源上传临时目录" "${HOT_SA}" "ALLOW" \
  "echo 'TMP_TEST' | gcloud storage cp - '${BUCKET}/spark-tmp/test_upload.tmp' --impersonate-service-account='${HOT_SA}'"

echo -e "\n【测试项 2】验证表 1: ods_can_origin_data_1s_p_t_i (60 天冷热分界)"
# 获取最新分区和历史冷分区
HOT_SAMPLE_1=$(gcloud storage ls "${BUCKET}/datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/**" 2>/dev/null | grep -E "server_dt_utc=2026-09" | head -n 1 || true)
COLD_SAMPLE_1=$(gcloud storage ls "${BUCKET}/datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/**" 2>/dev/null | grep -E "server_dt_utc=2026-07-01" | head -n 1 || true)

test_action "读取表元数据 metadata/" "${HOT_SA}" "ALLOW" \
  "gcloud storage cat '${BUCKET}/datasets/ods.db/ods_can_origin_data_1s_p_t_i/metadata/v1.metadata.json' --impersonate-service-account='${HOT_SA}'"

if [ -n "${HOT_SAMPLE_1}" ]; then
  test_action "读取近 60 天热分区" "${HOT_SA}" "ALLOW" \
    "gcloud storage cat '${HOT_SAMPLE_1}' --impersonate-service-account='${HOT_SA}'"
fi

if [ -n "${COLD_SAMPLE_1}" ]; then
  test_action "读取 60 天前冷分区 (强拦截)" "${HOT_SA}" "DENY (403)" \
    "gcloud storage cat '${COLD_SAMPLE_1}' --impersonate-service-account='${HOT_SA}'"
  
  test_action "Cold Reader 读取 60 天前冷分区" "${COLD_SA}" "ALLOW" \
    "gcloud storage cat '${COLD_SAMPLE_1}' --impersonate-service-account='${COLD_SA}'"
fi

echo -e "\n【测试项 3】验证表 2: ods_can_parse_1s_p_t_i (90 天冷热分界)"
HOT_SAMPLE_2=$(gcloud storage ls "${BUCKET}/datasets/ods.db/ods_can_parse_1s_p_t_i/data/**" 2>/dev/null | grep -E "server_dt_utc=2026-07-15" | head -n 1 || true)
COLD_SAMPLE_2=$(gcloud storage ls "${BUCKET}/datasets/ods.db/ods_can_parse_1s_p_t_i/data/**" 2>/dev/null | grep -E "server_dt_utc=2026-05-01" | head -n 1 || true)

test_action "读取表元数据 metadata/" "${HOT_SA}" "ALLOW" \
  "gcloud storage cat '${BUCKET}/datasets/ods.db/ods_can_parse_1s_p_t_i/metadata/v1.metadata.json' --impersonate-service-account='${HOT_SA}'"

if [ -n "${HOT_SAMPLE_2}" ]; then
  test_action "读取 70 天前数据 (90天阈值内仍为热数据)" "${HOT_SA}" "ALLOW" \
    "gcloud storage cat '${HOT_SAMPLE_2}' --impersonate-service-account='${HOT_SA}'"
fi

if [ -n "${COLD_SAMPLE_2}" ]; then
  test_action "读取 145 天前冷分区 (强拦截)" "${HOT_SA}" "DENY (403)" \
    "gcloud storage cat '${COLD_SAMPLE_2}' --impersonate-service-account='${HOT_SA}'"

  test_action "Cold Reader 读取 145 天前冷分区" "${COLD_SA}" "ALLOW" \
    "gcloud storage cat '${COLD_SAMPLE_2}' --impersonate-service-account='${COLD_SA}'"
fi

echo -e "\n========================================================================="
if [ "${ALL_PASS}" = true ]; then
  echo -e "${GREEN}🎉 恭喜！客户定制场景所有隔离与依赖用例全部 [PASS] 通过！${NC}"
else
  echo -e "${RED}✘ 存在未通过的用例，请检查上述详情。${NC}"
fi
echo "========================================================================="
