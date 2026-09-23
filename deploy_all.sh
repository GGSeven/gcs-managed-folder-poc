#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 客户定制生产级自动化部署脚本 (双表精准管控 + Spark依赖补齐)
# ==============================================================================
set -euo pipefail

# ------------------------------------------------------------------------------
# 1. 基础配置与环境变量读取 (支持纯 export，无需 .env 文件)
# ------------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
if [ -z "${PROJECT_ID}" ]; then
  echo "✘ 错误: 未检测到 GCP Project ID，请先执行: export PROJECT_ID=\"your-project-id\""
  exit 1
fi

REGION="${REGION:-us-central1}"
BUCKET="${BUCKET:-gs://${PROJECT_ID}-mf-poc}"
if [[ "${BUCKET}" != gs://* ]]; then
  echo "✘ 错误: BUCKET 必须以 gs:// 开头，当前为: ${BUCKET}"
  exit 1
fi

BUCKET_NAME="${BUCKET#gs://}"
BUCKET_NAME="${BUCKET_NAME%/}"

# 业务账号
HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
WRITER_SA="${WRITER_SA:-iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com}"

# 平台调和专属账号与角色
RECONCILER_SA="${RECONCILER_SA:-iceberg-reconciler@${PROJECT_ID}.iam.gserviceaccount.com}"
CUSTOM_ROLE_ID="mfReconciler"

# 客户核心管控表与保留期配置 (注入 Cloud Run 环境变量，方便控制台随时调试修改)
TARGET_TABLES="${TARGET_TABLES:-datasets/ods.db/ods_can_origin_data_1s_p_t_i,datasets/ods.db/ods_can_parse_1s_p_t_i}"
TABLE_RETENTION="${TABLE_RETENTION:-ods_can_origin_data_1s_p_t_i:60,ods_can_parse_1s_p_t_i:90}"
DEFAULT_HOT_DAYS="${DEFAULT_HOT_DAYS:-60}"

JOB_NAME="gcs-mf-reconciler"
SCHEDULER_JOB_NAME="gcs-mf-daily-job"
IMAGE_NAME="gcr.io/${PROJECT_ID}/gcs-mf-reconciler:latest"

echo "========================================================================="
echo "🚀 启动 GCS Managed Folder 精准调和自动化部署"
echo "   项目 ID:     ${PROJECT_ID}"
echo "   目标存储桶:  ${BUCKET}"
echo "   管控表范围:  ${TARGET_TABLES}"
echo "   保留期配置:  ${TABLE_RETENTION} (默认兜底: ${DEFAULT_HOT_DAYS} 天)"
echo "   Hot Reader:  ${HOT_SA}"
echo "   Cold Reader: ${COLD_SA}"
echo "========================================================================="

# ------------------------------------------------------------------------------
# 2. 清理旧版本遗留 Job 与定时器 (避免双 Job 并行冲突)
# ------------------------------------------------------------------------------
echo -e "\n--> [1/7] 检查并清理旧版本 Job (mf-reconcile)..."
gcloud scheduler jobs delete mf-reconcile-daily --location="${REGION}" --quiet &>/dev/null || true
gcloud run jobs delete mf-reconcile --region="${REGION}" --quiet &>/dev/null || true
echo "    ✔ 旧版本作业与触发器清理完成。"

# ------------------------------------------------------------------------------
# 3. 检查存储桶 UBLA 状态
# ------------------------------------------------------------------------------
echo -e "\n--> [2/7] 检查存储桶 Uniform Bucket-Level Access (UBLA)..."
UBLA_STATUS=$(gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access)" 2>/dev/null || true)
if [ "${UBLA_STATUS}" != "True" ] && [ "${UBLA_STATUS}" != "enabled" ]; then
  echo "    ✔ 启用存储桶 UBLA..."
  gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
else
  echo "    ✔ UBLA 已处于启用状态。"
fi

# ------------------------------------------------------------------------------
# 3. 幂等性创建 Service Accounts 与 IAM 角色
# ------------------------------------------------------------------------------
echo -e "\n--> [2/6] 检查并初始化 IAM 服务账号与角色..."
create_sa_if_not_exists() {
  local sa_email="$1"
  local sa_id="${sa_email%%@*}"
  local display_name="$2"
  if ! gcloud iam service-accounts describe "${sa_email}" &>/dev/null; then
    echo "    ✔ 创建 SA: ${sa_email}"
    gcloud iam service-accounts create "${sa_id}" --display-name="${display_name}"
  else
    echo "    ✔ SA 已存在: ${sa_email}"
  fi
}

create_sa_if_not_exists "${WRITER_SA}" "Iceberg Pipeline Writer"
create_sa_if_not_exists "${HOT_SA}" "Iceberg Hot Partition Reader"
create_sa_if_not_exists "${COLD_SA}" "Iceberg Cold Partition Reader"
create_sa_if_not_exists "${RECONCILER_SA}" "Managed Folder Reconciler Automation"

if ! gcloud iam roles describe "${CUSTOM_ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    ✔ 创建自定义权限角色: ${CUSTOM_ROLE_ID}"
  gcloud iam roles create "${CUSTOM_ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --permissions="storage.managedFolders.create,storage.managedFolders.delete,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.setIamPolicy,storage.managedFolders.getIamPolicy,storage.objects.list,storage.objects.get" \
    --stage="GA"
else
  echo "    ✔ 自定义角色 ${CUSTOM_ROLE_ID} 已存在。"
fi

# 授予调和账号权限
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${RECONCILER_SA}" \
  --role="projects/${PROJECT_ID}/roles/${CUSTOM_ROLE_ID}" \
  --condition=None --quiet >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${RECONCILER_SA}" \
  --role="roles/run.invoker" \
  --condition=None --quiet >/dev/null

# ------------------------------------------------------------------------------
# 4. 初始化根级与 Spark 辅助依赖 Managed Folders
# ------------------------------------------------------------------------------
echo -e "\n--> [3/6] 初始化 GCS Managed Folder 顶层策略与 Spark 辅助依赖放行..."
TMP_POL=$(mktemp)

# 1. 顶层 datasets/
gcloud storage managed-folders create "${BUCKET}/datasets/" &>/dev/null || true
cat <<EOF > "${TMP_POL}"
{
  "bindings": [
    {"role": "roles/storage.objectAdmin", "members": ["serviceAccount:${WRITER_SA}"]},
    {"role": "roles/storage.admin", "members": ["serviceAccount:${RECONCILER_SA}"]}
  ]
}
EOF
gcloud storage managed-folders set-iam-policy "${BUCKET}/datasets/" "${TMP_POL}" >/dev/null
echo "    ✔ 顶层目录 datasets/ 基础策略配置完成。"

# 2. Spark/Kyuubi 依赖 (user/spark/ 与 flink_jar/)
cat <<EOF > "${TMP_POL}"
{
  "bindings": [
    {"role": "roles/storage.objectViewer", "members": ["serviceAccount:${HOT_SA}"]}
  ]
}
EOF
for dep_path in "user/spark/" "flink_jar/"; do
  gcloud storage managed-folders create "${BUCKET}/${dep_path}" &>/dev/null || true
  gcloud storage managed-folders set-iam-policy "${BUCKET}/${dep_path}" "${TMP_POL}" >/dev/null
  echo "    ✔ 依赖目录 [${dep_path}] 已对 hot-reader 放行只读。"
done

# 3. Spark 日志与上传临时目录 (spark-job-history/ 与 spark-tmp/)
cat <<EOF > "${TMP_POL}"
{
  "bindings": [
    {"role": "roles/storage.objectUser", "members": ["serviceAccount:${HOT_SA}"]}
  ]
}
EOF
for rw_path in "spark-job-history/" "spark-tmp/"; do
  gcloud storage managed-folders create "${BUCKET}/${rw_path}" &>/dev/null || true
  gcloud storage managed-folders set-iam-policy "${BUCKET}/${rw_path}" "${TMP_POL}" >/dev/null
  echo "    ✔ 临时与日志目录 [${rw_path}] 已对 hot-reader 放行读写。"
done
rm -f "${TMP_POL}"

# ------------------------------------------------------------------------------
# 5. 构建隔离 Docker 上下文并提交 Cloud Build
# ------------------------------------------------------------------------------
echo -e "\n--> [4/6] 准备独立构建上下文并编译容器镜像..."
BUILD_DIR=$(mktemp -d)

cat <<'PYEOF' > "${BUILD_DIR}/reconcile.py"
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, time, json, urllib.parse, re
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter

PROJECT_ID = os.environ.get("PROJECT_ID", "")
BUCKET = os.environ.get("BUCKET", "")
BUCKET_NAME = BUCKET.replace("gs://", "").strip("/")
DEFAULT_HOT_DAYS = int(os.environ.get("DEFAULT_HOT_DAYS", os.environ.get("HOT_DAYS", "60")))

TABLE_RULES = {}
raw_rules = os.environ.get("TABLE_RETENTION", "")
if raw_rules:
    for item in raw_rules.split(","):
        if ":" in item:
            k, v = item.strip().split(":")
            TABLE_RULES[k.strip()] = int(v.strip())

raw_tables = os.environ.get("TARGET_TABLES", "")
TARGET_TABLES = [t.strip().strip("/") for t in raw_tables.split(",") if t.strip()]

HOT_SA = os.environ.get("HOT_SA", "")
COLD_SA = os.environ.get("COLD_SA", "")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "30"))

API_BASE = f"https://storage.googleapis.com/storage/v1/b/{BUCKET_NAME}"
DATE_REGEX = re.compile(r"(?:server_dt_utc|dt)=(\d{4}-\d{2}-\d{2})")

def get_access_token():
    env_token = os.environ.get("GCS_TOKEN") or os.environ.get("ACCESS_TOKEN")
    if env_token:
        return env_token.strip()
    try:
        r = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"},
            timeout=3
        )
        if r.status_code == 200:
            return r.json().get("access_token")
    except Exception:
        pass
    import subprocess
    return subprocess.check_output(["gcloud", "auth", "print-access-token"], text=True).strip()

def build_http_session(token):
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 2, max_retries=3)
    session.mount("https://", adapter)
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    return session

def list_sub_prefixes(session, prefix):
    prefixes = []
    page_token = ""
    while True:
        url = f"{API_BASE}/o?prefix={prefix}&delimiter=/&fields=prefixes,nextPageToken"
        if page_token:
            url += f"&pageToken={page_token}"
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            break
        data = resp.json()
        prefixes.extend(data.get("prefixes", []))
        page_token = data.get("nextPageToken", "")
        if not page_token:
            break
    return prefixes

def ensure_managed_folder(session, folder_path):
    url = f"{API_BASE}/managedFolders"
    resp = session.post(url, json={"name": folder_path}, timeout=10)
    return resp.status_code in (200, 201, 409)

def set_managed_folder_iam(session, folder_path, sa_list, role="roles/storage.objectViewer"):
    encoded = urllib.parse.quote(folder_path, safe="")
    url = f"{API_BASE}/managedFolders/{encoded}/iam"
    body = {"bindings": [{"role": role, "members": [f"serviceAccount:{sa}" for sa in sa_list]}]}
    resp = session.put(url, json=body, timeout=10)
    return resp.status_code == 200

def find_date_partitions(session, base_prefix, depth=1, max_depth=4):
    results = []
    sub_prefixes = list_sub_prefixes(session, base_prefix)
    for p in sub_prefixes:
        match = DATE_REGEX.search(p)
        if match:
            results.append(p)
        elif depth < max_depth:
            results.extend(find_date_partitions(session, p, depth + 1, max_depth))
    return results

TODAY_EPOCH = time.time()

def process_partition(session, part_path, hot_days_threshold):
    match = DATE_REGEX.search(part_path)
    if not match:
        return (part_path, "SKIP_NO_DATE")
    d_str = match.group(1)
    try:
        dt_epoch = datetime.strptime(d_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return (part_path, "SKIP_BAD_DATE")

    age_days = (TODAY_EPOCH - dt_epoch) / 86400
    ensure_managed_folder(session, part_path)
    if age_days < hot_days_threshold:
        ok = set_managed_folder_iam(session, part_path, [HOT_SA], "roles/storage.objectViewer")
        return (part_path, "HOT", ok)
    else:
        ok = set_managed_folder_iam(session, part_path, [COLD_SA], "roles/storage.objectViewer")
        return (part_path, "COLD", ok)

def main():
    print("=========================================================================", flush=True)
    print(f"🚀 GCS Managed Folder 目标表定向调和任务启动", flush=True)
    print(f"   项目: {PROJECT_ID} | 存储桶: {BUCKET_NAME}", flush=True)
    print(f"   目标表白名单: {TARGET_TABLES}", flush=True)
    print(f"   保留期规则映射: {TABLE_RULES} (默认: {DEFAULT_HOT_DAYS} 天)", flush=True)
    print("=========================================================================", flush=True)

    token = get_access_token()
    session = build_http_session(token)

    total_parts = 0
    hot_count = 0
    cold_count = 0

    for tbl_prefix in TARGET_TABLES:
        tbl_path = f"{tbl_prefix}/" if not tbl_prefix.endswith("/") else tbl_prefix
        tbl_name = tbl_path.rstrip("/").split("/")[-1]
        hot_threshold = TABLE_RULES.get(tbl_name, DEFAULT_HOT_DAYS)

        print(f"\n--> 开始处理目标表: {tbl_name} (阈值: {hot_threshold} 天)...", flush=True)
        
        meta_path = f"{tbl_path}metadata/"
        ensure_managed_folder(session, meta_path)
        set_managed_folder_iam(session, meta_path, [HOT_SA, COLD_SA], "roles/storage.objectViewer")
        print(f"    ✔ 元数据目录已就绪: {meta_path}", flush=True)

        data_prefix = f"{tbl_path}data/"
        date_parts = find_date_partitions(session, data_prefix)
        print(f"    ✔ 检索到 {len(date_parts)} 个日期分区...", flush=True)

        for p in date_parts:
            part_path, status, ok = process_partition(session, p, hot_threshold)
            total_parts += 1
            if status == "HOT":
                hot_count += 1
                print(f"    [HOT 放行]  {part_path} -> hot-reader (OK)", flush=True)
            elif status == "COLD":
                cold_count += 1
                print(f"    [COLD 拦截] {part_path} -> cold-reader (OK)", flush=True)

    print("\n=========================================================================", flush=True)
    print(f"🎉 调和完成！总分区数: {total_parts} | 热分区(放行): {hot_count} | 冷分区(拦截): {cold_count}", flush=True)
    print("=========================================================================", flush=True)

if __name__ == "__main__":
    main()
PYEOF

cat <<'DOCKEREOF' > "${BUILD_DIR}/Dockerfile"
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY reconcile.py .
CMD ["python", "reconcile.py"]
DOCKEREOF

echo "    ✔ 提交 Cloud Build 编译镜像..."
gcloud builds submit "${BUILD_DIR}" --tag "${IMAGE_NAME}" --quiet
rm -rf "${BUILD_DIR}"

# ------------------------------------------------------------------------------
# 6. 部署 / 更新 Cloud Run Job (通过 YAML 传入环境变量，彻底解决逗号分隔解析问题)
# ------------------------------------------------------------------------------
echo -e "\n--> [5/6] 部署 / 更新 Cloud Run Job (${JOB_NAME})..."
ENV_YAML_FILE=$(mktemp)
cat <<EOF > "${ENV_YAML_FILE}"
PROJECT_ID: "${PROJECT_ID}"
BUCKET: "${BUCKET}"
HOT_SA: "${HOT_SA}"
COLD_SA: "${COLD_SA}"
TARGET_TABLES: "${TARGET_TABLES}"
TABLE_RETENTION: "${TABLE_RETENTION}"
DEFAULT_HOT_DAYS: "${DEFAULT_HOT_DAYS}"
EOF

if gcloud run jobs describe "${JOB_NAME}" --region="${REGION}" &>/dev/null; then
  echo "    ✔ 更新现有 Cloud Run Job..."
  gcloud run jobs update "${JOB_NAME}" \
    --image="${IMAGE_NAME}" \
    --region="${REGION}" \
    --service-account="${RECONCILER_SA}" \
    --tasks=1 \
    --cpu=2 \
    --memory=2Gi \
    --max-retries=2 \
    --task-timeout=3600 \
    --env-vars-file="${ENV_YAML_FILE}" \
    --quiet
else
  echo "    ✔ 创建全新 Cloud Run Job..."
  gcloud run jobs create "${JOB_NAME}" \
    --image="${IMAGE_NAME}" \
    --region="${REGION}" \
    --service-account="${RECONCILER_SA}" \
    --tasks=1 \
    --cpu=2 \
    --memory=2Gi \
    --max-retries=2 \
    --task-timeout=3600 \
    --env-vars-file="${ENV_YAML_FILE}" \
    --quiet
fi
rm -f "${ENV_YAML_FILE}"

# ------------------------------------------------------------------------------
# 7. 配置 Cloud Scheduler 定时触发
# ------------------------------------------------------------------------------
echo -e "\n--> [6/6] 配置 Cloud Scheduler 每日定时任务 (${SCHEDULER_JOB_NAME})..."
RUN_JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "${SCHEDULER_JOB_NAME}" --location="${REGION}" &>/dev/null; then
  echo "    ✔ 更新定时触发器..."
  gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
    --location="${REGION}" \
    --schedule="0 2 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${RUN_JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${RECONCILER_SA}" \
    --quiet
else
  echo "    ✔ 创建定时触发器..."
  gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
    --location="${REGION}" \
    --schedule="0 2 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${RUN_JOB_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${RECONCILER_SA}" \
    --quiet
fi

# ------------------------------------------------------------------------------
# 8. 触发首次全量定向调和 (异步执行，不卡死 Cloud Shell)
# ------------------------------------------------------------------------------
echo -e "\n========================================================================="
echo "🎉 部署全部成功！触发首次定向调和任务..."
echo "========================================================================="
gcloud run jobs execute "${JOB_NAME}" --region="${REGION}" --async

echo -e "\n✔ 任务已进入后台执行！您可以随时运行以下命令查看实时调和日志："
echo "   gcloud logging read 'resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"${JOB_NAME}\"' --limit 20 --format='value(textPayload)'"
