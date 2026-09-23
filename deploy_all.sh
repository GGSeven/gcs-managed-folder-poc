#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 客户生产一键全自动部署脚本 (deploy_all.sh)
# 作用：
#   1. 幂等检查并自动补齐 UBLA 与 IAM 基础权限（防止漏配）
#   2. 自动配置 Spark/Kyuubi 辅助依赖与日志临时目录（彻底消除 403 启动阻碍）
#   3. 内嵌增强版调和引擎源码：支持单表差异化天数（60天/90天）、多级目录(server_dt_utc)与标准目录(dt=)
#   4. 临时构建隔离，秒级 Cloud Build 构建推送轻量容器镜像
#   5. 平滑替换升级现有 Cloud Run Job 与 Cloud Scheduler 定时作业
#   6. 异步触发首次全量存量调和作业（--async，不卡终端，可后台观察日志）
# ==============================================================================
set -euo pipefail

# ==============================================================================
# 【参数配置区】请根据客户实际生产环境替换或直接在当前 Shell export
# ==============================================================================
# 🔴【客户环境必填/确认项】
export PROJECT_ID="${PROJECT_ID:-fr-xjsy-prod}"             # 客户 GCP 项目 ID
export REGION="${REGION:-asia-east1}"                       # 部署地域
export BUCKET="${BUCKET:-gs://fr-xjsy-bigdata-gcs-dev}"     # 客户 Iceberg 存储桶名称
export DATA_ROOT_PREFIX="${DATA_ROOT_PREFIX:-datasets}"     # 存储桶内 Iceberg 根目录
export DEFAULT_HOT_DAYS="${DEFAULT_HOT_DAYS:-60}"           # 默认兜底热数据天数

# 🟡【表级差异化保留天数配置】（环境变量随时可在 Cloud Run 控制台直接修改调试！）
# 格式：表名1:天数,表名2:天数
export TABLE_RETENTION="${TABLE_RETENTION:-ods_can_origin_data_1s_p_t_i:60,ods_can_parse_1s_p_t_i:90}"

# 🟡【库级黑白名单配置】（以英文逗号分隔，库名请带上 .db 后缀）
export INCLUDE_DBS="${INCLUDE_DBS:-ods.db}"                 # 白名单示例: "ods.db" (留空表示处理全部)
export EXCLUDE_DBS="${EXCLUDE_DBS:-tmp.db,kafka_test.db}"   # 黑名单示例: "tmp.db,kafka_test.db"

# 🟢【服务账号配置】
export HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
export COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
export WRITER_SA="${WRITER_SA:-iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com}"
export OPS_SA="${OPS_SA:-mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com}"

export JOB_NAME="mf-reconcile"
export REPO_NAME="mf-poc"
export ROLE_ID="mfReconciler"
export IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "========================================================================="
echo "🚀 开始 GCS Managed Folder 生产自动化升级部署"
echo "   项目 ID:            ${PROJECT_ID}"
echo "   部署地域:           ${REGION}"
echo "   目标存储桶:         ${BUCKET}"
echo "   数据根目录:         ${DATA_ROOT_PREFIX}/"
echo "   表级保留天数规则:   ${TABLE_RETENTION} (默认兜底: ${DEFAULT_HOT_DAYS} 天)"
echo "   库白名单 (仅处理):  ${INCLUDE_DBS:-[全部库]}"
echo "   库黑名单 (排除库):  ${EXCLUDE_DBS:-[无]}"
echo "   运维服务账号:       ${OPS_SA}"
echo "========================================================================="

# ------------------------------------------------------------------------------
# 阶段一：检查并启用必须的 GCP API 服务
# ------------------------------------------------------------------------------
echo -e "\n==> [1/6] 检查并启用 GCP 基础 API 服务..."
gcloud services enable \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"

# ------------------------------------------------------------------------------
# 阶段二：存储桶 UBLA 状态排查与修复（幂等检查）
# ------------------------------------------------------------------------------
echo -e "\n==> [2/6] 检查存储桶 UBLA (Uniform Bucket-Level Access)..."
UBLA_STATUS=$(gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access)" 2>/dev/null || echo "False")
if [[ "${UBLA_STATUS}" != "True" ]]; then
  echo "    ⚠️ 存储桶未开启 UBLA，正在自动为您开启..."
  gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
  echo "    ✔ 存储桶 UBLA 开启成功"
else
  echo "    ✔ 存储桶已开启 UBLA (状态: True)，符合 Managed Folder 准入红线"
fi

# ------------------------------------------------------------------------------
# 阶段三：IAM 账号、角色、顶级与辅助 Managed Folder 授权（幂等检查）
# ------------------------------------------------------------------------------
echo -e "\n==> [3/6] 检查并补齐 IAM 身份与依赖目录权限..."

# 1. 检查 4 个 Service Account
declare -A SAS=(
  ["iceberg-writer"]="Iceberg Pipeline Writer (Spark/Flink)"
  ["iceberg-hot-reader"]="Hot Data Reader (Hue/BI Analyst)"
  ["iceberg-cold-reader"]="Cold Data Reader (Audit/Archive Query)"
  ["mf-reconciler"]="Managed Folder Daily Reconciler (Cloud Run)"
)

for sa in "${!SAS[@]}"; do
  sa_email="${sa}@${PROJECT_ID}.iam.gserviceaccount.com"
  if ! gcloud iam service-accounts describe "${sa_email}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "    ➕ 检测到未创建账号，正在创建: ${sa_email}..."
    gcloud iam service-accounts create "${sa}" \
      --project="${PROJECT_ID}" \
      --display-name="${SAS[$sa]}"
  else
    echo "    ✔ Service Account 已就绪: ${sa_email}"
  fi
done

# 2. 检查自定义角色 mfReconciler
if ! gcloud iam roles describe "${ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    ➕ 正在创建 Reconciler 自定义角色 [${ROLE_ID}]..."
  gcloud iam roles create "${ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
else
  echo "    ✔ 自定义角色已就绪: ${ROLE_ID}"
fi

# 3. 检查顶级 Managed Folder (datasets/)
TOP_MF="${BUCKET}/${DATA_ROOT_PREFIX}/"
gcloud storage managed-folders create "${TOP_MF}" &>/dev/null || true

POLICY_TMP=$(mktemp)
cat > "${POLICY_TMP}" <<EOF_POL
{
  "bindings": [
    {
      "role": "roles/storage.objectUser",
      "members": ["serviceAccount:${WRITER_SA}"]
    },
    {
      "role": "projects/${PROJECT_ID}/roles/${ROLE_ID}",
      "members": ["serviceAccount:${OPS_SA}"]
    }
  ]
}
EOF_POL
gcloud storage managed-folders set-iam-policy "${TOP_MF}" "${POLICY_TMP}" &>/dev/null
rm -f "${POLICY_TMP}"
echo "    ✔ 顶级 Managed Folder [${TOP_MF}] 权限策略确认就绪"

# 4. 配置 Spark/Kyuubi 辅助依赖与日志临时目录 (解决 Spark 启动 403 阻碍)
echo "    ➕ 配置 Spark 运行依赖与日志临时 Managed Folders..."
for aux in "user/spark/" "flink_jar/"; do
  gcloud storage managed-folders create "${BUCKET}/${aux}" &>/dev/null || true
  cat > "${BUILD_DIR:-/tmp}/aux_viewer.json" <<EOF_AUX
{"bindings": [{"role": "roles/storage.objectViewer", "members": ["serviceAccount:${HOT_SA}"]}]}
EOF_AUX
  gcloud storage managed-folders set-iam-policy "${BUCKET}/${aux}" "${BUILD_DIR:-/tmp}/aux_viewer.json" &>/dev/null || true
  rm -f "${BUILD_DIR:-/tmp}/aux_viewer.json"
done

for aux in "spark-job-history/" "spark-tmp/"; do
  gcloud storage managed-folders create "${BUCKET}/${aux}" &>/dev/null || true
  cat > "${BUILD_DIR:-/tmp}/aux_user.json" <<EOF_AUX
{"bindings": [{"role": "roles/storage.objectUser", "members": ["serviceAccount:${HOT_SA}"]}]}
EOF_AUX
  gcloud storage managed-folders set-iam-policy "${BUCKET}/${aux}" "${BUILD_DIR:-/tmp}/aux_user.json" &>/dev/null || true
  rm -f "${BUILD_DIR:-/tmp}/aux_user.json"
done
echo "    ✔ Spark 辅助依赖目录 (user/spark/, flink_jar/) 与读写目录 (spark-job-history/, spark-tmp/) 授权就绪"

# ------------------------------------------------------------------------------
# 阶段四：容器镜像极速构建与推送（隔离上下文，避免打包家目录）
# ------------------------------------------------------------------------------
echo -e "\n==> [4/6] 准备 Artifact Registry 并构建轻量镜像..."

if ! gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    ➕ 创建 Docker 仓库 [${REPO_NAME}]..."
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="Docker repository for Managed Folder reconcile engine"
else
  echo "    ✔ Docker 仓库已就绪: ${REPO_NAME}"
fi

BUILD_DIR=$(mktemp -d)
trap 'rm -rf "${BUILD_DIR}"' EXIT

cat << 'EOF_DOCKER' > "${BUILD_DIR}/Dockerfile"
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY reconcile_fast.py /app/
ENTRYPOINT ["python3", "reconcile_fast.py"]
EOF_DOCKER

cat << 'EOF_PY' > "${BUILD_DIR}/reconcile_fast.py"
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, time, json, urllib.parse, re
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("错误: 缺少 requests 库，请先执行: pip install requests", flush=True)
    sys.exit(1)

PROJECT_ID = os.environ.get("PROJECT_ID", "fr-xjsy-prod")
BUCKET = os.environ.get("BUCKET", "gs://fr-xjsy-bigdata-gcs-dev")
BUCKET_NAME = BUCKET.replace("gs://", "").strip("/")
DATA_ROOT_PREFIX = os.environ.get("DATA_ROOT_PREFIX", "datasets").strip("/")
DEFAULT_HOT_DAYS = int(os.environ.get("DEFAULT_HOT_DAYS") or os.environ.get("HOT_DAYS") or "60")

# 表保留天数规则解析 (支持通过环境变量 TABLE_RETENTION 随时热更新与调试)
TABLE_RULES = {}
env_rules = os.environ.get("TABLE_RETENTION", "ods_can_origin_data_1s_p_t_i:60,ods_can_parse_1s_p_t_i:90")
if env_rules:
    for item in env_rules.split(","):
        if ":" in item:
            k, v = item.strip().split(":")
            TABLE_RULES[k.strip()] = int(v.strip())

HOT_SA = os.environ.get("HOT_SA", f"iceberg-hot-reader@{PROJECT_ID}.iam.gserviceaccount.com")
COLD_SA = os.environ.get("COLD_SA", f"iceberg-cold-reader@{PROJECT_ID}.iam.gserviceaccount.com")

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "30"))
RECONCILE_MODE = os.environ.get("RECONCILE_MODE", "incremental").lower()

EXCLUDE_DBS = set(filter(None, os.environ.get("EXCLUDE_DBS", "tmp.db,kafka_test.db").split(",")))
INCLUDE_DBS = set(filter(None, os.environ.get("INCLUDE_DBS", "ods.db").split(",")))

AUX_DIRECTORIES = {
    "user/spark/": ("roles/storage.objectViewer", [HOT_SA]),
    "flink_jar/": ("roles/storage.objectViewer", [HOT_SA]),
    "spark-job-history/": ("roles/storage.objectUser", [HOT_SA]),
    "spark-tmp/": ("roles/storage.objectUser", [HOT_SA]),
}

API_BASE = f"https://storage.googleapis.com/storage/v1/b/{BUCKET_NAME}"

# 同时兼容标准单级 dt=YYYY-MM-DD 和复杂多级 server_dt_utc=YYYY-MM-DD
DATE_REGEX = re.compile(r"(?:server_dt_utc|dt)=(\d{4}-\d{2}-\d{2})")

def get_access_token():
    env_token = os.environ.get("GCS_TOKEN") or os.environ.get("ACCESS_TOKEN")
    if env_token:
        return env_token.strip()
    if sys.platform != "win32" or os.environ.get("K_SERVICE") or os.environ.get("CLOUD_RUN_JOB"):
        try:
            r = requests.get(
                "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
                headers={"Metadata-Flavor": "Google"},
                timeout=2
            )
            if r.status_code == 200:
                return r.json().get("access_token")
        except Exception:
            pass
    import subprocess
    return subprocess.check_output(["gcloud", "auth", "print-access-token"], text=True, shell=(sys.platform == "win32")).strip()

def build_http_session(token):
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 2, max_retries=2)
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
        resp = session.get(url, timeout=10)
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

def find_date_partitions(session, base_prefix, depth=1, max_depth=3):
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
        return (part_path, "SKIP_NO_DATE", False)
    d_str = match.group(1)
    try:
        dt_epoch = datetime.strptime(d_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return (part_path, "SKIP_BAD_DATE", False)

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
    print(f"🚀 启动客户定制化 GCS Managed Folder 调和引擎 (并发线程数: {MAX_WORKERS})", flush=True)
    print(f"   项目: {PROJECT_ID} | 存储桶: {BUCKET_NAME}", flush=True)
    print(f"   表保留天数映射: {TABLE_RULES} (默认兜底: {DEFAULT_HOT_DAYS} 天)", flush=True)
    print(f"   运行模式: {RECONCILE_MODE.upper()}", flush=True)
    print("=========================================================================", flush=True)

    token = get_access_token()
    session = build_http_session(token)

    # 1. 配置 Spark/Kyuubi 辅助目录
    print("--> [阶段 1/3] 检查并配置 Spark/Kyuubi 辅助依赖与日志临时目录...", flush=True)
    for aux_path, (role, sa_list) in AUX_DIRECTORIES.items():
        ensure_managed_folder(session, aux_path)
        ok = set_managed_folder_iam(session, aux_path, sa_list, role)
        print(f"    ✔ 辅助目录 [{aux_path}] -> 角色: {role} (状态: {'OK' if ok else 'FAIL'})", flush=True)

    # 2. 扫描业务库与表结构
    print(f"\n--> [阶段 2/3] 扫描业务库表结构 (根目录: {DATA_ROOT_PREFIX}/)...", flush=True)
    dbs = list_sub_prefixes(session, f"{DATA_ROOT_PREFIX}/")
    selected_tables = []
    for db in dbs:
        db_name = db.rstrip("/").split("/")[-1]
        if INCLUDE_DBS and db_name not in INCLUDE_DBS:
            continue
        if db_name in EXCLUDE_DBS:
            continue
        tables = list_sub_prefixes(session, db)
        selected_tables.extend(tables)

    print(f"✔ 发现 {len(selected_tables)} 张业务表", flush=True)

    # 3. 处理表元数据及分区调和
    print(f"\n--> [阶段 3/3] 开始调和表级元数据与冷热数据分区...", flush=True)
    total_parts = 0
    hot_count = 0
    cold_count = 0

    for tbl in selected_tables:
        tbl_name = tbl.rstrip("/").split("/")[-1]
        hot_threshold = TABLE_RULES.get(tbl_name, DEFAULT_HOT_DAYS)
        print(f"\n--- 处理数据表: {tbl_name} (冷热分界阈值: {hot_threshold} 天) ---", flush=True)

        meta_path = f"{tbl}metadata/"
        ensure_managed_folder(session, meta_path)
        set_managed_folder_iam(session, meta_path, [HOT_SA, COLD_SA], "roles/storage.objectViewer")
        print(f"    ✔ 元数据目录已赋权: {meta_path}", flush=True)

        data_prefix = f"{tbl}data/"
        date_parts = find_date_partitions(session, data_prefix)
        print(f"    扫描到 {len(date_parts)} 个日期分区...", flush=True)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_partition, session, p, hot_threshold): p for p in date_parts}
            for f in as_completed(futures):
                try:
                    part_path, status, ok = f.result()
                    total_parts += 1
                    if status == "HOT":
                        hot_count += 1
                    elif status == "COLD":
                        cold_count += 1
                except Exception:
                    pass

    print("\n=========================================================================", flush=True)
    print(f"🎉 调和完成！总分区数: {total_parts} | 热分区(放行): {hot_count} | 冷分区(拦截): {cold_count}", flush=True)
    print("=========================================================================", flush=True)

if __name__ == "__main__":
    main()
EOF_PY

cat << 'EOF_IGNORE' > "${BUILD_DIR}/.gcloudignore"
.git
.gitignore
*.env
*.sh
*.md
EOF_IGNORE

echo "    ✔ 构建上下文已隔离，提交 Cloud Build 构建镜像 (上传大小 <30KB)..."
gcloud builds submit "${BUILD_DIR}" --tag "${IMAGE}" --project="${PROJECT_ID}"

# ------------------------------------------------------------------------------
# 阶段五：部署 / 平滑更新 Cloud Run Job 与 Cloud Scheduler（平滑替换旧版本）
# ------------------------------------------------------------------------------
echo -e "\n==> [5/6] 部署/更新 Cloud Run Job [${JOB_NAME}] 与定时作业..."

ENV_FILE="${BUILD_DIR}/env_vars.yaml"
cat > "${ENV_FILE}" <<EOF_YAML
PROJECT_ID: "${PROJECT_ID}"
REGION: "${REGION}"
BUCKET: "${BUCKET}"
DATA_ROOT_PREFIX: "${DATA_ROOT_PREFIX}"
DEFAULT_HOT_DAYS: "${DEFAULT_HOT_DAYS}"
TABLE_RETENTION: "${TABLE_RETENTION}"
HOT_SA: "${HOT_SA}"
COLD_SA: "${COLD_SA}"
MAX_WORKERS: "30"
RECONCILE_MODE: "incremental"
EXCLUDE_DBS: "${EXCLUDE_DBS}"
INCLUDE_DBS: "${INCLUDE_DBS}"
EOF_YAML

# 平滑更新现有 Cloud Run Job
gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --cpu=2 \
  --memory=1Gi \
  --max-retries=0 \
  --task-timeout=15m \
  --env-vars-file="${ENV_FILE}"

gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" \
  --role="roles/run.invoker" &>/dev/null || true

# 配置或更新 Cloud Scheduler 每日定时触发
SCHEDULER_JOB="${JOB_NAME}-daily"
SCHEDULER_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "${SCHEDULER_JOB}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    ✔ 更新已有的 Cloud Scheduler 每日触发任务..."
  gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
else
  echo "    ✔ 创建全新的 Cloud Scheduler 每日触发任务..."
  gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" \
    --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
fi

# ------------------------------------------------------------------------------
# 阶段六：异步触发首次全量存量调和作业（--async，不卡终端）
# ------------------------------------------------------------------------------
echo -e "\n==> [6/6] 异步调用首次【全量存量调和】任务..."
echo "    指令: gcloud run jobs execute ${JOB_NAME} --update-env-vars=RECONCILE_MODE=full --async"

EXEC_NAME=$(gcloud run jobs execute "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --update-env-vars="RECONCILE_MODE=full" \
  --async \
  --format="value(metadata.name)")

echo "========================================================================="
echo "🎉 部署完成并已启动首次全量调和！"
echo "   作业名称:       ${JOB_NAME} (已平滑更新最新镜像与环境变量)"
echo "   已触发全量执行: ${EXEC_NAME}"
echo "   表级保留天数:   ${TABLE_RETENTION} (默认兜底: ${DEFAULT_HOT_DAYS} 天)"
echo "   Cloud Scheduler: ${SCHEDULER_JOB} (日常定时模式)"
echo "-------------------------------------------------------------------------"
echo "💡 提示：该任务已在 GCP 后台异步运行，终端无需等待！"
echo "   查看实时全量日志请在 GCP 控制台进入："
echo "   Cloud Run -> Jobs (作业) -> ${JOB_NAME} -> Executions (执行)"
echo "   或者执行命令查看："
echo "   gcloud run jobs executions describe ${EXEC_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo "========================================================================="
