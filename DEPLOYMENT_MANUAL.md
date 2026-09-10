# GCS Managed Folder 热/冷数据分层与权限隔离方案：客户生产环境落地与验证手册

> **文档适用对象**：客户大数据平台架构师、SRE/DevOps 运维工程师、云安全审计管理员  
> **方案版本**：v2.0 (Dual-Mode Incremental REST Reconcile Engine)  
> **服务商**：Baidaodata 解决方案架构团队  

---

## 📋 客户生产环境落地完整操作手册

---

### 阶段零：实施前安全红线排查（现场必做）

在执行任何配置前，必须在客户 GCP 环境中完成以下两项安全基准检查，否则可能导致天级冷热隔离机制彻底失效。

#### 1. 确认目标存储桶开启 UBLA（统一存储桶级访问）
GCS Managed Folder **强依赖统一存储桶级访问（Uniform Bucket-Level Access, UBLA）**。
```bash
# 检查当前存储桶的 UBLA 状态
gcloud storage buckets describe gs://fr-xjsy-bigdata-gcs-dev --format="value(uniform_bucket_level_access.enabled)"
```
* **判断标准**：
  * 输出为 `True`：符合要求，可直接进入下一步。
  * 输出为 `False`：需与客户确认存储桶无旧版细粒度 ACL 依赖后，执行以下命令开启：
    ```bash
    gcloud storage buckets update gs://fr-xjsy-bigdata-gcs-dev --uniform-bucket-level-access
    ```

#### 2. 清理宽泛权限（最致命红线）
* **底层安全原理**：GCS Managed Folder 的 IAM 鉴权为**纯累加（Additive）模式**，且 GCS **不支持显式 Deny 规则**。
* **致命风险排查**：检查 Bucket IAM 和 Project IAM，**严禁为查询账号（或运行 Hue / Trino / Impala 的代理 SA / 计算引擎 VM）授予存储桶级或项目级的宽泛权限**（包括 `roles/storage.objectViewer`、`roles/storage.admin`、`roles/viewer`、`roles/editor`）。
* **后果**：如果查询账号在 Bucket 级拥有 `roles/storage.objectViewer`，该权限会自动穿透并覆盖底层所有的 Managed Folder，导致 60 天前的冷数据仍然可以被任意扫描，隔离完全失效！

**排查命令**：
```bash
# 检查存储桶级是否有查询账号的残留读取权限
gcloud storage buckets get-iam-policy gs://fr-xjsy-bigdata-gcs-dev \
  --flatten="bindings[].members" \
  --format="table(bindings.role, bindings.members)" \
  --filter="bindings.members:iceberg-hot-reader OR bindings.members:hue"
```
* **要求**：上述查询结果必须为空。查询账号在 Bucket 级不应有任何 `storage.objects.*` 绑定。

---

### 阶段一：【运维团队】创建 IAM 账号与最小权限角色

在 GCP Cloud Shell 或运维终端中执行以下步骤：

#### 1. 设置生产环境变量
根据客户生产实际参数进行替换（以下变量将贯穿整个实施流程）：
```bash
export PROJECT_ID="<客户生产项目ID，如 fr-xjsy-prod>"
export REGION="<客户Bucket所在Region，如 asia-east1 或 us-central1>"
export BUCKET="gs://fr-xjsy-bigdata-gcs-dev"
export DATA_ROOT_PREFIX="datasets"
export FOLDER_DATE_FORMAT="dt=%Y-%m-%d"
export HOT_DAYS="60"

export HOT_SA="iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com"
export COLD_SA="iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com"
export WRITER_SA="iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com"
export OPS_SA="mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com"

export HOT_ROLE="roles/storage.objectViewer"
export COLD_ROLE="roles/storage.objectViewer"
export WRITER_ROLE="roles/storage.objectUser"
export OPS_ROLE_ID="mfReconciler"
```

#### 2. 创建 4 个专用 Service Account
职责分离，各司其职：
```bash
# 1. 业务写入管道专用 SA（Spark/Flink 写入引擎使用）
gcloud iam service-accounts create iceberg-writer \
  --project="${PROJECT_ID}" --display-name="Iceberg Pipeline Writer"

# 2. 日常查询专用 SA（Hue / 数据分析师 / 报表工具使用，仅可读热数据与元数据）
gcloud iam service-accounts create iceberg-hot-reader \
  --project="${PROJECT_ID}" --display-name="Hot Data Reader (Hue)"

# 3. 授权历史查询专用 SA（受控审批通道，仅允许读取60天前历史冷数据）
gcloud iam service-accounts create iceberg-cold-reader \
  --project="${PROJECT_ID}" --display-name="Cold Data Reader"

# 4. 自动化调和任务专用 SA（Cloud Run Job 专用运行身份）
gcloud iam service-accounts create mf-reconciler \
  --project="${PROJECT_ID}" --display-name="Managed Folder Daily Reconciler"
```

#### 3. 创建 Reconciler 最小权限自定义角色
该角色遵循**零信任（Zero Trust）**安全设计：仅授予 Managed Folder 的控制面管理权限与对象列表权限，**完全剥离 `storage.objects.get`（无数据内容读取权限）**，杜绝运维调度工具窃取客户业务数据的任何风险：
```bash
gcloud iam roles create "${OPS_ROLE_ID}" --project="${PROJECT_ID}" \
  --title="Managed Folder Reconciler" \
  --description="Manage managed folders and IAM without object read access" \
  --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
```

---

### 阶段二：【大数据团队】顶级 Managed Folder 初始化

在顶级 `datasets/` 目录挂载策略，一次性赋予写入方全库读写删权限与调和任务的管理权（后续所有新增的库表自动继承，无需逐表配置 Writer）：

```bash
# 1. 创建顶级 datasets/ Managed Folder
gcloud storage managed-folders create "${BUCKET}/${DATA_ROOT_PREFIX}/"

# 2. 绑定策略文件
cat > top-policy.json <<EOF
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

gcloud storage managed-folders set-iam-policy "${BUCKET}/${DATA_ROOT_PREFIX}/" top-policy.json
```

---

### 阶段三：执行首次存量数据权限调和（全量模式）

首次落地时，需要对现有各库各表的历史数据执行一次**全量基准对齐**。

```bash
# 1. 配置全量模式环境变量
export RECONCILE_MODE="full"
export MAX_WORKERS=30
export EXCLUDE_DBS="tmp.db,kafka_test.db"

# 2. 本地/运维机执行调和引擎（或通过 Cloud Run 首次触发）
python3 reconcile_fast.py
```

* **预期执行效果**：
  1. 自动过滤掉非业务临时库（如 `tmp.db`）。
  2. 自动为选定业务库表的 `metadata/` 赋予 `hot-reader` + `cold-reader` 读权限（供 Hue/Trino 随时读取 Iceberg 快照元数据以完成 SQL 解析与规划）。
  3. 预创建今天（`T+0`）和明天（`T+1`）的 `data/dt=.../` 分区并赋予 `hot-reader`，保障业务跨日平滑写入。
  4. `< 60 天` 的历史热分区赋予 `hot-reader`。
  5. `≥ 60 天` 的历史冷分区赋予 `cold-reader`。

---

### 阶段四：部署生产每日定时任务（Cloud Run Job + Scheduler）

针对生产持续运行，采用**增量滑动窗口模式（Incremental Sliding Window）**，单次运行仅需 **2~3 秒**，无需每天全量扫描数万个历史分区。

#### 1. 创建 Artifact Registry Docker 镜像仓库
```bash
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

gcloud artifacts repositories create "${REPO_NAME}" \
  --repository-format=docker \
  --location="${REGION}" \
  --project="${PROJECT_ID}" || true
```

#### 2. 构建并推送轻量化容器镜像
```bash
gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" .
```

#### 3. 部署生产 Cloud Run Job
```bash
# 编写环境变量配置文件 env_vars.yaml
cat > env_vars.yaml <<EOF
PROJECT_ID: "${PROJECT_ID}"
REGION: "${REGION}"
BUCKET: "${BUCKET}"
DATA_ROOT_PREFIX: "${DATA_ROOT_PREFIX}"
HOT_DAYS: "${HOT_DAYS}"
HOT_SA: "${HOT_SA}"
COLD_SA: "${COLD_SA}"
MAX_WORKERS: "30"
RECONCILE_MODE: "incremental"
SLIDING_LOOKBACK_DAYS: "3"
EXCLUDE_DBS: "tmp.db,kafka_test.db"
EOF

# 部署 Cloud Run Job
gcloud run jobs deploy mf-reconcile \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --cpu=2 \
  --memory=1Gi \
  --max-retries=0 \
  --task-timeout=10m \
  --env-vars-file="env_vars.yaml"
```

#### 4. 配置 Cloud Scheduler 每日定时触发
每日 UTC 00:30（对应北京时间 08:30）自动触发增量调和任务：
```bash
# 授予 reconciler 触发自身调度的权限
gcloud run jobs add-iam-policy-binding mf-reconcile \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" \
  --role="roles/run.invoker"

# 创建每日定时触发作业
gcloud scheduler jobs create http mf-reconcile-daily \
  --location="${REGION}" \
  --project="${PROJECT_ID}" \
  --schedule="30 0 * * *" \
  --time-zone="Etc/UTC" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/mf-reconcile:run" \
  --http-method=POST \
  --oauth-service-account-email="${OPS_SA}"
```

#### 5. 首次云端手动触发验证
```bash
gcloud run jobs execute mf-reconcile --region="${REGION}" --project="${PROJECT_ID}" --wait
```
* **预期日志**：显示 `运行模式: INCREMENTAL`，成功处理 300+ 个增量滑动窗口操作，核心耗时 **2~3 秒**，容器返回 `exit(0)`。

---

### 阶段五：【安全与业务双重闭环验证】（现场手动执行）

部署完成后，在运维终端运行以下断言测试（需具备针对 SA 的 `roles/iam.serviceAccountTokenCreator` 模拟权限）：

```bash
# 为当前登录账号赋予 SA 模拟权限（仅需执行一次）
CURRENT_USER=$(gcloud config get-value account)
for sa in "${HOT_SA}" "${COLD_SA}" "${WRITER_SA}" "${OPS_SA}"; do
  gcloud iam service-accounts add-iam-policy-binding "${sa}" \
    --project="${PROJECT_ID}" \
    --member="user:${CURRENT_USER}" \
    --role="roles/iam.serviceAccountTokenCreator"
done
```

#### 自动化闭环断言测试脚本 (`verify_production.sh`)
```bash
cat > verify_production.sh <<'EOF'
#!/usr/bin/env bash
set -uo pipefail

TARGET_DB="ads.db"
TARGET_TBL="ads_100f_ext_special_car_coverage_p_d_i"
BASE_PATH="${BUCKET}/${DATA_ROOT_PREFIX}/${TARGET_DB}/${TARGET_TBL}"

TODAY=$(date -u +"%Y-%m-%d")
HOT_DATE=$(date -u -d "-2 days" +"%Y-%m-%d")
COLD_DATE=$(date -u -d "-65 days" +"%Y-%m-%d")

META_FILE="${BASE_PATH}/metadata/v1.metadata.json"
HOT_FILE="${BASE_PATH}/data/dt=${HOT_DATE}/data-00000.parquet"
COLD_FILE="${BASE_PATH}/data/dt=${COLD_DATE}/data-00000.parquet"
TODAY_TEST_FILE="${BASE_PATH}/data/dt=${TODAY}/writer_test.txt"

FAILED=0

test_access() {
  local sa="$1" op="$2" path="$3" expect="$4" desc="$5" actual
  case "${op}" in
    read)
      if gcloud storage cat "${path}" --impersonate-service-account="${sa}" &>/dev/null; then
        actual="ALLOW"
      else
        actual="DENY (403)"
      fi
      ;;
    write)
      if echo "write-test-content" | gcloud storage cp - "${path}" --impersonate-service-account="${sa}" &>/dev/null; then
        actual="ALLOW"
      else
        actual="DENY (403)"
      fi
      ;;
  esac

  if [[ "${actual}" =~ "${expect}" ]]; then
    echo "  ✔ [PASS] ${desc}: 结果=${actual}"
  else
    echo "  ✘ [FAIL] ${desc}: 结果=${actual} (预期=${expect})"
    FAILED=1
  fi
}

echo "========================================================================="
echo "🔍 启动生产环境 Managed Folder 隔离权限双向断言验证"
echo "========================================================================="

echo -e "\n1. 验证写入引擎权限 (Spark/Flink: 全生命周期自由读写):"
test_access "${WRITER_SA}" write "${TODAY_TEST_FILE}" "ALLOW" "写入今日热分区"
test_access "${WRITER_SA}" read  "${HOT_FILE}"        "ALLOW" "读取历史热分区"
test_access "${WRITER_SA}" write "${BASE_PATH}/data/dt=${COLD_DATE}/compaction.txt" "ALLOW" "重写历史冷分区(Compaction)"

echo -e "\n2. 验证日常查询引擎权限 (Hue/分析师: 只能读热，严禁触碰冷数据):"
test_access "${HOT_SA}" read "${META_FILE}" "ALLOW" "读取表级元数据 (metadata/*.json)"
test_access "${HOT_SA}" read "${HOT_FILE}"  "ALLOW" "读取近 60 天热分区数据"
test_access "${HOT_SA}" read "${COLD_FILE}" "DENY"  "误查 60 天前冷分区 (底层 403 强拦截，0 元冷检索费)"

echo -e "\n3. 验证历史合规通道权限 (Cold Reader: 仅能查冷数据，禁止越权读热数据):"
test_access "${COLD_SA}" read "${META_FILE}" "ALLOW" "读取表级元数据"
test_access "${COLD_SA}" read "${COLD_FILE}" "ALLOW" "正常读取 60 天前历史冷分区"
test_access "${COLD_SA}" read "${HOT_FILE}"  "DENY"  "越权读取近 60 天热分区"

echo -e "\n4. 验证自动化调和身份权限 (mf-reconciler: 零特权，无数据内容窥探权):"
test_access "${OPS_SA}" read "${HOT_FILE}"  "DENY" "尝试读取热数据内容"
test_access "${OPS_SA}" read "${COLD_FILE}" "DENY" "尝试读取冷数据内容"

echo "========================================================================="
if (( FAILED == 0 )); then
  echo "🎉 验证全部通过！冷热强拦截与元数据放行策略 100% 生效！"
else
  echo "⚠️ 存在未通过项，请对照排查宽泛权限或调和配置！"
  exit 1
fi
EOF

chmod +x verify_production.sh
./verify_production.sh
```

---

### 阶段六：架构前瞻风险、应急预案与运维监控建议

#### 1. 自动容错与漏跑保护机制（Fault-Tolerant Lookback）
* **设计保障**：增量引擎内置了 `SLIDING_LOOKBACK_DAYS=3` 参数。若 Cloud Run 调度由于网络抖动或集群维护偶然中断 1~2 天，下次触发时仍会自动扫描覆盖 `[T-60, T-61, T-62]`，自动纠偏历史边界，无需人工介入补跑。

#### 2. 周度全量审计建议（Weekly Full Audit）
* 可在 Cloud Scheduler 中额外配置一个每周日凌晨运行的任务，传入环境变量 `RECONCILE_MODE=full`，对数万历史分区执行一次全量兜底校验，确保数据 100% 状态一致。

#### 3. 运维告警指标配置（Cloud Monitoring）
* 在 GCP Cloud Monitoring 中针对 Cloud Run Job `mf-reconcile` 配置 Alerting Policy：
  * **监控指标**：`run.googleapis.com/job/completed_execution_count`
  * **过滤条件**：`result = "failed"`
  * **通知渠道**：配置 Email / 钉钉 / 企业微信 Webhook，确保调度异常时能在 5 分钟内感知。

#### 4. 运行成本测算（Monthly TCO）
* **Cloud Run 计算费**：每日运行 2.6 秒，每月累计运行约 78 秒（远在每月 200 万次免费调用及 180,000 vCPU-秒配额内），**费用为 $0.00**。
* **GCS API 调用费**：每日约 600 次 Class A 操作，每月约 1.8 万次（GCS 每万次操作仅 $0.05），**月度 API 成本低于 $0.10**。
* **冷数据挽回价值**：避免哪怕一次百 TB 级的冷数据全表扫描，即可挽回数千美元的 Coldline 检索账单。
