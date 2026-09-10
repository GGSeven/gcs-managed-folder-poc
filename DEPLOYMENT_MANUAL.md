# GCS Managed Folder 热/冷数据分层与权限隔离方案：生产交付与部署手册

> **文档适用对象**：客户大数据平台架构师、SRE/DevOps 运维工程师、云安全管理员  
> **方案版本**：v2.0 (High-Performance REST Reconcile Engine)  
> **服务商**：Baidaodata 解决方案架构团队  

---

## 1. 方案背景与核心收益

客户大数据存储于 Google Cloud Storage (GCS) 上的 Iceberg 格式数仓，存储策略为 **60 天自动转入 Coldline 存储**。由于传统 SQL 引擎（如 Trino / Impala / Spark SQL）与交互式工具（Hue）由分析师或运维直接使用，曾发生由于未带分区过滤的全表扫描，误查数月至数年前的历史冷数据，触发高额的 **Coldline 冷数据检索费（Data Retrieval Fee）**。

### 核心收益与隔离目标
1. **0 元误查保底拦截**：分析人员或日常查询任务若误触 60 天前的历史冷数据，在 GCS 存储底层直接抛出 **`HTTP 403 Forbidden`** 强拦截，**冷数据检索量为 0，检索费用为 0**。
2. **读写管道完全解耦**：Spark / Flink 等写入引擎具备全库、全表、全生命周期读写权限，不受热冷流转状态影响。
3. **Iceberg 元数据透明放行**：查询引擎必须能够随时读取 Iceberg 的 `metadata/*.json`，以正确规划快照和推断分区，无论数据分区是否冷冻。
4. **极速自动化调和**：基于 Cloud Run Job Serverless 架构，内网多线程调和引擎在 **10,000+ 分区**场景下，调和耗时仅需 **68 秒**（吞吐率达 **147.5 分区/秒**），每日定时自动滚动。
5. **绝对数据安全（零特权原则）**：调和引擎仅具备 GCS 控制面管理权限，**无任何数据读取权限（无 `storage.objects.get`）**，杜绝运维工具窃取业务数据的合规风险。

---

## 2. 核心架构与权限模型

### 2.1 目录结构标准规范
```text
gs://<BUCKET>/<DATA_ROOT_PREFIX>/
  ├── <db_name>.db/
  │    └── <table_name>/
  │         ├── metadata/                         <-- 表级 Managed Folder (Hot/Cold Reader 均放行)
  │         │    └── v1.metadata.json
  │         └── data/
  │              ├── dt=2026-09-10/ (近60天热数据)  <-- 天级 Managed Folder (仅绑定 Hot Reader)
  │              │    └── data-00000.parquet
  │              └── dt=2026-04-12/ (60天前冷数据)  <-- 天级 Managed Folder (仅绑定 Cold Reader)
  │                   └── data-00000.parquet
```

### 2.2 服务账号（Service Accounts）矩阵

| 账号代号 | 账号命名示例 | 授权范围与角色 | 业务场景说明 |
| :--- | :--- | :--- | :--- |
| **Writer SA** | `iceberg-writer@<PROJECT>.iam.gserviceaccount.com` | `datasets/` 根 Managed Folder: `roles/storage.objectUser` | Spark/Flink 写入管道，全生命周期可读写删 |
| **Hot Reader SA** | `iceberg-hot-reader@<PROJECT>.iam.gserviceaccount.com` | `metadata/`: `objectViewer`<br>`dt < 60d`: `objectViewer` | Hue/日常报表/交互式查询引擎日常使用身份 |
| **Cold Reader SA** | `iceberg-cold-reader@<PROJECT>.iam.gserviceaccount.com` | `metadata/`: `objectViewer`<br>`dt >= 60d`: `objectViewer` | 历史归档数据调阅/审计专用通道（需审批使用） |
| **Reconciler SA** | `mf-reconciler@<PROJECT>.iam.gserviceaccount.com` | 自定义角色 `mfReconciler` (仅控制面，无对象读权限) | Cloud Run Job 调和引擎执行身份 |

---

## 3. 前置准备与环境检查

客户在执行部署前，请确保满足以下条件：

1. **项目权限**：当前部署人员需要具备该 GCP 项目的 `roles/owner` 或 `roles/editor` + `roles/iam.securityAdmin`。
2. **启用必要的 GCP API 服务**：
   ```bash
   gcloud services enable \
     storage.googleapis.com \
     run.googleapis.com \
     cloudbuild.googleapis.com \
     artifactregistry.googleapis.com \
     cloudscheduler.googleapis.com \
     iam.googleapis.com \
     --project="<YOUR_PROJECT_ID>"
   ```
3. **UBLA（统一存储桶级访问权限）硬性要求**：
   * GCS Managed Folder 依赖统一存储桶级访问权限。必须确保目标存储桶已开启 UBLA：
   ```bash
   gcloud storage buckets update gs://<YOUR_BUCKET_NAME> --uniform-bucket-level-access
   ```

---

## 4. 完整部署交付操作步骤

以下操作同时提供 **GCP Web 控制台界面路径** 与 **标准自动化命令**。

### 步骤一：创建自定义最小权限角色（`mfReconciler`）

该角色用于调度引擎，**严格遵循零信任权限原则**，剥离了数据读取权限 `storage.objects.get`。

* **控制台路径**：`IAM & 管理` -> `角色 (Roles)` -> `+ 创建角色`
  * 标题：`Managed Folder Reconciler`
  * 角色 ID：`mfReconciler`
  * 角色发布阶段：正式版 (GA)
  * 添加权限：
    - `storage.managedFolders.create`
    - `storage.managedFolders.get`
    - `storage.managedFolders.list`
    - `storage.managedFolders.getIamPolicy`
    - `storage.managedFolders.setIamPolicy`
    - `storage.objects.list`
    - `storage.buckets.get`
* **gcloud CLI 命令**：
  ```bash
  gcloud iam roles create mfReconciler \
    --project="<YOUR_PROJECT_ID>" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
  ```

---

### 步骤二：创建四大专用服务账号（Service Accounts）

* **控制台路径**：`IAM & 管理` -> `服务账号 (Service Accounts)` -> `+ 创建服务账号`
* **gcloud CLI 命令**：
  ```bash
  PROJECT_ID="<YOUR_PROJECT_ID>"
  for sa in "iceberg-writer" "iceberg-hot-reader" "iceberg-cold-reader" "mf-reconciler"; do
    gcloud iam service-accounts create "${sa}" \
      --project="${PROJECT_ID}" \
      --display-name="SA for ${sa}"
  done
  ```

---

### 步骤三：初始化存储桶根级 Managed Folder 与权限注入

在数据根目录（如 `gs://<YOUR_BUCKET>/datasets/`）创建根 Managed Folder，将写入方与运维方权限圈定在根目录下，杜绝权限扩散至存储桶外其他文件。

* **gcloud CLI 命令**：
  ```bash
  BUCKET="gs://<YOUR_BUCKET_NAME>"
  DATA_PREFIX="datasets"

  # 1. 创建根级 Managed Folder
  gcloud storage managed-folders create "${BUCKET}/${DATA_PREFIX}/"

  # 2. 下发写入方与调和方的基础权限
  cat <<EOF > /tmp/root_policy.json
  {
    "bindings": [
      {
        "role": "roles/storage.objectUser",
        "members": ["serviceAccount:iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com"]
      },
      {
        "role": "projects/${PROJECT_ID}/roles/mfReconciler",
        "members": ["serviceAccount:mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com"]
      }
    ]
  }
  EOF

  gcloud storage managed-folders set-iam-policy "${BUCKET}/${DATA_PREFIX}/" /tmp/root_policy.json
  ```

---

### 步骤四：创建 Artifact Registry 仓库并构建推送镜像

* **控制台路径**：`Artifact Registry` -> `代码库 (Repositories)` -> `+ 创建代码库`（格式: Docker，区域: 生产所在区域，如 `us-central1`）
* **gcloud CLI 命令**：
  ```bash
  REGION="<YOUR_REGION>" # 例如 us-central1
  REPO_NAME="mf-poc"
  IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

  # 1. 创建 Docker 仓库（若不存在）
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" || true

  # 2. 使用 Cloud Build 自动构建镜像
  gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" .
  ```

---

### 步骤五：部署 Cloud Run Job（计算型调和引擎）

* **控制台路径**：`Cloud Run` -> `任务 (Jobs)` -> `+ 创建任务 (Create Job)`
  * 容器映像网址：`${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest`
  * 区域：与 GCS 存储桶同区域（同机房内网延迟 <1ms）
  * 资源规格：2 vCPU / 1 GiB 内存
  * 任务超时：1 小时，最大重试次数：1
  * 服务账号：选择 `mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com`
  * 环境变量（Environment Variables）：
    * `PROJECT_ID`: `<YOUR_PROJECT_ID>`
    * `REGION`: `<YOUR_REGION>`
    * `BUCKET`: `gs://<YOUR_BUCKET_NAME>`
    * `DATA_ROOT_PREFIX`: `datasets`
    * `HOT_DAYS`: `60`
    * `HOT_SA`: `iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com`
    * `COLD_SA`: `iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com`
    * `MAX_WORKERS`: `30`
    * `EXCLUDE_DBS`: `tmp.db,kafka_test.db`
* **gcloud CLI 命令**：
  ```bash
  gcloud run jobs deploy mf-reconcile \
    --image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --service-account="mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com" \
    --cpu=2 \
    --memory=1Gi \
    --max-retries=1 \
    --task-timeout=1h \
    --set-env-vars="PROJECT_ID=${PROJECT_ID},REGION=${REGION},BUCKET=${BUCKET},DATA_ROOT_PREFIX=datasets,HOT_DAYS=60,HOT_SA=iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com,COLD_SA=iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com,MAX_WORKERS=30,EXCLUDE_DBS=tmp.db\,kafka_test.db"
  ```

---

### 步骤六：配置 Cloud Scheduler 每日定时触发

* **控制台路径**：`Cloud Scheduler` -> `创建作业`
  * 名称：`mf-reconcile-daily`
  * 频率：`30 0 * * *`（每天凌晨 00:30 UTC / 对应北京时间 08:30）
  * 目标类型：`HTTP`
  * 网址：`https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/mf-reconcile:run`
  * HTTP 方法：`POST`
  * Auth 标头：`添加 OAuth 令牌`
  * 服务账号：选择具备 `roles/run.invoker` 权限的服务账号
* **gcloud CLI 命令**：
  ```bash
  # 授予 reconciler 触发自身调度的权限（或使用调度专用 SA）
  gcloud run jobs add-iam-policy-binding mf-reconcile \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --member="serviceAccount:mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/run.invoker"

  # 创建每日定时触发任务
  gcloud scheduler jobs create http mf-reconcile-daily \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" \
    --time-zone="Etc/UTC" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/mf-reconcile:run" \
    --http-method=POST \
    --oauth-service-account-email="mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com"
  ```

---

## 5. 验收与权限验证方案

部署完成后，在客户端机器（拥有 `roles/iam.serviceAccountTokenCreator` 权限以进行 impersonation 模拟）运行以下验证命令：

### 5.1 验证日常热查询（模拟 Hue / 分析师）
```bash
HOT_SA="iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com"

# 1. 验证元数据读取：预期 HTTP 200 (ALLOW)
gcloud storage cat gs://${BUCKET_NAME}/datasets/<db>.db/<tbl>/metadata/v1.metadata.json \
  --impersonate-service-account="${HOT_SA}"

# 2. 验证近 60 天热分区读取：预期 HTTP 200 (ALLOW)
gcloud storage cat gs://${BUCKET_NAME}/datasets/<db>.db/<tbl>/data/dt=2026-09-08/data-00000.parquet \
  --impersonate-service-account="${HOT_SA}"

# 3. 验证误查 60 天前冷分区读取：预期 HTTP 403 Forbidden (DENY，0 元检索费)
gcloud storage cat gs://${BUCKET_NAME}/datasets/<db>.db/<tbl>/data/dt=2026-04-12/data-00000.parquet \
  --impersonate-service-account="${HOT_SA}"
```

### 5.2 验证写入方读写自由（模拟 Spark / Flink）
```bash
WRITER_SA="iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com"

# 1. 写入热分区：预期 ALLOW
echo "test" | gcloud storage cp - gs://${BUCKET_NAME}/datasets/<db>.db/<tbl>/data/dt=2026-09-08/test.txt \
  --impersonate-service-account="${WRITER_SA}"

# 2. 重写历史冷分区（Compaction 场景）：预期 ALLOW
echo "test" | gcloud storage cp - gs://${BUCKET_NAME}/datasets/<db>.db/<tbl>/data/dt=2026-04-12/test.txt \
  --impersonate-service-account="${WRITER_SA}"
```

---

## 6. 架构师前瞻性风险与运维避坑指南

### ⚠️ 陷阱一：Bucket 级 / Project 级权限污染（Additive IAM 叠加风险）
* **风险表现**：GCS Managed Folder 的 IAM 鉴权为**纯叠加（Additive）模式**，不支持显式 Deny。如果用户或 SA 在**Bucket 级别**或**Project 级别**被授予了 `roles/storage.objectViewer`，该权限会自动穿透所有 Managed Folder，导致冷数据隔离完全失效！
* **防范方案**：审计脚本必须确保 `iceberg-hot-reader` 在 Bucket 级没有任何 `storage.objects.*` 绑定，其权限只能逐层下发到 Managed Folder 上。

### ⚠️ 陷阱二：跨日作业写入阻断（Midnight Write Window Gap）
* **风险表现**：如果定时任务在每天 00:30 执行，而 Spark/Flink 在 00:01 开始写入今天新分区 `dt=TODAY`，此时如果该目录尚未打上 Managed Folder 标签，写入是否会受阻？
* **防范方案**：
  1. 本调和引擎具备**预创建机制**：每次运行都会自动为全量表预创建并绑定今天（`offset=0`）与明天（`offset=1`）两个分区。
  2. 即使未预创建，由于 `iceberg-writer` 在根目录 `datasets/` 上已持有 `roles/storage.objectUser`，具备向任意新子路径写入对象的权限。

### ⚠️ 陷阱三：命令行工具与 SDK 鉴权陷阱（CLI objects.get 403）
* **风险表现**：在纯容器中若调用 `gcloud storage managed-folders set-iam-policy`，CLI 会额外发起 `objects.get` 检查目录对象是否存在。若运维 SA 采用最小权限，CLI 会抛出 403。
* **防范方案**：交付镜像必须锁定为本方案提供的 Python 原生 REST 引擎（`reconcile-fast`），直接对接 GCS 控制面 API，规避 CLI 的非必要探测。

### ⚠️ 陷阱四：大规模分区 QPS 限流与网络优化
* **指标数据**：万级分区在跨公网调用时极易发生 Winsock 10053 重置或耗时过长；而在 Cloud Run 与 GCS 同一区域（`us-central1`）内网调用时，单核吞吐可达 **140~150 分区/秒**。
* **最佳实践**：
  * Cloud Run Job 必须与 GCS 部署在**同一 GCP Region**。
  * 并发线程建议设为 `20 ~ 30`。避免超过 50，以符合 GCP GCS 控制面单个资源建议的并发速率阈值。

---

## 7. 客户实施前调研清单（Customer Discovery Questions）

在客户实施前，架构师请与客户确认以下关键要素：

1. **现有数仓目录与分区命名规则**：
   * 是否全量表均严格遵循 `datasets/<db>.db/<table_name>/data/dt=YYYY-MM-DD/` 规范？是否有二级分区（如 `dt=YYYY-MM-DD/hh=XX/`）？
2. **读写管道身份对接**：
   * 目前生产集群的 Spark/Flink 任务是以何种凭据（Service Account Key、Workload Identity、还是 Compute Engine 默认 SA）挂载访问 GCS 的？
   * Hue / Trino / Impala 集群查询引擎是以何种凭据代理用户访问 GCS 的？
3. **冷热阈值与数据保留周期**：
   * 业务侧要求的冷热分界线是否统一为 60 天？是否有部分特殊报表表需要保留 90 天或 180 天？
   * 是否已在 GCS Bucket 上配置匹配的 GCS Lifecycle 规则（例如 60 天转 Coldline）？
