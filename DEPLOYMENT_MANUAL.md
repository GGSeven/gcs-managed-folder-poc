# GCS Managed Folder 热/冷数据分层与权限隔离方案：生产交付与部署手册

> **文档适用对象**：客户大数据平台架构师、SRE/DevOps 运维工程师、云安全管理员  
> **方案版本**：v2.1 (Dual-Mode Incremental Sliding-Window & Zero-Trust Reconcile Engine)  
> **服务商**：Baidaodata 解决方案架构团队  
> **Git 仓库**：https://github.com/GGSeven/gcs-managed-folder-poc.git  

---

## 1. 方案背景与核心收益

客户大数据存储于 Google Cloud Storage (GCS) 上的 Iceberg 格式数仓，存储策略为 **60 天自动转入 Coldline 存储**。由于传统 SQL 引擎（如 Trino / Impala / Spark SQL）与交互式工具（Hue）由分析师或运维直接使用，曾发生由于未带分区过滤的全表扫描，误查数月至数年前的历史冷数据，触发高额的 **Coldline 冷数据检索费（Data Retrieval Fee）**。

### 核心收益与隔离目标
1. **0 元误查保底拦截**：分析人员或日常查询任务若误触 60 天前的历史冷数据，在 GCS 存储底层直接抛出 **`HTTP 403 Forbidden`** 强拦截，**冷数据检索量为 0，检索费用为 0**。
2. **读写管道完全解耦**：Spark / Flink 等写入引擎具备全库、全表、全生命周期读写权限，不受热冷流转状态影响。
3. **Iceberg 元数据透明放行**：查询引擎必须能够随时读取 Iceberg 的 `metadata/*.json`，以正确规划快照和推断分区，无论数据分区是否冷冻。
4. **秒级增量自动化调和**：基于 Cloud Run Job Serverless 架构，采用**增量滑动窗口机制**，54 张表日度调和仅需 **2.36 秒**（全量 10,041 个分区调和仅需 **68 秒**），每日定时自动滚动。
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
  │              └── dt=2026-07-12/ (60天前冷数据)  <-- 天级 Managed Folder (仅绑定 Cold Reader)
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

## 3. 架构师自测验证方案（Pre-flight Self-Verification）

> 在正式向客户交付前，架构师可按照以下步骤在测试环境完整执行自检与数据断言。

### 3.1 环境变量预设
```bash
export PROJECT_ID="bd-host-2026-004"
export REGION="us-central1"
export BUCKET_NAME="bd-host-2026-004-mf-poc"
export BUCKET="gs://${BUCKET_NAME}"
export DATA_ROOT_PREFIX="datasets"
export HOT_SA="iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com"
export COLD_SA="iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com"
export WRITER_SA="iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com"
export RECONCILER_SA="mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com"
```

### 3.2 自检执行清单
1. **生成 10,000+ 分区模拟数据**：
   ```bash
   python generate_mock_partitions.py
   ```
2. **执行全量调和压测**：
   ```bash
   gcloud builds submit --config=bench.yaml --project="${PROJECT_ID}" .
   # 验证输出指标：10,041 分区，核心耗时约 68 秒，吞吐率 > 130 分区/秒
   ```
3. **执行增量调和压测**：
   ```bash
   gcloud builds submit --config=bench_inc.yaml --project="${PROJECT_ID}" .
   # 验证输出指标：54 张表滑动窗口 200~300 分区，耗时约 2~8 秒，吞吐率 > 100 操作/秒
   ```
4. **权限断言自动化校验**：
   ```bash
   bash verify.sh
   # 预期断言结果：
   # [PASS] Writer SA 写入热分区成功
   # [PASS] Writer SA 写入冷分区成功
   # [PASS] Hot Reader 读取 metadata/ 成功 (200)
   # [PASS] Hot Reader 读取近 60 天热分区成功 (200)
   # [PASS] Hot Reader 读取 60 天前冷分区强拦截 (403 Forbidden, 0 元检索费)
   # [PASS] Cold Reader 读取 60 天前冷分区成功 (200)
   # [PASS] Reconciler SA 尝试读取数据对象被拒绝 (403 零特权安全合规)
   ```

---

## 4. 客户生产环境部署方案（Customer Deployment Guide）

### 步骤一：前置环境与存储桶 UBLA 检查

1. **启用必要 GCP API**：
   ```bash
   gcloud services enable \
     storage.googleapis.com \
     run.googleapis.com \
     cloudbuild.googleapis.com \
     artifactregistry.googleapis.com \
     cloudscheduler.googleapis.com \
     iam.googleapis.com \
     --project="<CUSTOMER_PROJECT_ID>"
   ```
2. **开启存储桶统一访问控制（UBLA，强制要求）**：
   * **控制台路径**：`Cloud Storage` -> 选择目标存储桶 -> `权限` 标签页 -> 将“访问权限控制”切换为 **统一 (Uniform)**。
   * **gcloud CLI 命令**：
     ```bash
     gcloud storage buckets update gs://<CUSTOMER_BUCKET_NAME> --uniform-bucket-level-access
     ```

---

### 步骤二：创建自定义最小权限角色（`mfReconciler`）

严格遵循零信任原则，剥离业务数据读取权限 `storage.objects.get`。

* **控制台路径**：`IAM & 管理` -> `角色 (Roles)` -> `+ 创建角色`
  * 标题：`Managed Folder Reconciler`，角色 ID：`mfReconciler`
  * 权限列表：
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
    --project="<CUSTOMER_PROJECT_ID>" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
  ```

---

### 步骤三：创建四大专用服务账号

* **控制台路径**：`IAM & 管理` -> `服务账号 (Service Accounts)` -> `+ 创建服务账号`
* **gcloud CLI 命令**：
  ```bash
  PROJECT_ID="<CUSTOMER_PROJECT_ID>"
  for sa in "iceberg-writer" "iceberg-hot-reader" "iceberg-cold-reader" "mf-reconciler"; do
    gcloud iam service-accounts create "${sa}" \
      --project="${PROJECT_ID}" \
      --display-name="SA for ${sa}"
  done
  ```

---

### 步骤四：初始化数据根目录 Managed Folder

在数据根目录（如 `gs://<BUCKET>/datasets/`）创建根 Managed Folder，圈定写入方与运维方范围。

* **gcloud CLI 命令**：
  ```bash
  BUCKET="gs://<CUSTOMER_BUCKET_NAME>"
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

### 步骤五：构建并部署 Cloud Run Job 调和引擎

1. **创建 Artifact Registry 仓库并构建镜像**：
   ```bash
   REGION="<CUSTOMER_REGION>" # 例如 us-central1
   REPO_NAME="mf-reconciler"
   IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

   gcloud artifacts repositories create "${REPO_NAME}" \
     --repository-format=docker \
     --location="${REGION}" \
     --project="${PROJECT_ID}" || true

   gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" .
   ```

2. **部署 Cloud Run Job（计算型）**：
   * **控制台路径**：`Cloud Run` -> `任务 (Jobs)` -> `+ 创建任务 (Create Job)`
     * 容器映像：`${IMAGE}`
     * 区域：与 GCS 存储桶保持同一 Region
     * 规格：2 vCPU / 1 GiB 内存
     * 服务账号：`mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com`
     * 超时：10 分钟，最大重试次数：0
     * 环境变量配置：
       - `PROJECT_ID`: `<CUSTOMER_PROJECT_ID>`
       - `REGION`: `<CUSTOMER_REGION>`
       - `BUCKET`: `gs://<CUSTOMER_BUCKET_NAME>`
       - `DATA_ROOT_PREFIX`: `datasets`
       - `HOT_DAYS`: `60`
       - `RECONCILE_MODE`: `incremental` (生产默认增量模式)
       - `SLIDING_LOOKBACK_DAYS`: `3`
       - `MAX_WORKERS`: `30`
       - `EXCLUDE_DBS`: `tmp.db,kafka_test.db`
   * **gcloud CLI 命令**：
     ```bash
     gcloud run jobs deploy mf-reconcile \
       --image="${IMAGE}" \
       --region="${REGION}" \
       --project="${PROJECT_ID}" \
       --service-account="mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com" \
       --cpu=2 \
       --memory=1Gi \
       --max-retries=0 \
       --task-timeout=10m \
       --env-vars-file="env_vars.yaml"
     ```

---

### 步骤六：配置双模自动调度策略（Cloud Scheduler）

1. **每日增量调和任务（默认高频，执行约 2~3 秒）**：
   * **控制台路径**：`Cloud Scheduler` -> `创建作业`
     * 名称：`mf-reconcile-daily`
     * 频率：`05 0 * * *`（每天凌晨 00:05 UTC / 北京时间 08:05）
     * 目标：`HTTP`，方法：`POST`
     * 网址：`https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/mf-reconcile:run`
     * Auth 标头：`添加 OAuth 令牌`，服务账号：`mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com`
   * **CLI 命令**：
     ```bash
     gcloud run jobs add-iam-policy-binding mf-reconcile \
       --region="${REGION}" \
       --project="${PROJECT_ID}" \
       --member="serviceAccount:mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com" \
       --role="roles/run.invoker"

     gcloud scheduler jobs create http mf-reconcile-daily \
       --location="${REGION}" \
       --project="${PROJECT_ID}" \
       --schedule="05 0 * * *" \
       --time-zone="Etc/UTC" \
       --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/mf-reconcile:run" \
       --http-method=POST \
       --oauth-service-account-email="mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com"
     ```

2. **周度全量巡检兜底任务（每周日执行一次，全库审计）**：
   ```bash
   gcloud scheduler jobs create http mf-reconcile-weekly-full \
     --location="${REGION}" \
     --project="${PROJECT_ID}" \
     --schedule="00 1 * * 0" \
     --time-zone="Etc/UTC" \
     --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/mf-reconcile:run" \
     --http-method=POST \
     --message-body='{"overrides":{"containerOverrides":[{"env":[{"name":"RECONCILE_MODE","value":"full"}]}]}}' \
     --oauth-service-account-email="mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com"
   ```

---

## 5. 客户侧验收测试用例与标准（Acceptance Criteria）

| 用例编号 | 测试场景 | 测试操作（使用 impersonation 模拟） | 预期结果 |
| :--- | :--- | :--- | :--- |
| **TC-01** | **写入管道不受限** | 使用 `iceberg-writer` 向任意热/冷分区写入测试文件 | **成功写入 (HTTP 200)** |
| **TC-02** | **元数据常开** | 使用 `iceberg-hot-reader` 读取某表 `metadata/*.json` | **成功读取 (HTTP 200)** |
| **TC-03** | **日常近60天热查询** | 使用 `iceberg-hot-reader` 读取 `dt=TODAY` 分区文件 | **成功读取 (HTTP 200)** |
| **TC-04** | **误查 60 天冷数据拦截** | 使用 `iceberg-hot-reader` 读取 60 天前冷分区文件 | **抛出 403 Forbidden，检索费为 0** |
| **TC-05** | **审计专用冷通道** | 使用 `iceberg-cold-reader` 读取 60 天前冷分区文件 | **成功读取 (HTTP 200)** |
| **TC-06** | **调和引擎零特权** | 使用 `mf-reconciler` 尝试直接读取任意业务数据 | **抛出 403 Forbidden（无数据访问权）** |
| **TC-07** | **增量性能验收** | 触发 Cloud Run 增量任务执行 | **耗时 < 10 秒，100% 成功退出** |

---

## 6. 架构师前瞻性风险与运维避坑指南 (Architect Proactivity)

### ⚠️ 风险 1：存储桶或项目级权限穿透（Additive IAM 叠加陷阱）
* **风险描述**：GCS Managed Folder 是纯累加鉴权机制，不支持 Deny 策略。若客户管理员误将 `roles/storage.objectViewer` 赋予在 **Bucket 级别** 或 **Project 级别**，该权限会自动穿透至所有底层分区，冷数据 403 强拦截将直接失效！
* **缓解方案**：
  1. 交付验收时运行审计脚本，确保 Bucket 级 IAM 列表中不存在 `iceberg-hot-reader`。
  2. 在客户组织策略（Organization Policy）中配置 IAM 最小授权审查，或使用 Cloud Asset Inventory 设置 Bucket 级越权告警。

### ⚠️ 风险 2：跨零点 ETL 写入延迟与目录预热（Midnight Gap）
* **风险描述**：若 Spark/Flink 在 00:01 开始写入今天新分区，而调度调和任务在 00:05 执行，这 4 分钟内新分区尚未配置 Managed Folder，写入是否报错？
* **缓解方案**：
  1. 引擎内置**T+1 预建机制**：前一天调和时已提前将明天的 Managed Folder 预建并授权。
  2. 写入方 `iceberg-writer` 在根目录 `datasets/` 上具备 `roles/storage.objectUser`，即便子目录尚未创建 Managed Folder，写入管道也拥有继承权，绝不会阻断 ETL 生产管道。

### ⚠️ 风险 3：大规模分区 QPS 限流与网络延迟
* **风险描述**：跨地域或公网调用 GCS 控制面 API 会产生 200ms+ RTT 延迟且极易受网络波动影响。
* **缓解方案**：
  1. Cloud Run Job 必须部署在与 GCS 存储桶相同的 Region（同可用区/机房内网调用延迟 < 1ms）。
  2. 引擎内部设置最大并发为 30，既达到 140+ 分区/秒的高吞吐，又安全处于 GCP GCS 控制面 QPS 配额范围内。

---

## 7. 客户实施前调研清单 (Customer Discovery Questions)

在向客户正式推行本方案前，请务必与客户技术负责人澄清以下事项：

1. **目录规范一致性**：
   - 生产环境中各库表是否严格遵循 `datasets/<db>.db/<tbl>/data/dt=YYYY-MM-DD/`？
   - 是否存在二级分区（如 `dt=.../hour=...`）？若有，Managed Folder 仅需挂载在天级 `dt=.../` 即可。
2. **计算引擎凭据身份对接**：
   - 写入管道（Spark/Flink）目前是通过 Workload Identity 还是 SA 静态密钥访问 GCS？
   - 查询引擎（Hue/Trino/Impala）是以固定 Service Account 访问 GCS，还是开启了用户身份模拟（Impersonation）？
3. **冷热转换与生命周期规则对齐**：
   - 业务部门认定的冷数据分水岭是否统一为 60 天？是否有某些核心维度表要求 180 天或永久为热？
   - 存储桶现有的 GCS Lifecycle 规则是否已配置为 60 天转换到 Coldline 存储类？（必须确保 IAM 隔离周期与存储转冷周期严格对齐）。
