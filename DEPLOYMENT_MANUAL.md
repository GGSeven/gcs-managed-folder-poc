# GCS Managed Folder 热/冷数据分层与权限隔离方案：生产交付与部署手册

> **文档适用对象**：客户大数据平台架构师、SRE/DevOps 运维工程师、云安全管理员、百道数据交付架构师  
> **方案版本**：v2.0 (High-Performance REST Reconcile Engine & Serverless Cloud Run)  
> **服务商**：Baidaodata 解决方案架构团队  

---

## 目录
1. [方案背景与业务价值](#1-方案背景与业务价值)
2. [执行前置条件与同级文件清单（必读）](#2-执行前置条件与同级文件清单必读)
3. [参数配置文件说明（config.env）](#3-参数配置文件说明configenv)
4. [一键全自动化部署指南（推荐：5 分钟落地）](#4-一键全自动化部署指南推荐5-分钟落地)
5. [底层服务与 GCP 控制台可视化核验（按需查看）](#5-底层服务与-gcp-控制台可视化核验按需查看)
6. [权限断言与客户验收测试](#6-权限断言与客户验收测试)
7. [架构师深度避坑指南与细节原理解析](#7-架构师深度避坑指南与细节原理解析)
8. [运维监控与应急回滚预案](#8-运维监控与应急回滚预案)

---

## 1. 方案背景与业务价值

客户大数据存储于 Google Cloud Storage (GCS) 上的 Iceberg 格式数仓，存储策略为 **60 天自动转入 Coldline 存储**。由于传统 SQL 引擎（如 Trino / Impala / Spark SQL）与交互式工具（Hue）由分析师或运维直接使用，曾发生由于未带分区过滤的全表扫描，误查数月至数年前的历史冷数据，触发高额的 **Coldline 冷数据检索费（Data Retrieval Fee）**。

### 核心收益与隔离目标
1. **0 元误查保底拦截**：分析人员或日常查询任务若误触 60 天前的历史冷数据，在 GCS 存储底层直接抛出 **`HTTP 403 Forbidden`** 强拦截，**冷数据检索量为 0，检索费用为 0**。
2. **读写管道完全解耦**：Spark / Flink 等写入引擎具备全库、全表、全生命周期读写权限，不受热冷流转状态影响。
3. **Iceberg 元数据透明放行**：查询引擎能够随时读取各表 `metadata/*.json`，以正确规划快照和推断分区，无论数据分区是否冷冻。
4. **极速自动化调和**：基于 Cloud Run Job Serverless 架构，内网多线程调和引擎在 **10,000+ 分区**场景下，调和耗时仅需 **68 秒**（吞吐率达 **147.5 分区/秒**），每日定时自动滚动。
5. **绝对数据安全（零特权原则）**：调和引擎仅具备 GCS 控制面管理权限，**无任何数据读取权限（无 `storage.objects.get`）**，杜绝运维工具窃取业务数据的合规风险。

---

## 2. 执行前置条件与同级文件清单（必读）

### 2.1 依赖环境与权限前置
在执行部署前，部署人员的客户端机器需要满足：
1. **安装 Google Cloud SDK (`gcloud`)**：已安装且已完成认证登录：
   ```bash
   gcloud auth login
   gcloud config set project <YOUR_PROJECT_ID>
   ```
2. **账号 IAM 权限**：当前操作人员账号须具备该 GCP 项目的 **`roles/owner`**，或者拥有以下复合权限：
   * `roles/resourcemanager.projectIamAdmin`（创建角色与绑定 IAM）
   * `roles/iam.serviceAccountAdmin`（创建服务账号）
   * `roles/run.admin`（部署 Cloud Run Job）
   * `roles/cloudbuild.builds.editor`（构建容器镜像）
   * `roles/cloudscheduler.admin`（配置定时任务）
   * `roles/storage.admin`（管理存储桶与 Managed Folder）
3. **存储桶 UBLA（Uniform Bucket-Level Access）**：
   * Managed Folder **强制要求**存储桶开启统一存储桶级访问权限。如果未开启，部署脚本会自动尝试开启，亦可手动执行：
   ```bash
   gcloud storage buckets update gs://<YOUR_BUCKET_NAME> --uniform-bucket-level-access
   ```

### 2.2 部署目录同级文件结构清单
在解压或克隆代码仓库后，**执行一键部署脚本时，以下核心文件必须保存在同一级目录下**：

```text
managed-folder-poc/
├── deploy.sh               # ⭐【核心】一键全自动化生产部署脚本（可执行文件）
├── config.env              # ⭐【必配】环境参数定义文件（指定项目ID、Bucket、区域等）
├── Dockerfile              # ⭐【构建】轻量化 Python 3.11 容器镜像定义
├── reconcile_fast.py       # ⭐【代码】高性能多线程 GCS 原生 REST 调和引擎源码
├── verify.sh               # 🧪【测试】8 种访问组合权限断言验收测试脚本
├── DEPLOYMENT_MANUAL.md    # 📖【文档】本部署交付与运维手册
└── README.md               # 📖【文档】项目概述与架构概览
```

> **注意**：`deploy.sh` 会自动检测同级目录下是否存在 `config.env`、`Dockerfile`、`reconcile_fast.py`，若缺失任意一个将立即终止并提示。

---

## 3. 参数配置文件说明（config.env）

在执行部署前，**只需用文本编辑器修改 `config.env` 中的核心变量**以匹配客户实际环境：

```bash
# ------------------------------------------------------------------------------
# 1. GCP 基础环境配置（客户必填）
# ------------------------------------------------------------------------------
export PROJECT_ID="bd-host-2026-004"            # 客户的 GCP 项目 ID
export REGION="us-central1"                    # GCS 存储桶所在 Region（确保与 Cloud Run 同 Region）
export BUCKET="gs://bd-host-2026-004-mf-poc"   # 数仓所在的 GCS 目标存储桶完整路径
export DATA_ROOT_PREFIX="datasets"             # 数仓数据根目录前缀（桶下的根目录名，如 datasets 或 warehouse）

# ------------------------------------------------------------------------------
# 2. 隔离规则与天数阈值
# ------------------------------------------------------------------------------
export FOLDER_DATE_FORMAT="dt=%Y-%m-%d"        # 天级分区目录正则/前缀格式（例如 dt=YYYY-MM-DD）
export HOT_DAYS="60"                           # 热数据窗口天数阈值（距今 <60 天为热，>=60 天为冷）

# ------------------------------------------------------------------------------
# 3. 四大专用服务账号（Service Accounts）定义
# ------------------------------------------------------------------------------
export HOT_SA="iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com"   # 查询引擎/Hue日常使用的身份
export COLD_SA="iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com" # 审计/冷数据调阅专用身份
export WRITER_SA="iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com"     # Spark/Flink 写入管道身份
export OPS_SA="mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com"        # Cloud Run Job 自动化调和身份

# ------------------------------------------------------------------------------
# 4. 角色与最小特权定义
# ------------------------------------------------------------------------------
export HOT_ROLE="roles/storage.objectViewer"
export COLD_ROLE="roles/storage.objectViewer"
export WRITER_ROLE="roles/storage.objectUser"
export OPS_ROLE_ID="mfReconciler"              # 自动创建的自定义最小权限角色 ID
```

---

## 4. 一键全自动化部署指南（推荐：5 分钟落地）

为了避免在客户现场逐个手动在 GCP 控制台点击或分步执行出错，本项目封装了**全自动化、具备幂等性（重复执行安全）的 `deploy.sh` 脚本**。

### 步骤 4.1：为脚本授予执行权限并运行
在终端进入目录，直接执行：
```bash
chmod +x deploy.sh verify.sh
./deploy.sh
```

### 步骤 4.2：脚本自动化流转过程解析
脚本会自动按顺序执行以下 8 个阶段，无需人工介入：
1. **环境检查**：检查 `gcloud` 登录身份、检查同级依赖文件、确保存储桶已开启 UBLA。
2. **启用 API**：自动启用 `storage`, `run`, `cloudbuild`, `artifactregistry`, `cloudscheduler`, `iam` 等 GCP API。
3. **创建最小权限角色**：自动创建自定义角色 `mfReconciler`（严格移除了 `storage.objects.get`，确保调和过程碰不到业务数据）。
4. **创建四大专用 SA**：自动幂等创建 `iceberg-writer`、`iceberg-hot-reader`、`iceberg-cold-reader`、`mf-reconciler`。
5. **初始化数据根目录**：在 `gs://${BUCKET}/${DATA_ROOT_PREFIX}/` 创建根 Managed Folder，并注入写入方与调和方的基线权限。
6. **镜像构建与推送**：在 Artifact Registry 创建 `mf-poc` 仓库，使用 Cloud Build 将 `reconcile_fast.py` 打包构建为轻量容器镜像（`reconcile-fast:latest`）。
7. **部署 Cloud Run Job**：部署 `mf-reconcile` 任务（配置 2 vCPU / 1 GiB / 30 并发 Worker，超时 1 小时）。
8. **配置 Cloud Scheduler**：自动挂载每日 UTC 00:30（北京时间 08:30）自动运行的定时调度器。

### 步骤 4.3：部署完成后立即触发全量验证
部署脚本运行完毕后，建议立即手动触发一次执行，以确认全流程畅通并记录耗时：
```bash
gcloud run jobs execute mf-reconcile --region=us-central1 --project=${PROJECT_ID} --wait
```
* **控制台回显验证**：状态显示 `Execution [mf-reconcile-xxxx] has successfully completed` 且退出代码为 0。

---

## 5. 底层服务与 GCP 控制台可视化核验（按需查看）

如果客户安全或合规部门需要核验控制台中的配置，可按以下路径在 GCP Web Console 中查验：

### 5.1 核验自定义角色（`mfReconciler`）
* **路径**：`IAM & 管理` -> `角色` -> 搜索 `mfReconciler`。
* **权限清单**：
  * `storage.managedFolders.create`
  * `storage.managedFolders.get`
  * `storage.managedFolders.list`
  * `storage.managedFolders.getIamPolicy`
  * `storage.managedFolders.setIamPolicy`
  * `storage.objects.list`
  * `storage.buckets.get`
  * ❌ *确认不包含 `storage.objects.get`（杜绝数据泄露风险）*。

### 5.2 核验 Cloud Run Job
* **路径**：`Cloud Run` -> `任务 (Jobs)` -> 点击 `mf-reconcile`。
* **配置参数**：
  * 映像：`us-central1-docker.pkg.dev/<PROJECT_ID>/mf-poc/reconcile-fast:latest`
  * 资源：2 vCPU，1 GiB 内存
  * 服务账号：`mf-reconciler@<PROJECT_ID>.iam.gserviceaccount.com`
  * 并发 Worker：`MAX_WORKERS=30`

### 5.3 核验 Cloud Scheduler 定时调度器
* **路径**：`Cloud Scheduler` -> 点击 `mf-reconcile-daily`。
* **频率**：`30 0 * * *`（UTC），目标：HTTP POST 对应 Cloud Run Job API。

---

## 6. 权限断言与客户验收测试

现场向客户展示时，运行自带的 `./verify.sh` 或通过 `gcloud` 模拟身份（Impersonation）执行以下三组抽验命令：

```bash
# 获取临时配置
source ./config.env

# ------------------------------------------------------------------------------
# 验收 1：验证近 60 天热分区数据读取（日常分析师正常工作）
# ------------------------------------------------------------------------------
# 预期结果：HTTP 200 OK，正常返回数据流
gcloud storage cat gs://${BUCKET#gs://}/${DATA_ROOT_PREFIX}/<db>.db/<tbl>/data/dt=$(date -u -d '-5 days' +%Y-%m-%d)/data-00000.parquet \
  --impersonate-service-account="${HOT_SA}"

# ------------------------------------------------------------------------------
# 验收 2：验证 60 天前冷数据误查强拦截（防高额账单保底）
# ------------------------------------------------------------------------------
# 预期结果：HTTP 403 Forbidden 强拦截，读取字节数为 0，产生 0 元冷检索费
gcloud storage cat gs://${BUCKET#gs://}/${DATA_ROOT_PREFIX}/<db>.db/<tbl>/data/dt=$(date -u -d '-75 days' +%Y-%m-%d)/data-00000.parquet \
  --impersonate-service-account="${HOT_SA}"

# ------------------------------------------------------------------------------
# 验收 3：验证表元数据读取（Iceberg 查询引擎快照推断）
# ------------------------------------------------------------------------------
# 预期结果：HTTP 200 OK，正常返回 metadata.json，保证 SQL 引擎规划不断表
gcloud storage cat gs://${BUCKET#gs://}/${DATA_ROOT_PREFIX}/<db>.db/<tbl>/metadata/v1.metadata.json \
  --impersonate-service-account="${HOT_SA}"

# ------------------------------------------------------------------------------
# 验收 4：验证写入方全生命周期读写（Spark / Flink 管道）
# ------------------------------------------------------------------------------
# 预期结果：热分区与冷分区均可自由写入与覆盖（用于 Compaction 与历史修正）
echo "write-test" | gcloud storage cp - gs://${BUCKET#gs://}/${DATA_ROOT_PREFIX}/<db>.db/<tbl>/data/dt=$(date -u -d '-5 days' +%Y-%m-%d)/test.parquet \
  --impersonate-service-account="${WRITER_SA}"
```

---

## 7. 架构师深度避坑指南与细节原理解析

### ⚠️ 陷阱 1：Bucket 级 / Project 级权限污染（Additive IAM 叠加风险）
* **原理**：GCS Managed Folder 的 IAM 鉴权为**纯叠加（Additive）模式**，不支持显式 Deny。如果用户或 SA 在**Bucket 级别**或**Project 级别**被授予了 `roles/storage.objectViewer`，该权限会自动穿透所有 Managed Folder，导致冷数据隔离完全失效！
* **防范方案**：必须确保 `iceberg-hot-reader` 在 Bucket 级没有任何 `storage.objects.*` 绑定，其权限只能逐层下发到 Managed Folder 上。

### ⚠️ 陷阱 2：跨日作业写入阻断（Midnight Write Window Gap）
* **原理**：如果定时任务在每天 00:30 执行，而 Spark/Flink 在 00:01 开始写入今天新分区 `dt=TODAY`，此时如果该目录尚未打上 Managed Folder 标签，写入是否会受阻？
* **防范方案**：
  1. 本调和引擎具备**预创建机制**：每次运行都会自动为全量表预创建并绑定今天（`offset=0`）与明天（`offset=1`）两个分区。
  2. 即使未预创建，由于 `iceberg-writer` 在根目录 `datasets/` 上已持有 `roles/storage.objectUser`，具备向任意新子路径写入对象的权限。

### ⚠️ 陷阱 3：命令行工具与 SDK 鉴权陷阱（CLI objects.get 403）
* **原理**：在纯容器中若调用 `gcloud storage managed-folders set-iam-policy`，CLI 会额外发起 `objects.get` 检查目录对象是否存在。若运维 SA 采用最小权限，CLI 会抛出 403。
* **防范方案**：交付镜像锁定为本方案提供的 Python 原生 REST 引擎（`reconcile_fast.py`），直接对接 GCS 控制面 API，规避 CLI 的非必要探测。

### ⚠️ 陷阱 4：大规模分区 QPS 限流与网络优化
* **实测指标**：在 Cloud Run 与 GCS 同一区域（`us-central1`）内网调用时，30 线程并发吞吐达 **147.5 分区/秒**，10,041 个分区调和仅需 **68 秒**。
* **最佳实践**：
  * Cloud Run Job 必须与 GCS 部署在**同一 GCP Region**。
  * 并发线程建议设为 `20 ~ 30`。避免超过 50，以符合 GCP GCS 控制面建议的并发速率阈值。

---

## 8. 运维监控与应急回滚预案

### 8.1 查看每日执行日志
在 GCP 控制台进入 **Cloud Run > Jobs > mf-reconcile > Executions**，或在命令行执行：
```bash
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="mf-reconcile"' \
  --limit=50 --freshness=24h --format='value(textPayload)'
```

### 8.2 应急一键全通回滚预案（Rollback）
如果在生产部署后客户因紧急历史数据追溯需求需要临时解除隔离，执行以下应急回滚操作即可立即恢复全通状态（延迟 < 5 秒），无需改动任何代码：

```bash
source ./config.env

# 方案 A：临时在 Bucket 级给 hot reader 放开权限（权限立即全通生效，延迟 < 5 秒）
gcloud storage buckets add-iam-policy-binding "${BUCKET}" \
  --member="serviceAccount:${HOT_SA}" \
  --role="roles/storage.objectViewer"

# 方案 B：暂停 Cloud Scheduler 每日定时任务
gcloud scheduler jobs pause mf-reconcile-daily --location="${REGION}"
```
需求处理完毕后，只需删除 Bucket 级别的临时绑定并恢复 Scheduler，即可平滑重新生效热冷隔离策略。
