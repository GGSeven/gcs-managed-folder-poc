# 操作手册：GCS Managed Folder 热/冷数据访问隔离

> 本手册面向两个部门：
> - **【运维部门】**：负责所有 IAM 身份（service account）的创建与项目/bucket 级授权（第 3 章）
> - **【大数据运维部门】**：负责存储配置、managed folder、定时调和任务的部署与日常运维（第 4 章）
>
> 第 1、2 章是方案原理，建议两个部门都阅读。

---

## 1. 方案原理

### 1.1 要解决的问题

- Bucket 内 `warehouse/odc.db/` 下每天由 Spark/Flink 生成一个天级目录，存放 Iceberg 数据；
- Lifecycle 已配置：对象创建 60 天后转 Coldline，365 天后转 Archive；
- 曾发生员工 SQL 误扫大量冷数据，产生高额**检索费**（Coldline ≈ $0.02/GB、Archive ≈ $0.05/GB，按实际取回字节计费）；
- 目标：**Spark/Flink 写入管道可读写全部数据；人工和查询任务在权限层面就碰不到冷数据**。

### 1.2 为什么用 IAM 能控制成本

检索费在字节被取回时产生，而 IAM 的 403 拒绝发生在**任何字节被取回之前**。
因此把冷数据的读权限从查询身份上拿走后：

```
员工执行 SELECT *（未加分区过滤）
→ 引擎扫到 60 天前的目录 → 403 Forbidden → 查询当场失败
→ 检索费：0
```

误操作从"悄悄产生大额账单"变成"当场报错"。存储类别（Coldline/Archive）本身不影响可读性、
只影响计费，所以 IAM 是存储层唯一的"硬拦截"手段。

### 1.3 Managed Folder 是什么

GCS 的扁平命名空间里本没有真正的"文件夹"（所谓目录只是对象名前缀）。
**Managed folder 是一种可以单独挂 IAM 策略的前缀资源**，特性：

| 特性 | 说明 |
|---|---|
| 前提条件 | bucket 必须开启 uniform bucket-level access（UBLA） |
| 数量上限 | 每 bucket **无上限**（按天建目录数年无配额压力） |
| 生效范围 | 策略作用于该前缀下的**所有对象** |
| 叠加性 | 嵌套 managed folder 的权限**只增不减**（additive），且**不支持 deny** |
| 独立于对象 | 可以在对象写入**之前**创建并配好权限 |
| 更新限速 | 每个 managed folder 每秒最多 1 次策略更新 |

两条重要推论：

1. **所有业务 SA 都不能持有 bucket 级对象权限**——因为没有 deny，bucket 级授权无法被
   folder 级"排除"，一旦授了，folder 级隔离整体失效。
2. **写任务不需要感知权限配置**——managed folder 先于对象存在，Spark/Flink 照常写文件即可。

### 1.4 三类身份的授权模型

| 身份 | 用途 | 授权位置 | 角色 | 是否随热/冷切换 |
|---|---|---|---|---|
| `iceberg-writer` | Spark/Flink 写入管道 | `warehouse/odc.db/` 表前缀级 managed folder | `roles/storage.objectUser`（读写删） | **否**，长期固定 |
| `iceberg-hot-reader` | 人工/查询任务 | 近 60 天的各天级 managed folder | `roles/storage.objectViewer` | 是，每日调和 |
| `iceberg-cold-reader` | 少数授权的历史数据查询 | 60 天以前的各天级 managed folder | `roles/storage.objectViewer` | 是，每日调和 |
| `mf-reconciler` | 自动化调和任务（非业务身份） | 表前缀级 managed folder | 自定义角色 `mfReconciler`（**无数据读取权限**） | — |

### 1.4.1 权限矩阵（最终授权清单，逐层核对用）

| 资源层级 | `iceberg-writer`<br>(Spark/Flink) | `iceberg-hot-reader`<br>(人工/查询) | `iceberg-cold-reader`<br>(历史查询) | `mf-reconciler`<br>(自动化) |
|---|---|---|---|---|
| **Project 级** | 无 | 无 | 无 | 无 |
| **Bucket 级** | 无 | 无 | 无 | 无 |
| **表前缀 managed folder**（`odc.db/`） | `roles/storage.objectUser`（读写删） | 无 | 无 | 自定义角色 `mfReconciler` |
| **热天级 folder**（目录日期 < 60 天） | 无直接授权（继承表前缀） | `roles/storage.objectViewer` | 无 | 无直接授权（继承表前缀） |
| **冷天级 folder**（目录日期 ≥ 60 天） | 无直接授权（继承表前缀） | 无 | `roles/storage.objectViewer` | 无直接授权（继承表前缀） |

矩阵要点：

- **Project 级和 bucket 级的授权一个都不需要**（自定义角色的"定义"存放在 project 里，
  但那只是角色定义，不是授权绑定）。全部授权都收敛在 `odc.db/` 前缀以内。
- 天级 folder 上 hot/cold reader 的绑定由 reconcile 任务每日自动维护（每个天级 folder
  的策略里**只有一个成员**：热的是 hot-reader，冷的是 cold-reader）。
- `iceberg-writer` 和 `mf-reconciler` 在任何天级 folder 上都没有直接绑定，各自的能力
  全部来自表前缀 managed folder 的继承（managed folder 权限对前缀下所有对象和
  嵌套 managed folder 生效且叠加）。
- **`mf-reconciler` 与 `iceberg-hot-reader` 是两个不同的权限平面，不能合并**：
  前者管权限（创建 managed folder、改冷热两侧所有天级 folder 的策略——"热转冷"正是
  它执行的），后者读数据；reconciler 连热数据的内容都读不了。
- **为什么 `mf-reconciler` 不用 `roles/storage.admin` 或 `roles/storage.folderAdmin`**：
  这两个预定义角色都包含 `storage.objects.get`（能读对象内容，即冷热数据都能读走）。
  自定义角色 `mfReconciler` 只含：
  `storage.managedFolders.{create,get,list,getIamPolicy,setIamPolicy}` +
  `storage.objects.list`（枚举天级目录，只见文件名不见内容）+ `storage.buckets.get`。
  已实测：持此角色（且仅绑定在表前缀级）可完整执行 reconcile，
  但读任何对象内容均返回 403。

**为什么写入方不能按天授权**：Iceberg 每次提交（commit）都要读写表级共享的 `metadata/`
（metadata.json、manifest）；compaction、expire snapshots、Flink upsert 会跨天读写删历史
文件。若按天授权，管道第一步读 metadata 就会 403。因此写入方授在表前缀一层，权限被圈定在
`odc.db/` 内（不是 bucket 级），bucket 中其他业务数据仍然碰不到。

### 1.5 热→冷的权限切换机制（reconcile 模式）

关键认知：**lifecycle 转储（Standard→Coldline）只改对象的存储类别，与 IAM 完全无关**。
所以不需要监听"转冷事件"——天级目录的名字自带日期，每天一个定时任务即可：

```
Cloud Scheduler（每日 00:30 UTC）
      │
      ▼
Cloud Run Job（reconcile.sh，以 mf-reconciler 身份运行）
      │
      ├─ 预创建"今天 + 明天"的 managed folder，授权 hot-reader
      │  （保证权限总是先于数据落地，与写任务零时序依赖）
      └─ 扫描 odc.db/ 下全部天级目录：
           目录日期距今 < 60 天 → 策略 = hot-reader (objectViewer)
           目录日期距今 ≥ 60 天 → 策略 = cold-reader (objectViewer)
```

可靠性设计：

- **全量扫描 + 期望状态调和**：不是只处理"今天该转的那一个"，而是每次算出所有目录的
  应有状态并修正差异。漏跑一天、任务中途失败，下次运行自动补齐（自愈）。
- **幂等**：`set-iam-policy` 是整体覆盖式写入；写之前先 diff，状态一致则跳过，
  既可随时重跑，也避开每秒 1 次的更新限速。
- **失败重试与告警**：Cloud Run Job 配置 max-retries；对执行失败配置 Cloud Monitoring 告警。

### 1.6 已排除的备选方案（应对客户追问）

- **把冷数据搬到另一个桶**：若在转冷后搬，要付检索费 + 最短存储期（Coldline 90 天/
  Archive 365 天）提前删除费 + 每对象操作费；即使在第 60 天仍是 Standard 时用 Storage
  Transfer Service 搬，也有决定性缺陷——**Iceberg metadata 记录带 bucket 名的绝对路径，
  跨桶搬迁即断表**。managed folder 方案数据一字节不动，隔离效果相同。
- **Autoclass**：能消灭检索费但无权限治理，且与现有 lifecycle 转储规则互斥，仅当客户
  "只要防账单、不要隔离"时才考虑。
- **IAM Conditions**：CEL 表达不了"路径中的日期距今 60 天内"这类动态条件。
- **事件驱动监听转储**：转储是对象级、时间分散，而权限切换本就不依赖转储发生，徒增复杂度。

---

## 2. 职责划分总览

| # | 操作 | 负责部门 | 方式 |
|---|---|---|---|
| 1 | 创建 4 个 service account（3 业务 + 1 自动化） | 运维部门 | 第 3.1 节命令 |
| 2 | 创建自定义角色 `mfReconciler`（仅角色定义，无绑定） | 运维部门 | 第 3.2 节命令 |
| 3 | （可选，仅验证期）授验证人员 impersonation 权限 | 运维部门 | 第 3.3 节命令 |
| 4 | 确认/开启 bucket 的 UBLA | 大数据运维部门 | 第 4.1 节 |
| 5 | 配置 lifecycle（60 天 Coldline / 365 天 Archive） | 大数据运维部门 | 第 4.2 节 |
| 6 | 创建表前缀 managed folder，授权 writer + reconciler | 大数据运维部门 | 第 4.3 节 |
| 7 | 首次执行 reconcile、验证隔离 | 大数据运维部门 | 第 4.4–4.5 节 |
| 8 | 部署每日定时任务 + 监控告警 | 大数据运维部门 | 第 4.6 节 |
| 9 | 日常运维（阈值调整、故障处理） | 大数据运维部门 | 第 4.7 节 |

> 分工边界说明：**project/bucket 级 IAM 绑定和 SA 生命周期归运维部门**；
> managed folder 上的策略属于存储资源配置，由大数据运维部门（及其自动化任务）管理，
> 前提是运维部门已按第 3.2 节授予 `mf-reconciler` 相应权限。

所有命令都先加载同一份配置（两个部门使用同一份 `config.env`，先按实际环境修改其中的
`PROJECT_ID`、`REGION`、`DATA_PREFIX` 等变量）：

```bash
source ./config.env
```

---

## 3. 【运维部门】IAM 操作步骤

### 3.1 创建 service account（共 4 个）

```bash
# 业务身份 ×3
gcloud iam service-accounts create iceberg-writer      --project="${PROJECT_ID}" --display-name="Iceberg pipeline writer (Spark/Flink)"
gcloud iam service-accounts create iceberg-hot-reader  --project="${PROJECT_ID}" --display-name="Hot data reader (queries/humans)"
gcloud iam service-accounts create iceberg-cold-reader --project="${PROJECT_ID}" --display-name="Cold data reader (authorized history access)"

# 自动化调和任务身份 ×1
gcloud iam service-accounts create mf-reconciler       --project="${PROJECT_ID}" --display-name="Managed folder daily reconciler"
```

### 3.2 创建最小权限自定义角色（仅角色定义，不做任何绑定）

`mf-reconciler` 只需要"创建 managed folder + 管理其 IAM + 列目录名"。
**不要用 `roles/storage.admin` 或 `roles/storage.folderAdmin`**——两者都含
`storage.objects.get`，会让自动化身份能读走全部冷热数据（见 1.4.1 节）。

```bash
# 创建自定义角色（项目级角色定义，仅需一次；这只是定义，不产生任何访问权限）
gcloud iam roles create mfReconciler --project="${PROJECT_ID}" \
  --title="Managed Folder Reconciler" \
  --description="Manage managed folders and their IAM; list prefixes; cannot read object data" \
  --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
```

角色的**绑定**发生在表前缀级 managed folder 上（存储资源配置），由大数据运维部门在
第 4.3 节完成——`mf-reconciler` **不需要任何 bucket 级或 project 级绑定**。
前缀级授权对其下嵌套的天级 managed folder 同样生效（已实测跑通完整 reconcile）。

### 3.3 （可选）验证期的 impersonation 授权

大数据运维部门执行 `verify.sh` 时需要模拟三个业务 SA 发起请求。给执行验证的人员/身份授
`serviceAccountTokenCreator`（验证完成后建议回收）：

```bash
VERIFIER="user:someone@example.com"        # 或 serviceAccount:xxx（按实际执行者填写）
for sa in "${HOT_SA}" "${COLD_SA}" "${WRITER_SA}"; do
  gcloud iam service-accounts add-iam-policy-binding "${sa}" \
    --member="${VERIFIER}" --role="roles/iam.serviceAccountTokenCreator"
done
```

### 3.4 红线（请运维部门把关）

- **绝不给本方案的任何 SA（包括 `mf-reconciler`）授 bucket 级或项目级的存储角色**
  （objectViewer/objectUser/objectAdmin/storage.admin/storage.folderAdmin 等）。
  原理见 1.3 节：managed folder 无 deny，bucket 级授权会让整个隔离方案失效；
  且本方案的全部授权都能在表前缀级 managed folder 内完成（见 1.4.1 矩阵）。
- Spark/Flink 作业改用 `iceberg-writer` 运行时，同步梳理并回收其原有 SA 上的旧权限。
- IAM 变更有分钟级传播延迟，授权后 1–2 分钟再验证属正常现象。

---

## 4. 【大数据运维部门】存储与自动化操作步骤

> 前置条件：第 3 章的 SA 已由运维部门创建、`mf-reconciler` 已获授权。

### 4.1 确认 bucket 开启 UBLA（managed folder 硬性前提）

```bash
gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access)"
# 若为 False（且确认无 ACL 依赖）：
gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
```

⚠️ 存量 bucket 开 UBLA 前需确认没有依赖对象 ACL 的访问方；开启 90 天后不可回退。

### 4.2 配置 lifecycle

`lifecycle.json`（60 天 Coldline、365 天 Archive，仅作用于数据前缀）已在仓库中，按需修改后：

```bash
gcloud storage buckets update "${BUCKET}" --lifecycle-file=lifecycle.json
```

注意阈值语义：lifecycle 的 `age` 按**对象创建时间**计，reconcile 按**目录名日期**计，
两者相差最多 1 天，对权限切换无实质影响，但要让业务方知晓口径。

### 4.3 创建表前缀 managed folder，授权写入方和 reconciler

```bash
gcloud storage managed-folders create "${BUCKET}/${DATA_PREFIX}/"

cat > /tmp/prefix-policy.json <<EOF
{"bindings": [
  {"role": "${WRITER_ROLE}", "members": ["serviceAccount:${WRITER_SA}"]},
  {"role": "projects/${PROJECT_ID}/roles/${OPS_ROLE_ID}", "members": ["serviceAccount:${OPS_SA}"]}
]}
EOF
gcloud storage managed-folders set-iam-policy "${BUCKET}/${DATA_PREFIX}/" /tmp/prefix-policy.json
```

这一条策略同时完成两件事：writer 获得全表读写删；reconciler 获得对嵌套天级
managed folder 的管理能力（前缀级授权向下生效）。两者都被圈定在 `odc.db/` 内。

此后 Spark/Flink 侧唯一的改动是：**作业改用 `iceberg-writer` 这个 SA 运行**。
作业代码、写入路径、目录生成方式都不用变（原理见 1.3 节"独立于对象"）。

### 4.4 首次执行调和

```bash
./reconcile.sh
```

预期输出：为今天/明天预创建 managed folder 并授 hot-reader；历史目录按 60 天阈值分别
落到 hot/cold 策略；重复执行时已一致的目录显示 `[SKIP]`（幂等验证）。

### 4.5 验证访问隔离

需要 3.3 节的 impersonation 授权，等待 1–2 分钟传播后：

```bash
./verify.sh
```

8 条断言全部 PASS 才算通过：

| 身份 | 操作 | 预期 |
|---|---|---|
| hot-reader | 读热 / 读冷 | ALLOW / **DENY** |
| cold-reader | 读冷 / 读热 | ALLOW / **DENY** |
| writer | 读热 / 读冷 / 写热 / 写冷 | 全部 ALLOW |

### 4.6 部署每日定时任务

```bash
deploy/deploy.sh
```

内容：构建 reconcile 镜像 → 部署 Cloud Run Job（以 `mf-reconciler` 运行，max-retries=3）
→ Cloud Scheduler 每日 00:30 UTC 触发。部署后请再配置一条 Cloud Monitoring 告警：
Cloud Run Job 执行失败时通知值班。

调度时间选择：只要在每天**数据开始写入之前**任意时刻即可；由于 reconcile 总是预建
"明天"的目录，即使某天错过窗口也不影响写入和授权。

### 4.7 日常运维要点

- **改热数据保留天数**：改 `config.env` 的 `HOT_DAYS` 并同步修改 `lifecycle.json` 的
  `age`，重新部署。下次 reconcile 会自动把所有目录调整到新阈值（全量调和的好处）。
- **任务挂了怎么办**：无需补数据，修复后跑一次 `./reconcile.sh` 即可全量自愈。
- **403 是预期行为**：hot-reader 查询碰到冷分区报 403 是方案设计的拦截效果，
  请提前告知业务方，并给出走 cold-reader 的申请流程。
- **compaction/expire 作业注意**：重写已转冷的文件会产生检索费 + 最短存储期提前删除费，
  此类作业应只针对热分区，冷分区冻结不动。
- **Iceberg `metadata/` 位置确认**：若表级 `metadata/` 目录与数据同前缀且会被查询规划
  读取，需给 hot/cold reader 增授 metadata 路径的读权限（文件小，检索费可忽略）。

---

## 5. 仓库文件索引

> 所有文件的**完整内容**已内嵌在本文档第 6 章附录中，拿到本文档即可直接复制使用，
> 不依赖代码仓库。

| 文件 | 作用 | 主要使用者 |
|---|---|---|
| `config.env` | 全部参数（项目、bucket、前缀、日期格式、SA、阈值） | 两个部门共用 |
| `setup.sh` | POC 一次性初始化（含模拟数据；生产环境按第 3/4 章拆开执行） | POC 演示 |
| `reconcile.sh` | 核心调和脚本（生产即跑此脚本） | 大数据运维 |
| `verify.sh` | 8 条访问断言 | 大数据运维 |
| `lifecycle.json` | 生命周期规则 | 大数据运维 |
| `deploy/` | Cloud Run Job + Scheduler 部署 | 大数据运维 |
| `README.md` | 方案评估与备选方案对比 | 两个部门 |

---

## 6. 附录：完整脚本与配置（可直接复制使用）

以下为全部脚本的完整内容（与实际验证通过的版本一致）。使用方式：

1. 按相同的相对路径保存各文件（`deploy/` 下两个文件放入 deploy 子目录）；
2. 修改 6.1 `config.env` 中的 `PROJECT_ID`、`REGION`、`DATA_PREFIX`、`FOLDER_DATE_FORMAT` 为实际环境值；
3. `chmod +x *.sh deploy/*.sh` 后按第 3、4 章的步骤执行。

| 附录 | 文件 | 用途 | 使用者 |
|---|---|---|---|
| 6.1 | `config.env` | 全部参数，所有脚本共用 | 两个部门 |
| 6.2 | `lifecycle.json` | 生命周期规则（60 天 Coldline / 365 天 Archive） | 大数据运维 |
| 6.3 | `reconcile.sh` | 核心调和脚本（每日定时执行的就是它） | 大数据运维 |
| 6.4 | `verify.sh` | 10 条访问断言验证 | 大数据运维 |
| 6.5 | `setup.sh` | POC 一次性初始化（生产环境请按第 3/4 章拆步执行，此脚本含模拟数据生成） | POC 演示 |
| 6.6 | `deploy/Dockerfile` | reconcile 容器镜像 | 大数据运维 |
| 6.7 | `deploy/deploy.sh` | Cloud Run Job + Scheduler 部署 | 大数据运维 |

### 6.1 `config.env`

```bash
# ===== POC 配置，所有脚本 source 此文件 =====
# !!! 使用前必须修改：替换为你自己的项目 ID 和 region（或运行前 export 同名环境变量覆盖）!!!
export PROJECT_ID="${PROJECT_ID:-your-project-id}"
export REGION="${REGION:-us-central1}"

# 演示用 bucket（必须启用 uniform bucket-level access，managed folder 的前提条件）
export BUCKET="gs://${PROJECT_ID}-mf-poc"

# Iceberg 数据所在前缀（bucket 内路径，不带尾部斜杠）
export DATA_PREFIX="warehouse/odc.db"

# 天级 folder 的命名格式（date +FORMAT），按客户实际命名调整，如 %Y%m%d 或 dt=%Y-%m-%d
export FOLDER_DATE_FORMAT="%Y-%m-%d"

# 热数据保留天数，与 bucket lifecycle 转 Coldline 的天数保持一致
# （可用环境变量临时覆盖，便于模拟"folder 跨过阈值"的切换测试）
export HOT_DAYS="${HOT_DAYS:-60}"

# 热/冷两个 reader service account（人工/查询任务用，被按天隔离）
export HOT_SA_NAME="iceberg-hot-reader"
export COLD_SA_NAME="iceberg-cold-reader"
export HOT_SA="${HOT_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export COLD_SA="${COLD_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# 授予的角色：热端读，冷端读
export HOT_ROLE="roles/storage.objectViewer"
export COLD_ROLE="roles/storage.objectViewer"

# 写入方 SA（Spark/Flink 用）：对整个表前缀读写删，不参与热冷切换。
# Iceberg 提交要读写 metadata/，compaction/expire/upsert 要跨天读写删历史文件。
export WRITER_SA_NAME="iceberg-writer"
export WRITER_SA="${WRITER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export WRITER_ROLE="roles/storage.objectUser"

# 自动化调和任务 SA：通过自定义角色仅能管理 managed folder 和列目录，
# 不含 storage.objects.get，读不到任何数据内容
export OPS_SA_NAME="mf-reconciler"
export OPS_SA="${OPS_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export OPS_ROLE_ID="mfReconciler"
```

### 6.2 `lifecycle.json`

```json
{
  "rule": [
    {
      "action": { "type": "SetStorageClass", "storageClass": "COLDLINE" },
      "condition": { "age": 60, "matchesPrefix": ["warehouse/odc.db/"] }
    },
    {
      "action": { "type": "SetStorageClass", "storageClass": "ARCHIVE" },
      "condition": { "age": 365, "matchesPrefix": ["warehouse/odc.db/"] }
    }
  ]
}
```

### 6.3 `reconcile.sh`

```bash
#!/usr/bin/env bash
# 核心调和(reconcile)脚本：每天定时运行一次（也可随时手工重跑，幂等）。
#
# 设计要点：
#  - 不去“检测” storage class 变化。folder 名字自带日期，直接用日期算出
#    每个 folder 【应该】属于热还是冷，然后把 IAM 调成应有状态。
#  - 全量扫描 + 按需修正：漏跑一天、脚本中途失败，下次运行会自愈。
#  - set-iam-policy 是整体覆盖式写入，天然幂等；先 diff 再写，
#    避免无谓更新（managed folder 有每秒 1 次更新的限制）。
#  - 为“今天”的 folder 预创建 managed folder 并授权热 SA——managed folder
#    可以在对象写入之前就存在，保证新数据落地时权限已就位。
set -euo pipefail
cd "$(dirname "$0")"
source ./config.env

TODAY_EPOCH=$(date -u +%s)

# managed folder 的创建/策略读写全部直连 JSON API：
# gcloud 的 managed-folders 子命令在执行前会对路径做一次 objects.get 探测，
# 而 reconciler 的自定义角色刻意不含该权限（不读数据内容）；
# API 本身只要求 storage.managedFolders.* 权限。
API="https://storage.googleapis.com/storage/v1/b/${BUCKET#gs://}"
TOKEN=$(gcloud auth print-access-token)

urlenc() {
  python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

# 从 folder 名解析日期。默认支持 %Y-%m-%d / %Y%m%d；
# 若客户命名如 dt=2026-08-13，请在这里先剥掉前缀再解析。
parse_folder_epoch() {
  date -u -d "$1" +%s 2>/dev/null || echo ""
}

# 列出 DATA_PREFIX 下的一层子目录名（JSON API delimiter 列举）。
# 不用 `gcloud storage ls`：它会先对路径本身做一次 objects.get，而 reconciler
# 的自定义角色刻意不含该权限；delimiter 列举单次调用只返回目录名、不枚举对象，
# 其授权可由【表前缀级 managed folder】上的 objects.list 满足——因此本脚本
# 不需要任何 bucket 级权限。
list_day_folders() {
  local page_token="" resp
  while :; do
    resp=$(curl -sf -H "Authorization: Bearer ${TOKEN}" \
      "${API}/o?prefix=${DATA_PREFIX}/&delimiter=/&fields=prefixes,nextPageToken${page_token:+&pageToken=${page_token}}")
    echo "${resp}" | python3 -c 'import json,sys; [print(p) for p in json.load(sys.stdin).get("prefixes",[])]'
    page_token=$(echo "${resp}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("nextPageToken",""))')
    [[ -z "${page_token}" ]] && break
  done
}

# 确保 managed folder 存在（已存在时 API 返回 409，忽略即可）
# 参数为 bucket 内相对路径，如 warehouse/odc.db/2026-08-13/
ensure_managed_folder() {
  local rel="$1"
  curl -s -o /dev/null -X POST \
    -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
    -d "{\"name\": \"${rel}\"}" "${API}/managedFolders"
}

# 将 managed folder 的 IAM 设置为“只有指定 SA 持有指定角色”
# 先检查现状，一致则跳过。参数 rel 为 bucket 内相对路径。
apply_policy() {
  local rel="$1" sa="$2" role="$3" tier="$4"
  local mf_url="${API}/managedFolders/$(urlenc "${rel}")/iam"
  local desired_member="serviceAccount:${sa}"
  local current resp

  current=$(curl -s -H "Authorization: Bearer ${TOKEN}" "${mf_url}" || echo '{}')

  if echo "${current}" | python3 -c "
import json,sys
policy=json.load(sys.stdin)
bindings={(b['role'],m) for b in policy.get('bindings',[]) for m in b.get('members',[])}
ok = bindings == {('${role}','${desired_member}')}
sys.exit(0 if ok else 1)
"; then
    echo "    [SKIP] ${BUCKET}/${rel} 已是 ${tier} 状态"
    return
  fi

  resp=$(curl -s -X PUT \
    -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
    -d "{\"bindings\": [{\"role\": \"${role}\", \"members\": [\"${desired_member}\"]}]}" \
    "${mf_url}")
  if echo "${resp}" | grep -q '"error"'; then
    echo "    [FAIL] ${BUCKET}/${rel} 设置策略失败: ${resp}" >&2
    return 1
  fi
  echo "    [SET ] ${BUCKET}/${rel} -> ${tier} (${sa})"
}

echo "==> 预创建今天和明天的 managed folder（数据落地前 hot reader 授权就位，"
echo "    预建明天是为了彻底消除与写任务的时序依赖）"
for day in 0 1; do
  rel="${DATA_PREFIX}/$(date -u -d "+${day} days" +"${FOLDER_DATE_FORMAT}")/"
  ensure_managed_folder "${rel}"
  apply_policy "${rel}" "${HOT_SA}" "${HOT_ROLE}" "hot"
done

echo "==> 扫描 ${BUCKET}/${DATA_PREFIX}/ 下所有天级 folder 并调和 IAM"
for p in $(list_day_folders); do
  name=$(basename "${p}")
  epoch=$(parse_folder_epoch "${name}")
  if [[ -z "${epoch}" ]]; then
    echo "    [WARN] ${name} 不是日期命名，跳过"
    continue
  fi

  age_days=$(( (TODAY_EPOCH - epoch) / 86400 ))
  ensure_managed_folder "${p}"

  if (( age_days < HOT_DAYS )); then
    apply_policy "${p}" "${HOT_SA}" "${HOT_ROLE}" "hot"
  else
    apply_policy "${p}" "${COLD_SA}" "${COLD_ROLE}" "cold"
  fi
done

echo "==> reconcile 完成"
```

### 6.4 `verify.sh`

```bash
#!/usr/bin/env bash
# 验证热/冷隔离是否生效：分别模拟(impersonate)两个 SA 读取热、冷 folder 中的对象。
# 前提：当前登录账号对两个 SA 具有 roles/iam.serviceAccountTokenCreator。
# 授权命令（只需一次，注意 IAM 传播可能需要 1-2 分钟；
# 若当前身份是用户账号，把 member 前缀 serviceAccount: 换成 user:）：
#   for sa in $HOT_SA $COLD_SA $WRITER_SA $OPS_SA; do
#     gcloud iam service-accounts add-iam-policy-binding $sa \
#       --member="serviceAccount:$(gcloud config get-value account)" \
#       --role="roles/iam.serviceAccountTokenCreator"
#   done
set -uo pipefail
cd "$(dirname "$0")"
source ./config.env

HOT_OBJ="${BUCKET}/${DATA_PREFIX}/$(date -u -d '-1 days' +"${FOLDER_DATE_FORMAT}")/data-00000.parquet"
COLD_OBJ="${BUCKET}/${DATA_PREFIX}/$(date -u -d '-65 days' +"${FOLDER_DATE_FORMAT}")/data-00000.parquet"

check() {
  local sa="$1" op="$2" obj="$3" expect="$4" actual
  case "${op}" in
    read)  gcloud storage cat "${obj}" --impersonate-service-account="${sa}" &>/dev/null \
             && actual="ALLOW" || actual="DENY" ;;
    write) echo "poc-write-test" | gcloud storage cp - "${obj}" \
             --impersonate-service-account="${sa}" &>/dev/null \
             && actual="ALLOW" || actual="DENY" ;;
  esac
  if [[ "${actual}" == "${expect}" ]]; then
    echo "  [PASS] ${sa%%@*} ${op} ${obj#${BUCKET}/} => ${actual}（符合预期）"
  else
    echo "  [FAIL] ${sa%%@*} ${op} ${obj#${BUCKET}/} => ${actual}（预期 ${expect}）"
    FAILED=1
  fi
}

FAILED=0
echo "==> 验证 reader 隔离（人工/查询任务：只能读热，碰不到冷）"
check "${HOT_SA}"  read "${HOT_OBJ}"  "ALLOW"   # 热 reader 读热数据：应成功
check "${HOT_SA}"  read "${COLD_OBJ}" "DENY"    # 热 reader 读冷数据：应被拒（误查冷数据 = 0 费用）
check "${COLD_SA}" read "${COLD_OBJ}" "ALLOW"   # 冷 reader 读冷数据：应成功
check "${COLD_SA}" read "${HOT_OBJ}"  "DENY"    # 冷 reader 读热数据：应被拒

echo "==> 验证写入方（Spark/Flink：冷热均可读写，不受热冷切换影响）"
check "${WRITER_SA}" read  "${HOT_OBJ}"  "ALLOW"                                        # 读热
check "${WRITER_SA}" read  "${COLD_OBJ}" "ALLOW"                                        # 读冷（compaction 场景）
check "${WRITER_SA}" write "${HOT_OBJ%/*}/writer-test.txt"  "ALLOW"                     # 写热
check "${WRITER_SA}" write "${COLD_OBJ%/*}/writer-test.txt" "ALLOW"                     # 写冷（重写历史文件场景）

echo "==> 验证自动化身份（mf-reconciler：只管权限，读不到数据内容）"
check "${OPS_SA}" read "${HOT_OBJ}"  "DENY"    # 自定义角色无 objects.get
check "${OPS_SA}" read "${COLD_OBJ}" "DENY"

(( FAILED == 0 )) && echo "==> 全部通过 ✔" || { echo "==> 存在失败项 ✘"; exit 1; }
```

### 6.5 `setup.sh`

```bash
#!/usr/bin/env bash
# 一次性初始化：bucket(UBLA) + lifecycle + 两个 SA + 模拟的天级 Iceberg 目录
set -euo pipefail
cd "$(dirname "$0")"
source ./config.env

echo "==> 1. 创建 bucket（uniform bucket-level access 是 managed folder 的硬性前提）"
if ! gcloud storage buckets describe "${BUCKET}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud storage buckets create "${BUCKET}" \
    --project="${PROJECT_ID}" --location="${REGION}" \
    --uniform-bucket-level-access
else
  echo "    bucket 已存在，跳过"
fi

echo "==> 2. 应用 lifecycle 规则（60 天 Coldline / 365 天 Archive）"
gcloud storage buckets update "${BUCKET}" --lifecycle-file=lifecycle.json

echo "==> 3. 创建 service account（热/冷 reader、写入方、自动化调和）及自定义角色"
for sa in "${HOT_SA_NAME}" "${COLD_SA_NAME}" "${WRITER_SA_NAME}" "${OPS_SA_NAME}"; do
  if ! gcloud iam service-accounts describe "${sa}@${PROJECT_ID}.iam.gserviceaccount.com" \
       --project="${PROJECT_ID}" &>/dev/null; then
    gcloud iam service-accounts create "${sa}" --project="${PROJECT_ID}" \
      --display-name="POC ${sa}"
  else
    echo "    ${sa} 已存在，跳过"
  fi
done

# reconciler 的最小权限自定义角色：只能管理 managed folder + 列目录名，
# 不含 storage.objects.get，读不到任何数据内容
# （storage.admin / storage.folderAdmin 都含 objects.get，不要用）
if ! gcloud iam roles describe "${OPS_ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud iam roles create "${OPS_ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and their IAM; list prefixes; cannot read object data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get" >/dev/null
  echo "    自定义角色 ${OPS_ROLE_ID} 已创建"
fi

# 注意：任何 SA（业务或自动化）都【不要】授予 bucket 级或 project 级的对象权限。
# managed folder 的 IAM 是叠加(additive)的，且不支持 deny，
# bucket 级授权会让 folder 级隔离完全失效。
# 写入方和 reconciler 的权限都授在表前缀级 managed folder 上（见第 4 步），
# 范围被圈定在 DATA_PREFIX 内，bucket 中其他业务数据碰不到。

echo "==> 4. 表前缀级 managed folder：授予写入方(Spark/Flink)整表读写删 + reconciler 管理权限"
echo "     （Iceberg 提交要读写 metadata/，compaction/expire 要跨天操作历史文件，"
echo "      故写入方不参与热冷切换；嵌套的天级 managed folder 权限叠加，互不干扰；"
echo "      reconciler 的自定义角色对嵌套 managed folder 同样生效，无需 bucket 级绑定）"
prefix_mf="${BUCKET}/${DATA_PREFIX}/"
gcloud storage managed-folders create "${prefix_mf}" &>/dev/null || true
prefix_policy=$(mktemp)
cat > "${prefix_policy}" <<EOF
{"bindings": [
  {"role": "${WRITER_ROLE}", "members": ["serviceAccount:${WRITER_SA}"]},
  {"role": "projects/${PROJECT_ID}/roles/${OPS_ROLE_ID}", "members": ["serviceAccount:${OPS_SA}"]}
]}
EOF
gcloud storage managed-folders set-iam-policy "${prefix_mf}" "${prefix_policy}" >/dev/null
rm -f "${prefix_policy}"
echo "    ${prefix_mf} -> ${WRITER_SA} (${WRITER_ROLE})"
echo "    ${prefix_mf} -> ${OPS_SA} (${OPS_ROLE_ID})"

echo "==> 5. 模拟客户目录：生成最近 65 天的天级 folder，每个放一个数据文件"
echo "     （覆盖 60 天阈值两侧，便于验证热/冷切换）"
for offset in 0 1 30 58 59 60 61 65; do
  d=$(date -u -d "-${offset} days" +"${FOLDER_DATE_FORMAT}")
  obj="${BUCKET}/${DATA_PREFIX}/${d}/data-00000.parquet"
  if ! gcloud storage objects describe "${obj}" &>/dev/null; then
    echo "iceberg-data-placeholder ${d}" | gcloud storage cp - "${obj}"
  fi
done

echo "==> 完成。下一步执行 ./reconcile.sh"
```

### 6.6 `deploy/Dockerfile`

```dockerfile
FROM google/cloud-sdk:slim
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY config.env reconcile.sh ./
RUN chmod +x reconcile.sh
ENTRYPOINT ["/app/reconcile.sh"]
```

### 6.7 `deploy/deploy.sh`

```bash
#!/usr/bin/env bash
# 生产化部署：Cloud Run Job（跑 reconcile）+ Cloud Scheduler（每天触发一次）。
# 前置条件：setup.sh（或按 OPERATIONS.md 第 3/4 章）已创建运维 SA、自定义角色，
# 并在【表前缀级 managed folder】上完成绑定——reconciler 不需要任何 bucket 级权限。
set -euo pipefail
cd "$(dirname "$0")/.."
source ./config.env

JOB_NAME="mf-reconcile"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/mf-poc/reconcile:latest"

echo "==> 前置检查：运维 SA 与前缀级授权"
gcloud iam service-accounts describe "${OPS_SA}" --project="${PROJECT_ID}" >/dev/null
gcloud storage managed-folders get-iam-policy "${BUCKET}/${DATA_PREFIX}/" --format=json \
  | grep -q "${OPS_SA}" || { echo "错误：${OPS_SA} 未绑定在 ${BUCKET}/${DATA_PREFIX}/，先执行 setup.sh 第 4 步"; exit 1; }

echo "==> 启用所需 API（幂等）"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com cloudscheduler.googleapis.com \
  --project="${PROJECT_ID}"

echo "==> 构建镜像并部署 Cloud Run Job"
gcloud artifacts repositories describe mf-poc --location="${REGION}" \
  --project="${PROJECT_ID}" &>/dev/null || \
  gcloud artifacts repositories create mf-poc --location="${REGION}" \
    --project="${PROJECT_ID}" --repository-format=docker

# builds submit 要求 Dockerfile 位于源目录根部，组装一个最小构建上下文
build_ctx=$(mktemp -d)
cp deploy/Dockerfile config.env reconcile.sh "${build_ctx}/"
gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" "${build_ctx}"
rm -rf "${build_ctx}"

# PROJECT_ID 通过环境变量注入（config.env 中为可覆盖的占位符）
gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" --region="${REGION}" --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" --max-retries=3 --task-timeout=30m \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},REGION=${REGION}"

echo "==> Cloud Scheduler：每天 UTC 00:30 触发（scheduler 以 OPS_SA 调用 Job，需 run.invoker）"
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" --role="roles/run.invoker" >/dev/null

gcloud scheduler jobs describe "${JOB_NAME}-daily" --location="${REGION}" \
  --project="${PROJECT_ID}" &>/dev/null || \
gcloud scheduler jobs create http "${JOB_NAME}-daily" \
  --location="${REGION}" --project="${PROJECT_ID}" \
  --schedule="30 0 * * *" --time-zone="Etc/UTC" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
  --http-method=POST \
  --oauth-service-account-email="${OPS_SA}"

echo "==> 完成。建议再配置 Cloud Monitoring 告警：Cloud Run Job 执行失败时通知。"
```

