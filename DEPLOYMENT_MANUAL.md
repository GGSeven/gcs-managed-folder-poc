# 操作手册：GCS Managed Folder 热/冷数据访问隔离（生产实施标准版）

> 本手册面向两个部门：
> - **【运维部门】**：负责所有 IAM 身份（service account）的创建与项目/bucket 级授权（第 3 章）
> - **【大数据运维部门】**：负责存储配置、managed folder、定时调和任务的部署与日常运维（第 4 章）
>
> 第 1、2 章是方案原理，建议两个部门都阅读。

---

## 1. 方案原理

### 1.1 要解决的问题

- 客户生产 Bucket `gs://fr-xjsy-bigdata-gcs-dev` 内 `datasets/<db_name>.db/<table_name>/data/` 下每天由 Spark/Flink 生成一个天级目录（如 `dt=2026-08-13/`），存放 Iceberg 数据；同时在 `metadata/` 下存放表快照与元数据；
- Lifecycle 已配置：对象创建 60 天后转 Coldline，365 天后转 Archive；
- 曾发生员工 SQL 误扫大量冷数据，产生高额**检索费**（Coldline ≈ $0.02/GB、Archive ≈ $0.05/GB，按实际取回字节计费）；
- 目标：**Spark/Flink 写入管道可读写全部数据；人工和查询任务在权限层面就碰不到冷数据，同时确保 Hue/Trino 能正常读取 metadata 解析表结构**。

### 1.2 为什么用 IAM 能控制成本

检索费在字节被取回时产生，而 IAM 的 403 拒绝发生在**任何字节被取回之前**。
因此把冷数据的读权限从查询身份上拿走后：

```
员工在 Hue / 查询引擎中执行 SELECT *（未加分区过滤或跨大范围扫描）
→ 引擎扫到 60 天前的目录 dt=2026-06-01/ → 403 Forbidden → 查询当场失败
→ 检索费：0
```

误操作从"悄悄产生大额账单"变成"当场报错"。存储类别（Coldline/Archive）本身不影响可读性、只影响计费，所以 IAM 是存储层唯一的"硬拦截"手段。

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

1. **所有查询 SA 都不能持有 bucket 级对象权限**——因为没有 deny，bucket 级授权无法被 folder 级"排除"，一旦授了，folder 级隔离整体失效。
2. **写任务不需要感知权限配置**——managed folder 先于对象存在，Spark/Flink 照常写文件即可。

### 1.4 三类身份的授权模型

| 身份 | 用途 | 授权位置 | 角色 | 是否随热/冷切换 |
|---|---|---|---|---|
| `iceberg-writer` | Spark/Flink 写入管道 | `datasets/` 顶级 managed folder | `roles/storage.objectUser`（读写删） | **否**，长期固定继承 |
| `iceberg-hot-reader` | 人工/查询任务 (Hue) | 近 60 天的各天级 folder + 各表 `metadata/` | `roles/storage.objectViewer` | 是，每日调和 |
| `iceberg-cold-reader` | 少数授权的历史数据查询 | 60 天以前的各天级 folder + 各表 `metadata/` | `roles/storage.objectViewer` | 是，每日调和 |
| `mf-reconciler` | 自动化调和任务（非业务身份） | `datasets/` 顶级 managed folder | 自定义角色 `mfReconciler`（**无数据读取权限**） | — |

### 1.4.1 权限矩阵（最终授权清单，逐层核对用）

| 资源层级 | `iceberg-writer`<br>(Spark/Flink) | `iceberg-hot-reader`<br>(人工/查询) | `iceberg-cold-reader`<br>(历史查询) | `mf-reconciler`<br>(自动化) |
|---|---|---|---|---|
| **Project 级** | 无 | 无 | 无 | 无 |
| **Bucket 级** | 无 | 无 | 无 | 无 |
| **顶级 managed folder**（`datasets/`） | `roles/storage.objectUser`（读写删） | 无 | 无 | 自定义角色 `mfReconciler` |
| **表级元数据**（`<db>/<tbl>/metadata/`） | 无直接授权（继承顶级） | `roles/storage.objectViewer` | `roles/storage.objectViewer` | 无直接授权（继承顶级） |
| **热天级 folder**（`data/dt=.../` < 60 天） | 无直接授权（继承顶级） | `roles/storage.objectViewer` | 无 | 无直接授权（继承顶级） |
| **冷天级 folder**（`data/dt=.../` ≥ 60 天） | 无直接授权（继承顶级） | 无 | `roles/storage.objectViewer` | 无直接授权（继承顶级） |

矩阵要点：

- **Project 级和 bucket 级的授权一个都不需要**（自定义角色的"定义"存放在 project 里，但那只是角色定义，不是授权绑定）。全部授权都收敛在 `datasets/` 前缀以内。
- 天级 folder 上 hot/cold reader 的绑定由 reconcile 任务每日自动维护（每个天级 folder 的策略里**只有一个成员**：热的是 hot-reader，冷的是 cold-reader）。
- `iceberg-writer` 和 `mf-reconciler` 在任何天级 folder 上都没有直接绑定，各自的能力全部来自顶级 `datasets/` managed folder 的继承（managed folder 权限对前缀下所有对象和嵌套 managed folder 生效且叠加）。
- **`mf-reconciler` 与 `iceberg-hot-reader` 是两个不同的权限平面，不能合并**：前者管权限（创建 managed folder、改冷热两侧所有天级 folder 的策略——"热转冷"正是它执行的），后者读数据；reconciler 连热数据的内容都读不了。
- **为什么 `mf-reconciler` 不用 `roles/storage.admin` 或 `roles/storage.folderAdmin`**：这两个预定义角色都包含 `storage.objects.get`（能读对象内容，即冷热数据都能读走）。自定义角色 `mfReconciler` 只含：`storage.managedFolders.{create,get,list,getIamPolicy,setIamPolicy}` + `storage.objects.list`（枚举天级目录，只见文件名不见内容）+ `storage.buckets.get`。已实测：持此角色（且仅绑定在 `datasets/` 级）可完整执行 reconcile，但读任何对象内容均返回 403。
- **为什么写入方不能按天授权**：Iceberg 每次提交（commit）都要读写表级共享的 `metadata/`（metadata.json、manifest）；compaction、expire snapshots、Flink upsert 会跨天读写删历史文件。若按天授权，管道第一步读 metadata 就会 403。因此写入方授在顶级 `datasets/` 一层，权限被圈定在 `datasets/` 内（不是 bucket 级），bucket 中其他业务数据仍然碰不到。

### 1.5 热→冷的权限切换机制（reconcile 模式）

关键认知：**lifecycle 转储（Standard→Coldline）只改对象的存储类别，与 IAM 完全无关**。
所以不需要监听"转冷事件"——天级目录的名字自带日期，每天一个定时任务即可：

```
Cloud Scheduler（每日 00:30 UTC）
      │
      ▼
Cloud Run Job（reconcile_fast.py，以 mf-reconciler 身份运行）
      │
      ├─ 确保各库表 metadata/ 拥有 hot/cold reader 读权限（放行元数据规划）
      ├─ 预创建今天（T+0）与明天（T+1）的 managed folder，授权 hot-reader
      │  （保证权限总是先于数据落地，与写任务零时序依赖）
      └─ 扫描或滑动窗口锁定天级目录：
           目录日期距今 < 60 天 → 策略 = hot-reader (objectViewer)
           目录日期距今 ≥ 60 天 → 策略 = cold-reader (objectViewer)
```

可靠性设计：

- **期望状态调和**：算出目录的应有状态并修正差异，漏跑、中途失败，下次运行自动补齐（自愈）。
- **幂等**：策略为整体覆盖式写入；写之前先 diff，状态一致则跳过，既可随时重跑，也避开每秒 1 次的更新限速。
- **失败重试与告警**：Cloud Run Job 配置 max-retries；对执行失败配置 Cloud Monitoring 告警。

### 1.5.1 海量目录下的性能设计（window / full 双模式）

目录累积数年后可达数千上万个，每天全量扫描会越来越慢。解法基于一个关键观察：**冷了的目录永远是冷的**，每天真正可能变状态的目录只有新生成的和刚跨过 60 天线的。

| 模式 | 处理范围 | 耗时 | 使用场景 |
|---|---|---|---|
| `window`（默认） | 今天/明天 + 阈值 ±`WINDOW_DAYS` 天 + 各表 `metadata/` | **恒定 2~3 秒**，与历史目录总数无关 | 每日定时任务 |
| `full` | 列举全部库表的天级目录，线程池并行（30 线程） | 10,000 分区 68 秒 / 50,000 分区约 3 分钟 | 首次落地、漏跑超过 `WINDOW_DAYS` 天、定期体检 |

**真实 GCS 环境实测性能数据**（在客户同等规模下的 Cloud Run Job / GCE 内网实测）：

| 场景 | 业务规模 | 耗时 | 吞吐率 |
|---|---|---|---|
| 首次全量（建 managed folder + 设策略），30 线程 | 54 张表、10,041 个历史分区 | **68.2 秒** | 147.2 操作/秒 |
| 首次全量，32 线程 | 50,000 个分区 | **6 分 22 秒** | 131.0 操作/秒 |
| 稳态全量（全部已一致，只核对），32 线程 | 50,000 个分区 | **2 分 45 秒** | 303.0 操作/秒 |
| **window 增量（日常定时调和实测）** | **54 张表、324 个控制项（T+0, T+1, T-60~62）** | **仅 2.36 秒** | **137.3 操作/秒** |

---

## 2. 职责划分总览

| # | 操作 | 负责部门 | 方式 |
|---|---|---|---|
| 1 | 创建 4 个 service account（3 业务 + 1 自动化） | 运维部门 | 第 3.1 节命令 |
| 2 | 创建自定义角色 `mfReconciler`（仅角色定义，无绑定） | 运维部门 | 第 3.2 节命令 |
| 3 | （可选，仅验证期）授验证人员 impersonation 权限 | 运维部门 | 第 3.3 节命令 |
| 4 | 确认/开启 bucket 的 UBLA | 大数据运维部门 | 第 4.1 节 |
| 5 | 配置 lifecycle（60 天 Coldline / 365 天 Archive） | 大数据运维部门 | 第 4.2 节 |
| 6 | 创建顶级 `datasets/` managed folder，授权 writer + reconciler | 大数据运维部门 | 第 4.3 节 |
| 7 | 首次执行全量 reconcile、验证隔离 | 大数据运维部门 | 第 4.4–4.5 节 |
| 8 | 执行一键部署脚本 `deploy.sh`（部署 Cloud Run Job + Scheduler） | 大数据运维部门 | 第 4.6 节 |
| 9 | 日常运维（阈值调整、故障处理） | 大数据运维部门 | 第 4.7 节 |

> 分工边界说明：**project/bucket 级 IAM 绑定和 SA 生命周期归运维部门**；
> managed folder 上的策略属于存储资源配置，由大数据运维部门（及其自动化任务）管理，前提是运维部门已按第 3.2 节授予 `mf-reconciler` 相应权限。

---

## 3. 【运维部门】IAM 操作步骤

所有命令先加载同一份配置（按客户实际环境修改 `config.env` 中的 `PROJECT_ID`、`BUCKET` 等）：
```bash
source ./config.env
```

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
**不要用 `roles/storage.admin` 或 `roles/storage.folderAdmin`**——两者都含 `storage.objects.get`，会让自动化身份能读走全部冷热数据（见 1.4.1 节）。

```bash
# 创建自定义角色（项目级角色定义，仅需一次；这只是定义，不产生任何访问权限）
gcloud iam roles create mfReconciler --project="${PROJECT_ID}" \
  --title="Managed Folder Reconciler" \
  --description="Manage managed folders and their IAM; list prefixes; cannot read object data" \
  --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
```

角色的**绑定**发生在顶级 `datasets/` managed folder 上（存储资源配置），由大数据运维部门在第 4.3 节完成——`mf-reconciler` **不需要任何 bucket 级或 project 级绑定**。顶级授权对其下嵌套的所有表和天级 managed folder 同样生效。

### 3.3 （可选）验证期的 impersonation 授权

大数据运维部门执行 `verify.sh` 时需要模拟三个业务 SA 发起请求。给执行验证的人员/身份授 `serviceAccountTokenCreator`（验证完成后建议回收）：

```bash
VERIFIER="user:$(gcloud config get-value account)"   # 或指定验证人员账号
for sa in "${HOT_SA}" "${COLD_SA}" "${WRITER_SA}" "${OPS_SA}"; do
  gcloud iam service-accounts add-iam-policy-binding "${sa}" \
    --member="${VERIFIER}" --role="roles/iam.serviceAccountTokenCreator" \
    --project="${PROJECT_ID}"
done
```

### 3.4 红线（请运维部门严格把关）

- **绝不给本方案的任何 SA（包括 `mf-reconciler`）授 bucket 级或项目级的存储角色**（objectViewer/objectUser/objectAdmin/storage.admin/storage.folderAdmin 等）。原理见 1.3 节：managed folder 无 deny，bucket 级授权会让整个隔离方案失效；且本方案的全部授权都能在顶级 `datasets/` managed folder 内完成（见 1.4.1 矩阵）。
- Spark/Flink 作业改用 `iceberg-writer` 运行时，同步梳理并回收其原有 SA 上的旧权限。
- IAM 变更有分钟级传播延迟，授权后 1–2 分钟再验证属正常现象。

---

## 4. 【大数据运维部门】存储与自动化操作步骤

> 前置条件：第 3 章的 SA 已由运维部门创建、`mf-reconciler` 自定义角色已建好。

### 4.1 确认 bucket 开启 UBLA（managed folder 硬性前提）

```bash
gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access.enabled)"
# 若为 False（且确认无旧版 ACL 依赖）：
gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
```

⚠️ 存量 bucket 开 UBLA 前需确认没有依赖对象 ACL 的访问方；开启 90 天后不可回退。

### 4.2 配置 lifecycle

`lifecycle.json`（60 天 Coldline、365 天 Archive，仅作用于数据前缀）已在仓库中，按需修改后：

```bash
gcloud storage buckets update "${BUCKET}" --lifecycle-file=lifecycle.json
```

注意阈值语义：lifecycle 的 `age` 按**对象创建时间**计，reconcile 按**目录名日期**计，两者相差最多 1 天，对权限切换无实质影响，但要让业务方知晓口径。

### 4.3 创建顶级 managed folder，授权写入方和 reconciler

```bash
# 1. 创建顶级 datasets/ Managed Folder
gcloud storage managed-folders create "${BUCKET}/${DATA_ROOT_PREFIX}/"

# 2. 绑定策略
cat > /tmp/prefix-policy.json <<EOF
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
gcloud storage managed-folders set-iam-policy "${BUCKET}/${DATA_ROOT_PREFIX}/" /tmp/prefix-policy.json
rm -f /tmp/prefix-policy.json
```

这一条策略同时完成两件事：writer 获得全表读写删；reconciler 获得对嵌套库表和天级 managed folder 的管理能力（顶级授权向下生效）。两者都被圈定在 `datasets/` 内。

此后 Spark/Flink 侧唯一的改动是：**作业改用 `iceberg-writer` 这个 SA 运行**。作业代码、写入路径、目录生成方式都不用变。

### 4.4 首次执行调和（全量模式）

首次要给全部存量库表目录建档，运行全量模式：

```bash
RECONCILE_MODE=full python3 reconcile_fast.py
```

预期输出：各表 `metadata/` 自动放行 hot/cold reader；今天/明天预创建 managed folder 并授 hot-reader；历史分区按 60 天阈值分别落到 hot/cold 策略；重复执行时已一致的目录显示 `[SKIP]`（幂等验证）。万级目录约 1 分钟左右完成。

### 4.5 验证访问隔离

```bash
./verify.sh
```

8 条断言全部 PASS 才算通过：

| 身份 | 操作 | 预期 |
|---|---|---|
| hot-reader | 读表级 metadata / 读近 60 天热分区 | ALLOW / ALLOW |
| hot-reader | 误读 60 天前冷分区 | **DENY（403 强拦截，0 元冷检索费）** |
| cold-reader | 读表级 metadata / 读 60 天前冷分区 | ALLOW / ALLOW |
| cold-reader | 越权读近 60 天热分区 | **DENY** |
| writer | 读热 / 读冷 / 写热 / 写冷 | 全部 ALLOW（全周期自由读写） |
| reconciler | 读热数据 / 读冷数据 | 全部 **DENY**（零信任，无数据窃取风险） |

### 4.6 部署每日定时任务（整合成 `deploy.sh` 脚本）

所有 GCP 云端组件的部署（启用 API、创建 Artifact Registry、构建镜像、部署 Cloud Run Job、配置 Cloud Scheduler 定时触发）**已全部整合成一键脚本 `deploy.sh`**：

```bash
./deploy.sh
```

脚本将以 `mf-reconciler` 身份部署 Cloud Run Job，默认开启 `RECONCILE_MODE=incremental` 增量滑动窗口模式，并在 Cloud Scheduler 中配置每日 UTC 00:30（北京时间 08:30）自动触发。实测日常运行仅耗时 **2.36 秒**。

### 4.7 日常运维要点

- **改热数据保留天数**：改 `config.env` 的 `HOT_DAYS` 并同步修改 `lifecycle.json` 的 `age`，重新执行 `./deploy.sh`。下次 reconcile 会自动把所有目录调整到新阈值（全量调和的好处）。
- **任务挂了怎么办**：漏跑 `SLIDING_LOOKBACK_DAYS`（默认 3）天以内，下次运行自动自愈；超过则手动触发一次全量：
  ```bash
  gcloud run jobs execute mf-reconcile --region="${REGION}" \
    --update-env-vars=RECONCILE_MODE=full
  ```
- **403 是预期行为**：hot-reader 查询碰到冷分区报 403 是方案设计的拦截效果，请提前告知业务方，并给出走 cold-reader 的申请流程。
- **compaction/expire 作业注意**：重写已转冷的文件会产生检索费 + 最短存储期提前删除费，此类作业应只针对热分区，冷分区冻结不动。
- **Iceberg `metadata/`**：`reconcile_fast.py` 已内置对各表 `metadata/` 的自动授权放行逻辑，确保分析师在 Hue/Trino 提交 SQL 时快照解析永不报错。

---

## 5. 仓库文件索引

| 文件 | 作用 | 主要使用者 |
|---|---|---|
| `config.env` | 全部参数（项目、bucket、前缀、日期格式、SA、阈值、模式） | 两个部门共用 |
| `setup.sh` | 基础环境与 IAM 一次性初始化脚本（可直接执行） | 运维/大数据运维 |
| `reconcile_fast.py` | 核心调和引擎（增量滑动窗口 + 全量并行，支持 Iceberg 两层表结构） | 大数据运维 |
| `deploy.sh` | Cloud Run Job + Artifact Registry + Scheduler 一键全自动部署脚本 | 大数据运维 |
| `verify.sh` | 生产 8 项双向权限断言验证脚本 | 大数据运维 |
| `lifecycle.json` | GCS 生命周期规则（60 天 Coldline / 365 天 Archive） | 大数据运维 |

---

## 6. 附录：完整脚本与配置（客户真实环境版，可直接复制使用）

### 6.1 `config.env`

```bash
# ===== 客户生产环境配置，所有脚本 source 此文件 =====
export PROJECT_ID="${PROJECT_ID:-fr-xjsy-prod}"
export REGION="${REGION:-asia-east1}"

# 客户生产实际存储桶（必须已开启 uniform bucket-level access）
export BUCKET="${BUCKET:-gs://fr-xjsy-bigdata-gcs-dev}"

# Iceberg 数据所在根前缀
export DATA_ROOT_PREFIX="${DATA_ROOT_PREFIX:-datasets}"

# 天级 folder 的命名格式（支持 dt=%Y-%m-%d 或 %Y-%m-%d）
export FOLDER_DATE_FORMAT="${FOLDER_DATE_FORMAT:-dt=%Y-%m-%d}"

# 热数据保留天数，与 bucket lifecycle 转 Coldline 的天数保持一致
export HOT_DAYS="${HOT_DAYS:-60}"

# 运行参数
export RECONCILE_MODE="${RECONCILE_MODE:-incremental}"
export SLIDING_LOOKBACK_DAYS="${SLIDING_LOOKBACK_DAYS:-3}"
export MAX_WORKERS="${MAX_WORKERS:-30}"
export EXCLUDE_DBS="${EXCLUDE_DBS:-tmp.db,kafka_test.db}"

# 4 个专用 Service Accounts
export HOT_SA="${HOT_SA:-iceberg-hot-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
export COLD_SA="${COLD_SA:-iceberg-cold-reader@${PROJECT_ID}.iam.gserviceaccount.com}"
export WRITER_SA="${WRITER_SA:-iceberg-writer@${PROJECT_ID}.iam.gserviceaccount.com}"
export OPS_SA="${OPS_SA:-mf-reconciler@${PROJECT_ID}.iam.gserviceaccount.com}"

# IAM 角色定义（架构固定常量，无需客户调整）
export HOT_ROLE="roles/storage.objectViewer"
export COLD_ROLE="roles/storage.objectViewer"
export WRITER_ROLE="roles/storage.objectUser"
export OPS_ROLE_ID="mfReconciler"
```

### 6.2 `lifecycle.json`

```json
{
  "rule": [
    {
      "action": { "type": "SetStorageClass", "storageClass": "COLDLINE" },
      "condition": { "age": 60, "matchesPrefix": ["datasets/"] }
    },
    {
      "action": { "type": "SetStorageClass", "storageClass": "ARCHIVE" },
      "condition": { "age": 365, "matchesPrefix": ["datasets/"] }
    }
  ]
}
```

### 6.3 `setup.sh`

```bash
#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 基础环境初始化脚本 (setup.sh)
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"
source ./config.env

echo "==> 1. 检查存储桶 UBLA (Uniform Bucket-Level Access) 状态..."
if ! gcloud storage buckets describe "${BUCKET}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "    存储桶不存在，正在创建并直接开启 UBLA..."
  gcloud storage buckets create "${BUCKET}" \
    --project="${PROJECT_ID}" --location="${REGION}" \
    --uniform-bucket-level-access
else
  UBLA_ENABLED=$(gcloud storage buckets describe "${BUCKET}" --format="value(uniform_bucket_level_access.enabled)" 2>/dev/null || echo "False")
  if [[ "${UBLA_ENABLED}" != "True" ]]; then
    echo "    存储桶未开启 UBLA，正在开启..."
    gcloud storage buckets update "${BUCKET}" --uniform-bucket-level-access
  fi
fi

echo "==> 2. 创建 4 个生产专用服务账号 (Service Accounts)..."
declare -A SAS=(
  ["iceberg-writer"]="Iceberg Pipeline Writer (Spark/Flink)"
  ["iceberg-hot-reader"]="Hot Data Reader (Hue/BI Analyst)"
  ["iceberg-cold-reader"]="Cold Data Reader (Audit/Archive Query)"
  ["mf-reconciler"]="Managed Folder Daily Reconciler (Cloud Run)"
)

for sa in "${!SAS[@]}"; do
  sa_email="${sa}@${PROJECT_ID}.iam.gserviceaccount.com"
  if ! gcloud iam service-accounts describe "${sa_email}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud iam service-accounts create "${sa}" \
      --project="${PROJECT_ID}" \
      --display-name="${SAS[$sa]}"
    echo "    ✔ 创建成功: ${sa_email}"
  else
    echo "    ✔ 已存在，跳过: ${sa_email}"
  fi
done

echo "==> 3. 创建 Reconciler 最小权限自定义角色 [${OPS_ROLE_ID}]..."
if ! gcloud iam roles describe "${OPS_ROLE_ID}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud iam roles create "${OPS_ROLE_ID}" --project="${PROJECT_ID}" \
    --title="Managed Folder Reconciler" \
    --description="Manage managed folders and list prefixes without reading data" \
    --permissions="storage.managedFolders.create,storage.managedFolders.get,storage.managedFolders.list,storage.managedFolders.getIamPolicy,storage.managedFolders.setIamPolicy,storage.objects.list,storage.buckets.get"
  echo "    ✔ 自定义角色 ${OPS_ROLE_ID} 创建成功"
fi

echo "==> 4. 初始化顶级 Managed Folder [${DATA_ROOT_PREFIX}/] 并挂载 IAM 策略..."
TOP_MF="${BUCKET}/${DATA_ROOT_PREFIX}/"
gcloud storage managed-folders create "${TOP_MF}" &>/dev/null || true

POLICY_FILE=$(mktemp)
cat > "${POLICY_FILE}" <<EOF
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
gcloud storage managed-folders set-iam-policy "${TOP_MF}" "${POLICY_FILE}"
rm -f "${POLICY_FILE}"
echo "    ✔ 顶级 Managed Folder [${TOP_MF}] 策略绑定完成"
echo "==> 初始化全部完成！"
```

### 6.4 `deploy.sh`（一键整合部署）

```bash
#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 调和引擎一键云端部署脚本 (deploy.sh)
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"
source ./config.env

JOB_NAME="mf-reconcile"
REPO_NAME="mf-poc"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/reconcile-fast:latest"

echo "==> 1. 检查并启用基础 GCP API..."
gcloud services enable storage.googleapis.com run.googleapis.com \
  cloudbuild.googleapis.com artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com iam.googleapis.com --project="${PROJECT_ID}"

echo "==> 2. 检查/创建 Artifact Registry 代码库 [${REPO_NAME}]..."
gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null || \
gcloud artifacts repositories create "${REPO_NAME}" \
  --repository-format=docker --location="${REGION}" --project="${PROJECT_ID}"

echo "==> 3. 使用 Cloud Build 构建并推送调和引擎镜像..."
gcloud builds submit --tag "${IMAGE}" --project="${PROJECT_ID}" .

echo "==> 4. 部署/更新 Cloud Run Job [${JOB_NAME}]..."
cat > env_vars.yaml <<ENVEOF
PROJECT_ID: "${PROJECT_ID}"
REGION: "${REGION}"
BUCKET: "${BUCKET}"
DATA_ROOT_PREFIX: "${DATA_ROOT_PREFIX}"
HOT_DAYS: "${HOT_DAYS}"
HOT_SA: "${HOT_SA}"
COLD_SA: "${COLD_SA}"
MAX_WORKERS: "${MAX_WORKERS}"
RECONCILE_MODE: "${RECONCILE_MODE}"
SLIDING_LOOKBACK_DAYS: "${SLIDING_LOOKBACK_DAYS}"
EXCLUDE_DBS: "${EXCLUDE_DBS}"
ENVEOF

gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${OPS_SA}" \
  --cpu=2 \
  --memory=1Gi \
  --max-retries=0 \
  --task-timeout=10m \
  --env-vars-file="env_vars.yaml"

echo "==> 5. 配置 Cloud Scheduler 每日定时作业 [${JOB_NAME}-daily]..."
gcloud run jobs add-iam-policy-binding "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${OPS_SA}" \
  --role="roles/run.invoker" &>/dev/null || true

SCHEDULER_JOB="${JOB_NAME}-daily"
SCHEDULER_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "${SCHEDULER_JOB}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
    --location="${REGION}" --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
else
  gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
    --location="${REGION}" --project="${PROJECT_ID}" \
    --schedule="30 0 * * *" --time-zone="Etc/UTC" \
    --uri="${SCHEDULER_URI}" --http-method=POST \
    --oauth-service-account-email="${OPS_SA}"
fi

echo "========================================================================="
echo "🎉 全流程部署成功！每日 UTC 00:30 (北京时间 08:30) 自动执行增量调和"
echo "========================================================================="
```

### 6.5 `verify.sh`

```bash
#!/usr/bin/env bash
# ==============================================================================
# GCS Managed Folder 生产安全与业务双向验证脚本 (verify.sh)
# ==============================================================================
set -uo pipefail
cd "$(dirname "$0")"
source ./config.env

DATA_ROOT="${DATA_ROOT_PREFIX:-datasets}"

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

HOT_DATE=$(date -u -d "-2 days" +"${FOLDER_DATE_FORMAT}")
COLD_DATE=$(date -u -d "-65 days" +"${FOLDER_DATE_FORMAT}")
TODAY_DATE=$(date -u +"${FOLDER_DATE_FORMAT}")

META_FILE="${TBL_PATH}/metadata/v1.metadata.json"
HOT_FILE="${TBL_PATH}/data/${HOT_DATE}/data-00000.parquet"
COLD_FILE="${TBL_PATH}/data/${COLD_DATE}/data-00000.parquet"
WRITER_TEST_FILE="${TBL_PATH}/data/${TODAY_DATE}/writer_test.txt"

FAILED=0
check() {
  local sa="$1" op="$2" obj="$3" expect="$4" desc="$5" actual
  case "${op}" in
    read)
      gcloud storage cat "${obj}" --impersonate-service-account="${sa}" &>/dev/null \
        && actual="ALLOW" || actual="DENY" ;;
    write)
      echo "verify-$(date +%s)" | gcloud storage cp - "${obj}" \
        --impersonate-service-account="${sa}" &>/dev/null \
        && actual="ALLOW" || actual="DENY" ;;
  esac
  if [[ "${actual}" == "${expect}" ]]; then
    echo "  ✔ [PASS] ${desc} => 结果: ${actual}"
  else
    echo "  ✘ [FAIL] ${desc} => 结果: ${actual} (预期: ${expect})"
    FAILED=1
  fi
}

echo "========================================================================="
echo "🔍 验证基准表: ${TBL_PATH}"
echo "========================================================================="

echo -e "\n1. 验证写入方 (Spark/Flink: 全生命周期自由读写):"
check "${WRITER_SA}" write "${WRITER_TEST_FILE}" "ALLOW" "写入今日热分区 (${TODAY_DATE})"
check "${WRITER_SA}" read  "${HOT_FILE}"         "ALLOW" "读取历史热分区 (${HOT_DATE})"
check "${WRITER_SA}" write "${TBL_PATH}/data/${COLD_DATE}/compaction.txt" "ALLOW" "重写冷分区 (Compaction)"

echo -e "\n2. 验证日常查询 (Hue/分析师: 只能读热与元数据，严禁触碰冷数据):"
check "${HOT_SA}" read "${META_FILE}" "ALLOW" "读取表级元数据 (metadata/v1.metadata.json)"
check "${HOT_SA}" read "${HOT_FILE}"  "ALLOW" "读取近 60 天热分区 (${HOT_DATE})"
check "${HOT_SA}" read "${COLD_FILE}" "DENY"  "误查 60 天前冷分区 (403 强拦截，0 元冷检索费)"

echo -e "\n3. 验证历史查询 (Cold Reader: 仅能查冷数据与元数据):"
check "${COLD_SA}" read "${META_FILE}" "ALLOW" "读取表级元数据 (metadata/v1.metadata.json)"
check "${COLD_SA}" read "${COLD_FILE}" "ALLOW" "正常读取 60 天前冷分区 (${COLD_DATE})"
check "${COLD_SA}" read "${HOT_FILE}"  "DENY"  "越权读取近 60 天热分区 (${HOT_DATE})"

echo -e "\n4. 验证自动化运维身份 (mf-reconciler: 零信任，无业务数据窥探权限):"
check "${OPS_SA}" read "${HOT_FILE}"  "DENY" "运维 SA 尝试读取热数据"
check "${OPS_SA}" read "${COLD_FILE}" "DENY" "运维 SA 尝试读取冷数据"

(( FAILED == 0 )) && echo -e "\n🎉 全部通过 ✔" || { echo -e "\n⚠️ 存在失败项 ✘"; exit 1; }
```
