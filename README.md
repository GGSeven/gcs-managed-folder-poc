# GCS Managed Folder 热/冷数据访问隔离 POC

> 📖 **给运维部门 / 大数据运维部门的分工操作手册见 [OPERATIONS.md](OPERATIONS.md)**。
> 使用前先修改 `config.env` 中的 `PROJECT_ID`、`REGION`、`DATA_PREFIX`。

## 背景与真实痛点

客户 bucket 下 `.../odc.db/` 每天由 Spark/Flink 任务生成一个天级 folder，存放 Iceberg 数据。
Lifecycle 已配置 60 天转 Coldline、更久转 Archive。

**真实痛点是成本**：曾有员工执行 SQL 误扫大量冷数据，产生高额检索费。
诉求确认为：**Spark/Flink（写入管道）冷热数据均可读写；人工和查询任务只读热数据、
物理上碰不到冷数据**。

## 方案结论（评估摘要）

**Managed Folder 按天授权 + 每日 reconcile 定时任务：可行，且是当前场景下的最优解。**

成本控制原理：Coldline/Archive 的检索费按实际取回的字节数收，而 IAM 的 403 拒绝发生在
**任何字节被取回之前**。误查冷数据 = 查询当场报错 + 0 费用。这是存储层唯一的"硬拦截"手段
（存储类别本身不影响可读性，只影响计费）。

## 授权模型（三类身份）

| 身份 | 用途 | 授权位置 | 角色 | 是否参与热冷切换 |
|---|---|---|---|---|
| `iceberg-writer` | Spark/Flink 写入管道 | `odc.db/` 表前缀级 managed folder | objectUser（读写删） | 否，长期固定 |
| `iceberg-hot-reader` | 人工/查询任务 | 近 60 天的天级 managed folder | objectViewer | 是，每日调和 |
| `iceberg-cold-reader` | 少数授权的历史查询 | 60 天以前的天级 managed folder | objectViewer | 是，每日调和 |

要点：
- **所有业务 SA 都不授 bucket 级对象权限**。managed folder IAM 是叠加(additive)的且不支持
  deny，bucket 级授权会让隔离整体失效。写入方的整表权限授在 `odc.db/` 前缀级 managed
  folder 上，范围被圈死在表内，bucket 里其他业务数据碰不到。
- **为什么写入方不能按天授权**：Iceberg 每次提交要读写共享的 `metadata/`（metadata.json、
  manifest）；compaction、expire snapshots、Flink upsert 会跨天读写删历史文件。按天授权
  会让管道第一步就 403。
- 嵌套 managed folder 权限叠加：前缀级的 writer 授权与天级的 reader 授权互不干扰。

## 架构

```
Cloud Scheduler (每日 00:30 UTC)
      │
      ▼
Cloud Run Job (reconcile.sh, 运维 SA: 表前缀级自定义角色 mfReconciler，无数据读取权限)
      │
      ├─ 预创建"今天+明天"的 managed folder，授权 hot reader
      │  （managed folder 可先于对象存在 → Spark/Flink 写任务无需感知、无时序依赖）
      └─ 扫描全部天级 folder：
           folder 日期距今 < 60 天 → IAM = hot reader (objectViewer)
           folder 日期距今 ≥ 60 天 → IAM = cold reader (objectViewer)
```

关键设计：**不检测 storage class 变化，只按 folder 名中的日期做调和(reconcile)**。
IAM 切换与 lifecycle 转储是两件独立的事，只需天数阈值对齐（都是 60）。
每次运行全量扫描、先 diff 再写，漏跑/失败后重跑均可自愈，天然幂等。

## 文件

| 文件 | 作用 |
|---|---|
| `config.env` | 全部参数（项目、bucket、前缀、日期格式、三个 SA、阈值） |
| `setup.sh` | 一次性初始化：bucket(UBLA)、lifecycle、三个 SA、writer 前缀授权、模拟数据 |
| `reconcile.sh` | 核心：每日调和天级 managed folder 的存在性与 reader IAM |
| `verify.sh` | 用 SA impersonation 验证 8 种访问组合（4 条 reader 隔离 + 4 条 writer 全通） |
| `deploy/` | 生产化：Cloud Run Job + Cloud Scheduler |

## 运行步骤

```bash
./setup.sh        # 初始化环境与模拟数据（跨 60 天阈值两侧的 folder）
./reconcile.sh    # 执行一次调和
./verify.sh       # 验证隔离（需先授 serviceAccountTokenCreator，见脚本头注释）
deploy/deploy.sh  # （可选）部署为每日定时任务
```

## 备选方案对比

### Autoclass（纯成本手段，无权限治理）

若客户只要"防账单意外"不要权限隔离，可对比 bucket 级 Autoclass：自动按访问频率转储，
读任何层级**不收检索费**，零运维。代价：管理费 $0.0025/千对象/月（Iceberg 小文件多需
估算）、被误读的对象会升回 Standard（一段时间存储费变高）、与现有 lifecycle 转储规则互斥。
本客户明确要"人工/查询碰不到冷数据"，故 IAM 方案更贴合，Autoclass 仅作备选。

### 为什么不把冷数据搬到另一个桶

- **时机 A（转冷后搬）最贵**：复制冷对象要收检索费（Coldline ≈$0.02/GB、Archive ≈$0.05/GB）
  + 最短存储期（90/365 天）的提前删除费 + 每对象操作费。
- **时机 B（第 60 天仍是 Standard 时用 STS 搬到同 region、默认 Coldline 的目标桶）**：
  只剩每对象操作费，但——
- **决定性缺陷**：Iceberg metadata 记录**带 bucket 名的绝对路径**，跨桶搬迁即断表。
  要么自行重写元数据（无官方工具、每日作业、风险高），要么历史数据脱表（查询需 union 两张表）。
- 搬桶想要的隔离效果，managed folder 原地零搬迁成本即可提供（数据一字节不动、不断表、
  无检索费无提前删除费）。仅当数据是非 Iceberg 普通文件时才值得考虑 STS 搬桶。

### 其他排除项

- **IAM Conditions**：CEL 表达不了"路径日期距今 60 天内"的动态条件。
- **事件驱动监听转储事件**：转储是对象级、时间分散，且 IAM 切换本就不依赖转储发生，反而更复杂。

## 辅助防护（建议叠加）

- 查询引擎层强制分区过滤（BigQuery/BigLake 有 `require_partition_filter`；需确认客户引擎）。
- Billing 预算告警兜底；Cloud Monitoring 对 reconcile Job 失败告警。

## ⚠️ 与客户确认清单

1. **Iceberg 目录结构**：`metadata/` 与天级数据 folder 的相对位置。若 metadata 是表级共享
   目录且被 lifecycle 一并转冷，hot reader 读表做查询规划时需要 metadata 的读权限——
   需给 hot/cold reader 都授 metadata 所在路径的读权限（文件小，检索费可忽略）。
2. **查询模式**：hot reader 的查询是否严格分区裁剪。误查冷分区会报 403（这正是想要的
   拦截效果），但需让业务方知晓这是预期行为而非故障。
3. **阈值语义**：lifecycle 的 `age` 按对象创建时间计，reconcile 按 folder 名日期计，
   两者可能相差 1 天内；需明确以哪个为准。
4. **管道现状**：Spark/Flink 当前使用的 SA 及已有权限；是否有 upsert/compaction/expire
   作业。注意：compaction 若重写已转冷的文件，会产生检索费 + 最短存储期提前删除费，
   建议此类作业只针对热分区，冷分区冻结不动。
