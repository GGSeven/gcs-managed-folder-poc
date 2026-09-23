#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
企业级高性能 GCS Managed Folder 调和引擎 (多线程 + 全量/增量双模 + 多级嵌套分区)
"""
import os, sys, time, json, urllib.parse, re
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("错误: 缺少 requests 库，请先执行: pip install requests", flush=True)
    sys.exit(1)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# ------------------------------------------------------------------------------
# 基础配置与环境变量
# ------------------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "bd-host-2026-004")
BUCKET = os.environ.get("BUCKET", f"gs://{PROJECT_ID}-mf-poc")
BUCKET_NAME = BUCKET.replace("gs://", "").strip("/")
DEFAULT_HOT_DAYS = int(os.environ.get("DEFAULT_HOT_DAYS", os.environ.get("HOT_DAYS", "60")))
RECONCILE_MODE = os.environ.get("RECONCILE_MODE", "full").lower()
SLIDING_LOOKBACK_DAYS = int(os.environ.get("SLIDING_LOOKBACK_DAYS", "3"))

# 表级差异化保留期映射
DEFAULT_TABLE_RULES = {
    "ods_can_origin_data_1s_p_t_i": 60,
    "ods_can_parse_1s_p_t_i": 90,
}
TABLE_RULES = dict(DEFAULT_TABLE_RULES)
raw_rules = os.environ.get("TABLE_RETENTION", "")
if raw_rules:
    for item in re.split(r"[,;]", raw_rules):
        if ":" in item:
            k, v = item.strip().split(":")
            TABLE_RULES[k.strip()] = int(v.strip())

# 精准白名单表
DEFAULT_TARGET_TABLES = [
    "datasets/ods.db/ods_can_origin_data_1s_p_t_i",
    "datasets/ods.db/ods_can_parse_1s_p_t_i"
]
raw_tables = os.environ.get("TARGET_TABLES", "")
if raw_tables:
    TARGET_TABLES = [t.strip().strip("/") for t in re.split(r"[,;]", raw_tables) if t.strip()]
else:
    TARGET_TABLES = DEFAULT_TARGET_TABLES

HOT_SA = os.environ.get("HOT_SA", f"iceberg-hot-reader@{PROJECT_ID}.iam.gserviceaccount.com")
COLD_SA = os.environ.get("COLD_SA", f"iceberg-cold-reader@{PROJECT_ID}.iam.gserviceaccount.com")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "30"))

API_BASE = f"https://storage.googleapis.com/storage/v1/b/{BUCKET_NAME}"
DATE_REGEX = re.compile(r"server_dt_utc=(\d{4}-\d{2}-\d{2})")

AUX_DIRECTORIES = {
    "user/spark/": ("roles/storage.objectViewer", [HOT_SA]),
    "flink_jar/": ("roles/storage.objectViewer", [HOT_SA]),
    "spark-job-history/": ("roles/storage.objectUser", [HOT_SA]),
    "spark-tmp/": ("roles/storage.objectUser", [HOT_SA]),
}

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
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS * 2,
        pool_maxsize=MAX_WORKERS * 2,
        max_retries=3
    )
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
        resp = session.get(url, timeout=12)
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

def find_date_partitions_recursive(session, base_prefix, depth=1, max_depth=4):
    """递归发现日期分区，遇到包含 dt= 或 server_dt_utc= 的目录作为边界停止下探"""
    results = []
    sub_prefixes = list_sub_prefixes(session, base_prefix)
    for p in sub_prefixes:
        match = DATE_REGEX.search(p)
        if match:
            results.append(p)
        elif depth < max_depth:
            results.extend(find_date_partitions_recursive(session, p, depth + 1, max_depth))
    return results

def reconcile_single_partition_task(args):
    session, part_path, target_sa, role = args
    ensure_managed_folder(session, part_path)
    ok = set_managed_folder_iam(session, part_path, [target_sa], role)
    return part_path, ok

def run_reconcile():
    print("=========================================================================", flush=True)
    print(f"🚀 企业级 GCS Managed Folder 调和引擎启动 (30线程并发加速)", flush=True)
    print(f"   项目: {PROJECT_ID} | 存储桶: {BUCKET_NAME}", flush=True)
    print(f"   运行模式: {RECONCILE_MODE.upper()}", flush=True)
    print(f"   管控表清单: {TARGET_TABLES}", flush=True)
    print(f"   保留期规则映射: {TABLE_RULES} (默认兜底: {DEFAULT_HOT_DAYS} 天)", flush=True)
    print("=========================================================================", flush=True)

    token = get_access_token()
    session = build_http_session(token)
    start_time = time.time()

    # 1. Spark/Kyuubi 辅助目录检查与放行
    print("\n--> [阶段 1/3] 检查并配置 Spark/Kyuubi 辅助依赖与日志临时目录...", flush=True)
    for aux_path, (role, sa_list) in AUX_DIRECTORIES.items():
        ensure_managed_folder(session, aux_path)
        ok = set_managed_folder_iam(session, aux_path, sa_list, role)
        print(f"    ✔ 辅助目录 [{aux_path}] -> 角色: {role} (状态: {'OK' if ok else 'FAIL'})", flush=True)

    # 2. 组装调和任务 (区分全量与增量模式)
    tasks = [] # [(session, part_path, sa, role)]
    today_epoch = time.time()
    now_utc = datetime.now(timezone.utc)

    if RECONCILE_MODE == "incremental":
        print(f"\n--> [阶段 2/3] 组装【增量滑动窗口】调和任务 (极速模式，跳过历史扫描)...", flush=True)
        for tbl_prefix in TARGET_TABLES:
            tbl_path = f"{tbl_prefix}/" if not tbl_prefix.endswith("/") else tbl_prefix
            tbl_name = tbl_path.rstrip("/").split("/")[-1]
            hot_threshold = TABLE_RULES.get(tbl_name, DEFAULT_HOT_DAYS)

            # 元数据常驻只读
            tasks.append((session, f"{tbl_path}metadata/", HOT_SA, "roles/storage.objectViewer"))
            tasks.append((session, f"{tbl_path}metadata/", COLD_SA, "roles/storage.objectViewer"))

            # 探测该表下的一级分区前缀 (如 country_code=XX/ 或直接是日期)
            sub_prefixes = list_sub_prefixes(session, f"{tbl_path}data/")
            parent_prefixes = []
            if any(DATE_REGEX.search(p) for p in sub_prefixes):
                parent_prefixes.append(f"{tbl_path}data/")
            else:
                parent_prefixes.extend(sub_prefixes)

            for parent in parent_prefixes:
                # T+0 (今天) 与 T+1 (明天) 预建放行
                for offset in [0, 1]:
                    d_str = (now_utc + timedelta(days=offset)).strftime("%Y-%m-%d")
                    p_path = f"{parent}server_dt_utc={d_str}/"
                    tasks.append((session, p_path, HOT_SA, "roles/storage.objectViewer"))
                # T-(hot_days) 到 T-(hot_days + lookback) 翻转为冷数据拦截
                for offset in range(hot_threshold, hot_threshold + SLIDING_LOOKBACK_DAYS):
                    d_str = (now_utc - timedelta(days=offset)).strftime("%Y-%m-%d")
                    p_path = f"{parent}server_dt_utc={d_str}/"
                    tasks.append((session, p_path, COLD_SA, "roles/storage.objectViewer"))

    else:
        # FULL 模式：递归扫描两张目标表的全部历史分区
        print(f"\n--> [阶段 2/3] 扫描目标表全量分区结构 (全量递归扫描)...", flush=True)
        for tbl_prefix in TARGET_TABLES:
            tbl_path = f"{tbl_prefix}/" if not tbl_prefix.endswith("/") else tbl_prefix
            tbl_name = tbl_path.rstrip("/").split("/")[-1]
            hot_threshold = TABLE_RULES.get(tbl_name, DEFAULT_HOT_DAYS)

            tasks.append((session, f"{tbl_path}metadata/", HOT_SA, "roles/storage.objectViewer"))
            tasks.append((session, f"{tbl_path}metadata/", COLD_SA, "roles/storage.objectViewer"))

            date_parts = find_date_partitions_recursive(session, f"{tbl_path}data/")
            print(f"    ✔ 目标表 [{tbl_name}] 发现 {len(date_parts)} 个历史分区 (阈值: {hot_threshold} 天)", flush=True)

            for p in date_parts:
                match = DATE_REGEX.search(p)
                if not match:
                    continue
                d_str = match.group(1)
                try:
                    dt_epoch = datetime.strptime(d_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
                except ValueError:
                    continue
                age_days = (today_epoch - dt_epoch) / 86400
                if age_days < hot_threshold:
                    tasks.append((session, p, HOT_SA, "roles/storage.objectViewer"))
                else:
                    tasks.append((session, p, COLD_SA, "roles/storage.objectViewer"))

    # 3. 30 线程并发执行调和任务
    total_tasks = len(tasks)
    print(f"\n--> [阶段 3/3] 启动 {MAX_WORKERS} 线程池并发调和 {total_tasks} 个分区控制项...", flush=True)
    success_count = 0
    start_worker_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(reconcile_single_partition_task, t): t for t in tasks}
        for f in as_completed(futures):
            try:
                part_path, ok = f.result()
                if ok:
                    success_count += 1
            except Exception:
                pass

            if success_count % 50 == 0 or success_count == total_tasks:
                elapsed = time.time() - start_worker_time
                pct = (success_count / max(total_tasks, 1)) * 100
                speed = success_count / max(elapsed, 0.05)
                eta = (total_tasks - success_count) / max(speed, 1.0)
                print(f"    ⏳ [{RECONCILE_MODE.upper()}调和进度] {success_count}/{total_tasks} ({pct:5.1f}%) | 速度: {speed:5.1f} 个/秒 | 预估剩余: {int(eta):2d} 秒", flush=True)

    elapsed_total = time.time() - start_time
    print("\n=========================================================================", flush=True)
    print(f"🎉 调和完成！模式: {RECONCILE_MODE.upper()}", flush=True)
    print(f"   成功处理分区/目录数: {success_count} / {total_tasks}", flush=True)
    print(f"   总耗时: {elapsed_total:.2f} 秒 (并发速率: {total_tasks/max(elapsed_total, 0.01):.1f} 操作/秒)", flush=True)
    print("=========================================================================", flush=True)

if __name__ == "__main__":
    run_reconcile()
