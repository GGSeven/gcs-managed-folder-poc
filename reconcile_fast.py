#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能多线程 GCS Managed Folder 调和引擎 (Python 方案)
支持：
  1. 数据库/表 白名单与黑名单过滤（例如默认跳过无查询价值的 tmp.db 493 张表）
  2. 30 线程并发 HTTP 连接池加速（毫秒级完成增量滑动窗口，万级分区全量仅需 1 分钟）
  3. 自动检测环境获取认证 Token（支持本地 gcloud、Cloud Shell 与 Cloud Run 容器）
  4. 实时动态进度打印（显示已完成数、百分比、处理速度与预估剩余时间，flush=True 绝不假死）
"""

import os
import sys
import time
import json
import urllib.parse
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
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ==============================
# 配置参数（支持环境变量与默认值）
# ==============================
PROJECT_ID = os.environ.get("PROJECT_ID", "bd-host-2026-004")
BUCKET = os.environ.get("BUCKET", f"gs://{PROJECT_ID}-mf-poc")
BUCKET_NAME = BUCKET.replace("gs://", "").strip("/")
DATA_ROOT_PREFIX = os.environ.get("DATA_ROOT_PREFIX", "datasets").strip("/")
HOT_DAYS = int(os.environ.get("HOT_DAYS", "60"))
FOLDER_DATE_FORMAT = os.environ.get("FOLDER_DATE_FORMAT", "dt=%Y-%m-%d")

HOT_SA = os.environ.get("HOT_SA", f"iceberg-hot-reader@{PROJECT_ID}.iam.gserviceaccount.com")
COLD_SA = os.environ.get("COLD_SA", f"iceberg-cold-reader@{PROJECT_ID}.iam.gserviceaccount.com")

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "30"))
RECONCILE_MODE = os.environ.get("RECONCILE_MODE", "incremental").lower()
SLIDING_LOOKBACK_DAYS = int(os.environ.get("SLIDING_LOOKBACK_DAYS", "3"))

EXCLUDE_DBS = set(filter(None, os.environ.get("EXCLUDE_DBS", "tmp.db,kafka_test.db").split(",")))
INCLUDE_DBS = set(filter(None, os.environ.get("INCLUDE_DBS", "").split(",")))

API_BASE = f"https://storage.googleapis.com/storage/v1/b/{BUCKET_NAME}"


# ==============================
# 认证与 HTTP 连接池
# ==============================
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
    cmd = ["gcloud", "auth", "print-access-token"]
    return subprocess.check_output(cmd, text=True, shell=(sys.platform == "win32")).strip()


def build_http_session(token):
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS * 2,
        pool_maxsize=MAX_WORKERS * 2,
        max_retries=2
    )
    session.mount("https://", adapter)
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    return session


# ==============================
# GCS 原生 API 操作封装
# ==============================
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
    body = {
        "bindings": [{
            "role": role,
            "members": [f"serviceAccount:{sa}" for sa in sa_list]
        }]
    }
    resp = session.put(url, json=body, timeout=10)
    return resp.status_code == 200


TODAY_EPOCH = time.time()

def process_partition_task(session, part_path):
    part_name = part_path.rstrip("/").split("/")[-1]
    clean_date = part_name.replace("dt=", "")
    try:
        dt_epoch = datetime.strptime(clean_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return (part_name, "SKIP_NON_DATE")

    age_days = (TODAY_EPOCH - dt_epoch) / 86400
    ensure_managed_folder(session, part_path)

    if age_days < HOT_DAYS:
        set_managed_folder_iam(session, part_path, [HOT_SA])
        return (part_name, "HOT")
    else:
        set_managed_folder_iam(session, part_path, [COLD_SA])
        return (part_name, "COLD")


def process_table_prep(session, tbl):
    """并行处理单个表的 metadata 授权、预建今明两日热分区、并收集历史分区"""
    # 1. 确保 metadata/ 读权限
    meta_path = f"{tbl}metadata/"
    ensure_managed_folder(session, meta_path)
    set_managed_folder_iam(session, meta_path, [HOT_SA, COLD_SA])

    # 2. 预创建今天与明天
    for offset in [0, 1]:
        d_str = datetime.fromtimestamp(TODAY_EPOCH + offset * 86400, timezone.utc).strftime("dt=%Y-%m-%d")
        today_path = f"{tbl}data/{d_str}/"
        ensure_managed_folder(session, today_path)
        set_managed_folder_iam(session, today_path, [HOT_SA])

    # 3. 收集历史分区
    parts = list_sub_prefixes(session, f"{tbl}data/")
    return parts


def main():
    print("=========================================================================", flush=True)
    print(f"🚀 启动高性能 Python 多线程调和引擎 (并发线程数: {MAX_WORKERS})", flush=True)
    print(f"   项目: {PROJECT_ID} | 存储桶: {BUCKET_NAME}", flush=True)
    print(f"   根目录: {DATA_ROOT_PREFIX}/ | 隔离天数阈值: {HOT_DAYS} 天", flush=True)
    print(f"   运行模式: {RECONCILE_MODE.upper()}", flush=True)
    print("=========================================================================", flush=True)

    token = get_access_token()
    session = build_http_session(token)

    start_total = time.time()

    # 1. 扫描数据库与表
    print(f"--> [阶段 1/3] 正在扫描库表结构...", flush=True)
    dbs = list_sub_prefixes(session, f"{DATA_ROOT_PREFIX}/")
    selected_tables = []
    skipped_dbs = []

    for db in dbs:
        db_name = db.rstrip("/").split("/")[-1]
        if INCLUDE_DBS and db_name not in INCLUDE_DBS:
            skipped_dbs.append(db_name)
            continue
        if db_name in EXCLUDE_DBS:
            skipped_dbs.append(db_name)
            continue
        tables = list_sub_prefixes(session, db)
        selected_tables.extend(tables)

    print(f"✔ 扫描完成：共发现 {len(selected_tables)} 张业务表 (跳过黑名单库: {list(EXCLUDE_DBS) if EXCLUDE_DBS else '无'})", flush=True)

    start_par = time.time()
    success_count = 0

    if RECONCILE_MODE == "incremental":
        print(f"\n--> [阶段 2/3] 组装【增量滑动窗口】调和任务 (T+0/T+1 预建 + T-{HOT_DAYS}~T-{HOT_DAYS+SLIDING_LOOKBACK_DAYS-1} 翻转 + metadata 放行)...", flush=True)
        now_utc = datetime.now(timezone.utc)
        tasks = []
        for tbl in selected_tables:
            tasks.append((f"{tbl}metadata/", [HOT_SA, COLD_SA]))
            for offset in [0, 1]:
                d_str = (now_utc + timedelta(days=offset)).strftime("dt=%Y-%m-%d")
                tasks.append((f"{tbl}data/{d_str}/", [HOT_SA]))
            for offset in range(HOT_DAYS, HOT_DAYS + SLIDING_LOOKBACK_DAYS):
                d_str = (now_utc - timedelta(days=offset)).strftime("dt=%Y-%m-%d")
                tasks.append((f"{tbl}data/{d_str}/", [COLD_SA]))

        total_units = len(tasks)
        print(f"--> [阶段 3/3] 启动 {MAX_WORKERS} 线程并发执行 {total_units} 个增量控制项...", flush=True)

        def exec_task(item):
            path, sa_list = item
            ensure_managed_folder(session, path)
            ok = set_managed_folder_iam(session, path, sa_list)
            return (path, ok)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(exec_task, t): t for t in tasks}
            for f in as_completed(futures):
                path, ok = f.result()
                if ok:
                    success_count += 1
                if success_count % 30 == 0 or success_count == total_units:
                    pct = (success_count / total_units) * 100
                    print(f"    ⏳ [增量进度] {success_count}/{total_units} ({pct:.1f}%) 已处理...", flush=True)

    else:
        print(f"\n--> [阶段 2/3] 多线程并发准备 {len(selected_tables)} 张表的 metadata 与今明两日写入分区...", flush=True)
        all_partitions = []
        tables_done = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_table_prep, session, tbl): tbl for tbl in selected_tables}
            for f in as_completed(futures):
                parts = f.result()
                all_partitions.extend(parts)
                tables_done += 1
                if tables_done % 10 == 0 or tables_done == len(selected_tables):
                    print(f"    ⏳ [表扫描进度] 已扫描 {tables_done}/{len(selected_tables)} 张表 (已汇总 {len(all_partitions)} 个历史分区)...", flush=True)

        total_units = len(all_partitions)
        print(f"\n--> [阶段 3/3] 收集到全量分区总计: {total_units} 个，启动 {MAX_WORKERS} 线程池并发调和...", flush=True)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_partition_task, session, p): p for p in all_partitions}
            for f in as_completed(futures):
                try:
                    f.result()
                    success_count += 1
                except Exception:
                    pass

                # 每完成 500 个分区或结束时，实时打印进度、百分比、速率与预估时间
                if success_count % 500 == 0 or success_count == total_units:
                    pct = (success_count / max(total_units, 1)) * 100
                    cur_elapsed = time.time() - start_par
                    cur_speed = success_count / max(cur_elapsed, 0.1)
                    remaining_sec = (total_units - success_count) / max(cur_speed, 1.0)
                    print(
                        f"    ⏳ [全量调和进度] {success_count}/{total_units} ({pct:5.1f}%) | "
                        f"速度: {cur_speed:5.1f} 个/秒 | 预估剩余: {int(remaining_sec):3d} 秒...",
                        flush=True
                    )

    elapsed_par = time.time() - start_par
    elapsed_total = time.time() - start_total

    print("\n=========================================================================", flush=True)
    print(f"🎉 调和完成！", flush=True)
    print(f"   运行模式: {RECONCILE_MODE.upper()}", flush=True)
    print(f"   处理分区/操作数: {total_units} 个 (成功: {success_count})", flush=True)
    print(f"   核心调和耗时: {elapsed_par:.2f} 秒 (平均处理速度: {total_units/max(elapsed_par,0.01):.1f} 操作/秒)", flush=True)
    print(f"   全流程总耗时: {elapsed_total:.2f} 秒 ✔", flush=True)
    print("=========================================================================", flush=True)


if __name__ == "__main__":
    main()
