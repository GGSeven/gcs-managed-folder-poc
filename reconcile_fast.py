#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能多线程 GCS Managed Folder 调和引擎 (Python 方案)
支持：
  1. 数据库/表 白名单与黑名单过滤（例如默认跳过无查询价值的 tmp.db 493 张表）
  2. 20 线程并发 HTTP 连接池加速（5 万个分区可在数分钟内调和完毕）
  3. 自动检测环境获取认证 Token（支持本地 gcloud、Cloud Shell 与 Cloud Run 容器）
"""

import os
import sys
import time
import json
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("错误: 缺少 requests 库，请先执行: pip install requests")
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

# 并发线程数（推荐 10~20，充分利用 GCS 高并发）
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "20"))

# 过滤规则：
# 1. 黑名单（默认跳过 ETL 临时表 tmp.db，瞬间砍掉 50% 无效工作量）
EXCLUDE_DBS = set(filter(None, os.environ.get("EXCLUDE_DBS", "tmp.db,kafka_test.db").split(",")))
# 2. 白名单（如果指定，则只处理指定库；留空则处理除黑名单外的所有库）
INCLUDE_DBS = set(filter(None, os.environ.get("INCLUDE_DBS", "").split(",")))

API_BASE = f"https://storage.googleapis.com/storage/v1/b/{BUCKET_NAME}"


# ==============================
# 认证与 HTTP 连接池
# ==============================
def get_access_token():
    # 0. 优先支持环境变量直接传入
    env_token = os.environ.get("GCS_TOKEN") or os.environ.get("ACCESS_TOKEN")
    if env_token:
        return env_token.strip()

    # 1. 优先尝试从元数据服务器获取（容器/GCP Linux 环境）
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

    # 2. 从本地 / Cloud Shell gcloud 获取
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
    """毫秒级枚举子目录（带 delimiter=/，不枚举底层文件）"""
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


# ==============================
# 多线程任务分发
# ==============================
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


def main():
    print("=========================================================================")
    print(f"🚀 启动高性能 Python 多线程调和引擎 (并发线程数: {MAX_WORKERS})")
    print(f"   项目: {PROJECT_ID} | 存储桶: {BUCKET_NAME}")
    print(f"   根目录: {DATA_ROOT_PREFIX}/ | 隔离天数阈值: {HOT_DAYS} 天")
    print(f"   黑名单跳过数据库: {list(EXCLUDE_DBS) if EXCLUDE_DBS else '无'}")
    print(f"   白名单处理数据库: {list(INCLUDE_DBS) if INCLUDE_DBS else '全部允许'}")
    print("=========================================================================")

    token = get_access_token()
    session = build_http_session(token)

    start_total = time.time()

    # 1. 扫描数据库
    dbs = list_sub_prefixes(session, f"{DATA_ROOT_PREFIX}/")
    print(f"==> [1/3] 发现总数据库数: {len(dbs)}")

    selected_tables = []
    skipped_dbs = []

    for db in dbs:
        db_name = db.rstrip("/").split("/")[-1]
        
        # 过滤数据库
        if INCLUDE_DBS and db_name not in INCLUDE_DBS:
            skipped_dbs.append(db_name)
            continue
        if db_name in EXCLUDE_DBS:
            skipped_dbs.append(db_name)
            continue

        tables = list_sub_prefixes(session, db)
        print(f"  📁 数据库 [{db_name}]: 发现 {len(tables)} 张表")
        selected_tables.extend(tables)

    if skipped_dbs:
        print(f"  ⏭️ 跳过未选中/黑名单数据库: {skipped_dbs}")

    print(f"\n==> [2/3] 待处理数据表总计: {len(selected_tables)} 张")

    # 2. 处理各表的 metadata/ 和今天/明天的预创建
    all_partitions = []
    print("  --> 正在为所有表配置 metadata 读权限及今明两天写入分区...")
    
    for tbl in selected_tables:
        # A. metadata/
        meta_path = f"{tbl}metadata/"
        ensure_managed_folder(session, meta_path)
        set_managed_folder_iam(session, meta_path, [HOT_SA, COLD_SA])

        # B. 预创建今天与明天
        for offset in [0, 1]:
            d_str = datetime.fromtimestamp(TODAY_EPOCH + offset * 86400, timezone.utc).strftime("dt=%Y-%m-%d")
            today_path = f"{tbl}data/{d_str}/"
            ensure_managed_folder(session, today_path)
            set_managed_folder_iam(session, today_path, [HOT_SA])

        # C. 收集历史分区
        parts = list_sub_prefixes(session, f"{tbl}data/")
        all_partitions.extend(parts)

    print(f"\n==> [3/3] 收集到全量分区总计: {len(all_partitions)} 个，启动 {MAX_WORKERS} 线程池并发调和...")
    
    success_count = 0
    start_par = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_partition_task, session, p): p for p in all_partitions}
        for f in as_completed(futures):
            try:
                name, status = f.result()
                success_count += 1
                if success_count % 100 == 0 or success_count == len(all_partitions):
                    print(f"    进度: [{success_count}/{len(all_partitions)}] 已处理...")
            except Exception as e:
                p_url = futures[f]
                print(f"    [FAIL] {p_url} 异常: {e}")

    elapsed_par = time.time() - start_par
    elapsed_total = time.time() - start_total

    print("\n=========================================================================")
    print(f"🎉 调和完成！")
    print(f"   处理分区数: {len(all_partitions)} 个")
    print(f"   并发处理耗时: {elapsed_par:.2f} 秒 (平均处理速度: {len(all_partitions)/max(elapsed_par,0.1):.1f} 分区/秒)")
    print(f"   全流程总耗时: {elapsed_total:.2f} 秒 ✔")
    print("=========================================================================")


if __name__ == "__main__":
    main()
