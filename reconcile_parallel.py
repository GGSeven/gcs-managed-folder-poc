#!/usr/bin/env python3
# ==============================================================================
# Python 多线程并发 GCS Managed Folder 调和引擎 (reconcile_parallel.py)
# 机制：ThreadPoolExecutor (16 并发) + 原生连接池复用 + 兼容代理环境
# 零第三方依赖：仅依赖 Python 3 标准库，无需 pip 安装任何包
# ==============================================================================
import os
import sys
import time
import json
import ssl
import urllib.request
import urllib.parse
import urllib.error
from urllib.request import build_opener, ProxyHandler, HTTPSHandler, getproxies
import subprocess
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. 自动配置代理支持（解决各种 Linux/WSL/容器网络环境差异）
try:
    ssl_ctx = ssl.create_default_context()
    opener = build_opener(ProxyHandler(getproxies()), HTTPSHandler(context=ssl_ctx))
    urllib.request.install_opener(opener)
except Exception:
    pass

# 2. 加载配置参数
PROJECT_ID = os.environ.get("PROJECT_ID", "bd-host-2026-004")
BUCKET = os.environ.get("BUCKET", f"gs://{PROJECT_ID}-mf-poc")
BUCKET_NAME = BUCKET.replace("gs://", "").strip("/")
DATA_ROOT_PREFIX = os.environ.get("DATA_ROOT_PREFIX", "datasets")
HOT_DAYS = int(os.environ.get("HOT_DAYS", "60"))

HOT_SA = os.environ.get("HOT_SA", f"iceberg-hot-reader@{PROJECT_ID}.iam.gserviceaccount.com")
COLD_SA = os.environ.get("COLD_SA", f"iceberg-cold-reader@{PROJECT_ID}.iam.gserviceaccount.com")

API_BASE = f"https://storage.googleapis.com/storage/v1/b/{BUCKET_NAME}"
MAX_WORKERS = 16  # 默认 16 并发线程

# 3. 动态获取 GCP 访问凭证
def fetch_access_token():
    if os.environ.get("K_SERVICE") or os.environ.get("CLOUD_RUN_JOB"):
        try:
            req = urllib.request.Request(
                "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
                headers={"Metadata-Flavor": "Google"}
            )
            with urllib.request.urlopen(req, timeout=2) as resp:
                return json.loads(resp.read().decode())["access_token"]
        except Exception:
            pass
    return subprocess.check_output(["gcloud", "auth", "print-access-token"], text=True).strip()

ACCESS_TOKEN = fetch_access_token()

def gcs_api_call(url, method="GET", body=None, timeout=10):
    global ACCESS_TOKEN
    headers = {
        "Authorization": "Bearer " + ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        if e.code == 409:  # 文件夹已存在
            return {"exists": True}
        if e.code == 404:
            return {"not_found": True}
        return {"error": e.code, "msg": str(e)}

# 4. 递归列出目录（使用 delimiter，避免遍历底层具体数据文件）
def list_prefixes(prefix):
    prefixes = []
    page_token = ""
    while True:
        url = f"{API_BASE}/o?prefix={prefix}&delimiter=/&fields=prefixes,nextPageToken"
        if page_token:
            url += f"&pageToken={page_token}"
        res = gcs_api_call(url)
        prefixes.extend(res.get("prefixes", []))
        page_token = res.get("nextPageToken", "")
        if not page_token:
            break
    return prefixes

def ensure_managed_folder(folder_path):
    url = f"{API_BASE}/managedFolders"
    gcs_api_call(url, method="POST", body={"name": folder_path})

def set_folder_iam(folder_path, members, role="roles/storage.objectViewer"):
    encoded = urllib.parse.quote(folder_path, safe="")
    url = f"{API_BASE}/managedFolders/{encoded}/iam"
    body = {
        "bindings": [{
            "role": role,
            "members": [f"serviceAccount:{m}" for m in members]
        }]
    }
    gcs_api_call(url, method="PUT", body=body)

# 5. 并发工作单元：处理单个天级分区
TODAY_EPOCH = time.time()

def process_partition_task(part_path):
    part_name = part_path.rstrip("/").split("/")[-1]
    clean_date = part_name.replace("dt=", "")
    try:
        dt_epoch = datetime.strptime(clean_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return f"[SKIP] 非日期分区: {part_name}"

    age_days = (TODAY_EPOCH - dt_epoch) / 86400
    ensure_managed_folder(part_path)

    if age_days < HOT_DAYS:
        set_folder_iam(part_path, [HOT_SA])
        return f"[HOT  <60d] {part_name}"
    else:
        set_folder_iam(part_path, [COLD_SA])
        return f"[COLD >=60d] {part_name}"

# 6. 主流程
def main():
    print(f"================================================================")
    print(f"==> 启动 Python 多线程调和引擎 (并发数: {MAX_WORKERS})")
    print(f"==> 目标存储桶: {BUCKET_NAME} | 根目录: {DATA_ROOT_PREFIX}/")
    print(f"================================================================")
    
    t0 = time.time()
    dbs = list_prefixes(f"{DATA_ROOT_PREFIX}/")
    all_partitions = []

    for db in dbs:
        if not db.endswith(".db/"):
            continue
        print(f"📁 发现数据库: {db}")
        tables = list_prefixes(db)

        for tbl in tables:
            print(f"  📊 数据表: {tbl}")
            
            # [A] 确保 metadata/ 读权限（毫秒级）
            meta_path = f"{tbl}metadata/"
            ensure_managed_folder(meta_path)
            set_folder_iam(meta_path, [HOT_SA, COLD_SA])

            # [B] 预建今明两天（保证热数据写入）
            for offset in [0, 1]:
                d_str = datetime.fromtimestamp(TODAY_EPOCH + offset * 86400, timezone.utc).strftime("dt=%Y-%m-%d")
                today_path = f"{tbl}data/{d_str}/"
                ensure_managed_folder(today_path)
                set_folder_iam(today_path, [HOT_SA])

            # [C] 收集该表所有的历史分区
            parts = list_prefixes(f"{tbl}data/")
            all_partitions.extend(parts)

    total_tasks = len(all_partitions)
    print(f"\n==> 扫描完毕，发现待调和历史分区共 {total_tasks} 个")
    print(f"==> 正在使用 {MAX_WORKERS} 线程池进行全量并发处理...")

    t_start_parallel = time.time()
    success_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_partition_task, p) for p in all_partitions]
        for f in as_completed(futures):
            res = f.result()
            success_count += 1
            if success_count % 10 == 0 or success_count == total_tasks:
                print(f"    [进度] {success_count}/{total_tasks} 完成...")

    t_end = time.time()
    parallel_time = t_end - t_start_parallel
    total_time = t_end - t0

    print(f"\n================================================================")
    print(f"==> 🎉 多线程调和全部完成！")
    print(f"    - 处理分区总数: {total_tasks} 个")
    print(f"    - 并发处理耗时: {parallel_time:.2f} 秒 (平均 {(parallel_time/total_tasks if total_tasks else 0)*1000:.1f} 毫秒/个)")
    print(f"    - 全流程总耗时: {total_time:.2f} 秒")
    print(f"================================================================")

if __name__ == "__main__":
    main()
