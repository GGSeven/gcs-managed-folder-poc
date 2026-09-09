#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速生成 10,000 个分区 Mock 数据脚本
结构：50 张表 × 200 天分区 (dt=YYYY-MM-DD) = 10,000 个独立分区
分布：0~59天为热数据 (3,000个)，60~199天为冷数据 (7,000个)
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("错误: 缺少 requests 库")
    sys.exit(1)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

PROJECT_ID = os.environ.get("PROJECT_ID", "bd-host-2026-004")
BUCKET_NAME = os.environ.get("BUCKET", f"gs://{PROJECT_ID}-mf-poc").replace("gs://", "").strip("/")
DATA_ROOT = os.environ.get("DATA_ROOT_PREFIX", "datasets").strip("/")
DB_NAME = "mock_perf.db"

NUM_TABLES = 50       # 50 张表
DAYS_PER_TABLE = 200  # 200 天分区 -> 共 10,000 分区
MAX_WORKERS = 80      # 80 线程高并发生成

UPLOAD_URL = f"https://storage.googleapis.com/upload/storage/v1/b/{BUCKET_NAME}/o?uploadType=media"

def get_token():
    env_token = os.environ.get("GCS_TOKEN") or os.environ.get("ACCESS_TOKEN")
    if env_token:
        return env_token.strip()
    import subprocess
    cmd = ["gcloud", "auth", "print-access-token"]
    return subprocess.check_output(cmd, text=True, shell=(sys.platform == "win32")).strip()

def build_session(token):
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 2, max_retries=3)
    s.mount("https://", adapter)
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    })
    return s

def upload_placeholder(session, object_name):
    url = f"{UPLOAD_URL}&name={object_name}"
    resp = session.post(url, data=b"", timeout=15)
    return resp.status_code in (200, 201)

def main():
    print(f"=========================================================================")
    print(f"🚀 开始生成 10,000 个分区 Mock 数据")
    print(f"   目标 Bucket: gs://{BUCKET_NAME}")
    print(f"   路径结构: {DATA_ROOT}/{DB_NAME}/<table_xx>/data/dt=YYYY-MM-DD/data-00000.parquet")
    print(f"   规模: {NUM_TABLES} 张表 × {DAYS_PER_TABLE} 天历史分区 = {NUM_TABLES * DAYS_PER_TABLE} 个独立分区")
    print(f"   并发上传线程数: {MAX_WORKERS}")
    print(f"=========================================================================")

    token = get_token()
    session = build_session(token)

    start_time = time.time()
    now_utc = datetime.now(timezone.utc)

    # 1. 生成所有待创建的对象名称列表
    object_list = []

    for t_idx in range(1, NUM_TABLES + 1):
        tbl_name = f"tbl_{t_idx:02d}"
        
        # metadata 占位文件
        object_list.append(f"{DATA_ROOT}/{DB_NAME}/{tbl_name}/metadata/v1.metadata.json")

        # 200 天分区数据占位文件
        for day_offset in range(DAYS_PER_TABLE):
            d_str = (now_utc - timedelta(days=day_offset)).strftime("dt=%Y-%m-%d")
            object_name = f"{DATA_ROOT}/{DB_NAME}/{tbl_name}/data/{d_str}/data-00000.parquet"
            object_list.append(object_name)

    total_objects = len(object_list)
    total_partitions = NUM_TABLES * DAYS_PER_TABLE
    print(f"--> 准备就绪，共计生成 {total_objects} 个对象（含 {total_partitions} 个数据分区）...")

    # 2. 多线程并发上传
    success_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(upload_placeholder, session, obj): obj for obj in object_list}
        for f in as_completed(futures):
            try:
                if f.result():
                    success_count += 1
                if success_count % 1000 == 0 or success_count == total_objects:
                    print(f"    生成进度: [{success_count}/{total_objects}] 已完成... ({(success_count/total_objects)*100:.1f}%)")
            except Exception as e:
                print(f"    [WARN] 上传失败: {e}")

    elapsed = time.time() - start_time
    print(f"=========================================================================")
    print(f"🎉 10,000 条 Mock 数据生成完毕！")
    print(f"   成功生成对象: {success_count} / {total_objects}")
    print(f"   总耗时: {elapsed:.2f} 秒")
    print(f"   平均写入吞吐: {success_count / max(elapsed, 0.1):.1f} 对象/秒")
    print(f"=========================================================================")

if __name__ == "__main__":
    main()
