#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
10,000 条分区 Mock 数据生成 + 调和压测验证 + 耗时统计
架构：GCP 内网环境极速压测
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
    print("错误: 缺少 requests 库")
    sys.exit(1)

# ==============================
# 配置参数
# ==============================
PROJECT_ID = os.environ.get("PROJECT_ID", "bd-host-2026-004")
BUCKET_NAME = os.environ.get("BUCKET", f"gs://{PROJECT_ID}-mf-poc").replace("gs://", "").strip("/")
DATA_ROOT = os.environ.get("DATA_ROOT_PREFIX", "datasets").strip("/")
DB_NAME = "perf_10k.db"
HOT_DAYS = int(os.environ.get("HOT_DAYS", "60"))

HOT_SA = os.environ.get("HOT_SA", f"iceberg-hot-reader@{PROJECT_ID}.iam.gserviceaccount.com")
COLD_SA = os.environ.get("COLD_SA", f"iceberg-cold-reader@{PROJECT_ID}.iam.gserviceaccount.com")

NUM_TABLES = 50        # 50 张表
DAYS_PER_TABLE = 200   # 200 天分区 -> 50 * 200 = 10,000 个分区
GEN_WORKERS = 60       # 生成并发
RECONCILE_WORKERS = 30 # 调和并发

API_BASE = f"https://storage.googleapis.com/storage/v1/b/{BUCKET_NAME}"
UPLOAD_URL = f"https://storage.googleapis.com/upload/storage/v1/b/{BUCKET_NAME}/o?uploadType=media"

def get_token():
    env_token = os.environ.get("GCS_TOKEN") or os.environ.get("ACCESS_TOKEN")
    if env_token:
        return env_token.strip()
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

def build_session(token, max_workers):
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=max_workers * 2, pool_maxsize=max_workers * 2, max_retries=3)
    s.mount("https://", adapter)
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    return s

def upload_placeholder(session, object_name):
    url = f"{UPLOAD_URL}&name={object_name}"
    resp = session.post(url, data=b"", timeout=15)
    return resp.status_code in (200, 201)

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

def process_partition(session, part_path):
    part_name = part_path.rstrip("/").split("/")[-1]
    clean_date = part_name.replace("dt=", "")
    try:
        dt_epoch = datetime.strptime(clean_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return (part_name, "SKIP")

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
    print("📊 GCS Managed Folder 10,000 分区端到端性能压测与调和验证")
    print(f"   项目: {PROJECT_ID} | 存储桶: {BUCKET_NAME}")
    print(f"   测试数据库: {DATA_ROOT}/{DB_NAME}/")
    print(f"   规模: {NUM_TABLES} 张表 × {DAYS_PER_TABLE} 天历史分区 = {NUM_TABLES * DAYS_PER_TABLE} 个独立分区")
    print(f"   隔离规则: < {HOT_DAYS} 天为热数据(只读)，>= {HOT_DAYS} 天为冷数据(403拦截)")
    print("=========================================================================")

    token = get_token()
    session = build_session(token, GEN_WORKERS)

    # -------------------------------------------------------------
    # 阶段 1：生成 10,000 个分区 Mock 数据
    # -------------------------------------------------------------
    print("\n===> [阶段 1/3] 正在生成 10,000 个 Mock 分区数据...")
    gen_start = time.time()
    now_utc = datetime.now(timezone.utc)
    object_list = []
    partition_list = []

    for t_idx in range(1, NUM_TABLES + 1):
        tbl_name = f"table_{t_idx:02d}"
        tbl_prefix = f"{DATA_ROOT}/{DB_NAME}/{tbl_name}"
        
        # 表元数据
        object_list.append(f"{tbl_prefix}/metadata/v1.metadata.json")
        
        # 200 天历史分区
        for d in range(DAYS_PER_TABLE):
            d_str = (now_utc - timedelta(days=d)).strftime("dt=%Y-%m-%d")
            part_folder = f"{tbl_prefix}/data/{d_str}/"
            partition_list.append(part_folder)
            object_list.append(f"{part_folder}data-00000.parquet")

    total_objects = len(object_list)
    print(f"     生成目标: {total_objects} 个 GCS 对象（含 {len(partition_list)} 个数据分区）")

    gen_success = 0
    with ThreadPoolExecutor(max_workers=GEN_WORKERS) as executor:
        futures = {executor.submit(upload_placeholder, session, obj): obj for obj in object_list}
        for f in as_completed(futures):
            try:
                if f.result():
                    gen_success += 1
                if gen_success % 2000 == 0 or gen_success == total_objects:
                    print(f"     [生成进度] {gen_success}/{total_objects} ({gen_success/total_objects*100:.1f}%)")
            except Exception as e:
                pass

    gen_elapsed = time.time() - gen_start
    print(f"  ✔ 阶段 1 完成！成功写入 {gen_success} 个对象，耗时: {gen_elapsed:.2f} 秒 (吞吐: {gen_success/max(gen_elapsed,0.1):.1f} 对象/秒)")

    # -------------------------------------------------------------
    # 阶段 2：执行 10,000 分区调和引擎 (reconcile)
    # -------------------------------------------------------------
    print(f"\n===> [阶段 2/3] 启动调和引擎，对 10,000 个分区下发 Managed Folder 与 IAM 策略...")
    reconcile_start = time.time()
    r_session = build_session(token, RECONCILE_WORKERS)

    # 1. 配置各表 metadata 读权限
    print(f"     正在配置 {NUM_TABLES} 张表的 metadata/ 授权...")
    for t_idx in range(1, NUM_TABLES + 1):
        tbl_name = f"table_{t_idx:02d}"
        meta_folder = f"{DATA_ROOT}/{DB_NAME}/{tbl_name}/metadata/"
        ensure_managed_folder(r_session, meta_folder)
        set_managed_folder_iam(r_session, meta_folder, [HOT_SA, COLD_SA])

    # 2. 并发调和 10,000 个分区
    print(f"     启动 {RECONCILE_WORKERS} 线程并发调和 10,000 个分区...")
    hot_count = 0
    cold_count = 0
    reconcile_success = 0

    with ThreadPoolExecutor(max_workers=RECONCILE_WORKERS) as executor:
        futures = {executor.submit(process_partition, r_session, p): p for p in partition_list}
        for f in as_completed(futures):
            try:
                p_name, status = f.result()
                reconcile_success += 1
                if status == "HOT":
                    hot_count += 1
                elif status == "COLD":
                    cold_count += 1
                if reconcile_success % 2000 == 0 or reconcile_success == len(partition_list):
                    print(f"     [调和进度] {reconcile_success}/{len(partition_list)} ({reconcile_success/len(partition_list)*100:.1f}%) - 热: {hot_count}, 冷: {cold_count}")
            except Exception as e:
                pass

    reconcile_elapsed = time.time() - reconcile_start
    print(f"  ✔ 阶段 2 完成！成功调和 {reconcile_success} 个分区，耗时: {reconcile_elapsed:.2f} 秒 (吞吐: {reconcile_success/max(reconcile_elapsed,0.1):.1f} 分区/秒)")

    # -------------------------------------------------------------
    # 阶段 3：抽样断言验证 (Assertion Checks)
    # -------------------------------------------------------------
    print("\n===> [阶段 3/3] 抽样验证 IAM 隔离有效性...")
    def check_iam(folder_path):
        encoded = urllib.parse.quote(folder_path, safe="")
        resp = r_session.get(f"{API_BASE}/managedFolders/{encoded}/iam", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            members = []
            for b in data.get("bindings", []):
                members.extend(b.get("members", []))
            return members
        return []

    # 抽查表 1 的热分区 (近 10 天) 与冷分区 (150 天前)
    sample_hot = partition_list[5]     # 5 天前 (热)
    sample_cold = partition_list[150]  # 150 天前 (冷)
    sample_meta = f"{DATA_ROOT}/{DB_NAME}/table_01/metadata/"

    hot_members = check_iam(sample_hot)
    cold_members = check_iam(sample_cold)
    meta_members = check_iam(sample_meta)

    hot_ok = any(HOT_SA in m for m in hot_members) and not any(COLD_SA in m for m in hot_members)
    cold_ok = any(COLD_SA in m for m in cold_members) and not any(HOT_SA in m for m in cold_members)
    meta_ok = any(HOT_SA in m for m in meta_members) and any(COLD_SA in m for m in meta_members)

    print(f"     [验证 1] 热分区 ({sample_hot.split('/')[-2]}): 绑定 {hot_members} => {'PASS ✔' if hot_ok else 'FAIL ✘'}")
    print(f"     [验证 2] 冷分区 ({sample_cold.split('/')[-2]}): 绑定 {cold_members} => {'PASS ✔' if cold_ok else 'FAIL ✘'}")
    print(f"     [验证 3] 元数据 (metadata/): 绑定 {meta_members} => {'PASS ✔' if meta_ok else 'FAIL ✘'}")

    # -------------------------------------------------------------
    # 总结与性能报告
    # -------------------------------------------------------------
    print("\n=========================================================================")
    print("🏆 10,000 分区性能与功能全流程测试报告")
    print(f"   1. Mock 数据规模: {NUM_TABLES} 表 × {DAYS_PER_TABLE} 天 = {len(partition_list)} 分区 ({total_objects} 对象)")
    print(f"   2. Mock 生成耗时: {gen_elapsed:.2f} 秒 (吞吐: {gen_success/max(gen_elapsed,0.1):.1f} 对象/秒)")
    print(f"   3. 调和引擎耗时: {reconcile_elapsed:.2f} 秒 (吞吐: {reconcile_success/max(reconcile_elapsed,0.1):.1f} 分区/秒)")
    print(f"   4. 热分区数量: {hot_count} (近 60 天，绑定 {HOT_SA})")
    print(f"   5. 冷分区数量: {cold_count} (60 天前，绑定 {COLD_SA})")
    print(f"   6. 权限断言结果: {'全部通过 PASS ✔' if (hot_ok and cold_ok and meta_ok) else '存在异常 FAIL ✘'}")
    print(f"   7. 端到端总用时: {gen_elapsed + reconcile_elapsed:.2f} 秒")
    print("=========================================================================")

if __name__ == "__main__":
    main()
