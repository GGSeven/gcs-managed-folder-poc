#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

PROJECT_ID = os.environ.get("PROJECT_ID", "bd-host-2026-004")
BUCKET = os.environ.get("BUCKET", "gs://bd-host-2026-004-mf-poc")
BUCKET_NAME = BUCKET.replace("gs://", "").strip("/")
DATA_ROOT_PREFIX = os.environ.get("DATA_ROOT_PREFIX", "datasets").strip("/")
DEFAULT_HOT_DAYS = int(os.environ.get("HOT_DAYS", "60"))

# 表级差异化保留期映射（可由环境变量覆盖: TABLE_RETENTION="tbl1:60,tbl2:90"）
DEFAULT_TABLE_RULES = {
    "ods_can_origin_data_1s_p_t_i": 60,
    "ods_can_parse_1s_p_t_i": 90,
}
env_rules = os.environ.get("TABLE_RETENTION", "")
TABLE_RULES = dict(DEFAULT_TABLE_RULES)
if env_rules:
    for item in env_rules.split(","):
        if ":" in item:
            k, v = item.strip().split(":")
            TABLE_RULES[k.strip()] = int(v.strip())

HOT_SA = os.environ.get("HOT_SA", f"iceberg-hot-reader@{PROJECT_ID}.iam.gserviceaccount.com")
COLD_SA = os.environ.get("COLD_SA", f"iceberg-cold-reader@{PROJECT_ID}.iam.gserviceaccount.com")

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "30"))
EXCLUDE_DBS = set(filter(None, os.environ.get("EXCLUDE_DBS", "tmp.db,kafka_test.db").split(",")))
INCLUDE_DBS = set(filter(None, os.environ.get("INCLUDE_DBS", "").split(",")))

# Spark/Kyuubi 辅助目录列表配置
AUX_DIRECTORIES = {
    "user/spark/": ("roles/storage.objectViewer", [HOT_SA]),
    "flink_jar/": ("roles/storage.objectViewer", [HOT_SA]),
    "spark-job-history/": ("roles/storage.objectUser", [HOT_SA]),
    "spark-tmp/": ("roles/storage.objectUser", [HOT_SA]),
}

API_BASE = f"https://storage.googleapis.com/storage/v1/b/{BUCKET_NAME}"

DATE_REGEX = re.compile(r"(?:server_dt_utc|dt)=(\d{4}-\d{2}-\d{2})")

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
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 2, max_retries=2)
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
    body = {"bindings": [{"role": role, "members": [f"serviceAccount:{sa}" for sa in sa_list]}]}
    resp = session.put(url, json=body, timeout=10)
    return resp.status_code == 200

def find_date_partitions(session, base_prefix, depth=1, max_depth=3):
    """递归发现日期分区，遇到包含 dt= 或 server_dt_utc= 的目录即作为 Managed Folder 边界停止深入"""
    results = []
    sub_prefixes = list_sub_prefixes(session, base_prefix)
    for p in sub_prefixes:
        match = DATE_REGEX.search(p)
        if match:
            results.append(p)
        elif depth < max_depth:
            results.extend(find_date_partitions(session, p, depth + 1, max_depth))
    return results

TODAY_EPOCH = time.time()

def process_partition(session, part_path, hot_days_threshold):
    match = DATE_REGEX.search(part_path)
    if not match:
        return (part_path, "SKIP_NO_DATE")
    d_str = match.group(1)
    try:
        dt_epoch = datetime.strptime(d_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return (part_path, "SKIP_BAD_DATE")

    age_days = (TODAY_EPOCH - dt_epoch) / 86400
    ensure_managed_folder(session, part_path)
    if age_days < hot_days_threshold:
        ok = set_managed_folder_iam(session, part_path, [HOT_SA], "roles/storage.objectViewer")
        return (part_path, "HOT", ok)
    else:
        ok = set_managed_folder_iam(session, part_path, [COLD_SA], "roles/storage.objectViewer")
        return (part_path, "COLD", ok)

def main():
    print("=========================================================================", flush=True)
    print(f"🚀 启动客户定制化 GCS Managed Folder 调和引擎", flush=True)
    print(f"   项目: {PROJECT_ID} | 存储桶: {BUCKET_NAME}", flush=True)
    print(f"   表保留天数规则: {TABLE_RULES} (默认兜底: {DEFAULT_HOT_DAYS} 天)", flush=True)
    print("=========================================================================", flush=True)

    token = get_access_token()
    session = build_http_session(token)

    # 1. 配置 Spark/Kyuubi 辅助目录（公共依赖、日志、临时上传路径）
    print("\n--> [阶段 1/3] 检查并配置 Spark/Kyuubi 辅助依赖与日志临时目录...", flush=True)
    for aux_path, (role, sa_list) in AUX_DIRECTORIES.items():
        ensure_managed_folder(session, aux_path)
        ok = set_managed_folder_iam(session, aux_path, sa_list, role)
        print(f"    ✔ 辅助目录 [{aux_path}] -> 角色: {role} (状态: {'OK' if ok else 'FAIL'})", flush=True)

    # 2. 扫描业务库与表结构
    print(f"\n--> [阶段 2/3] 扫描业务库表结构 (根目录: {DATA_ROOT_PREFIX}/)...", flush=True)
    dbs = list_sub_prefixes(session, f"{DATA_ROOT_PREFIX}/")
    selected_tables = []
    for db in dbs:
        db_name = db.rstrip("/").split("/")[-1]
        if INCLUDE_DBS and db_name not in INCLUDE_DBS:
            continue
        if db_name in EXCLUDE_DBS:
            continue
        tables = list_sub_prefixes(session, db)
        selected_tables.extend(tables)

    print(f"✔ 发现 {len(selected_tables)} 张业务表", flush=True)

    # 3. 处理表元数据及分区调和
    print(f"\n--> [阶段 3/3] 开始调和表级元数据与冷热数据分区...", flush=True)
    total_parts = 0
    hot_count = 0
    cold_count = 0

    for tbl in selected_tables:
        tbl_name = tbl.rstrip("/").split("/")[-1]
        hot_threshold = TABLE_RULES.get(tbl_name, DEFAULT_HOT_DAYS)
        print(f"\n--- 处理数据表: {tbl_name} (冷热分界阈值: {hot_threshold} 天) ---", flush=True)

        # 赋权元数据 metadata/
        meta_path = f"{tbl}metadata/"
        ensure_managed_folder(session, meta_path)
        set_managed_folder_iam(session, meta_path, [HOT_SA, COLD_SA], "roles/storage.objectViewer")
        print(f"    ✔ 元数据目录已赋权: {meta_path}", flush=True)

        # 扫描日期分区（支持单层及多层分级分区）
        data_prefix = f"{tbl}data/"
        date_parts = find_date_partitions(session, data_prefix)
        print(f"    扫描到 {len(date_parts)} 个日期分区...", flush=True)

        for p in date_parts:
            part_path, status, ok = process_partition(session, p, hot_threshold)
            total_parts += 1
            if status == "HOT":
                hot_count += 1
                print(f"    [HOT]  {part_path} -> 赋权 hot-reader (状态: {'OK' if ok else 'FAIL'})", flush=True)
            elif status == "COLD":
                cold_count += 1
                print(f"    [COLD] {part_path} -> 赋权 cold-reader (状态: {'OK' if ok else 'FAIL'})", flush=True)

    print("\n=========================================================================", flush=True)
    print(f"🎉 调和完成！总分区数: {total_parts} | 热分区(放行): {hot_count} | 冷分区(拦截): {cold_count}", flush=True)
    print("=========================================================================", flush=True)

if __name__ == "__main__":
    main()
