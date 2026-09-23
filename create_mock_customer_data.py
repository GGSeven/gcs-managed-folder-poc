#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建客户真实场景的 Mock 数据
- 涵盖 2 张表及不同的冷热分界（60天 vs 90天）
- 涵盖多级分区路径：.../country_code=XX/server_dt_utc=YYYY-MM-DD/...
- 涵盖 Spark/Kyuubi 依赖文件与日志、临时路径
"""
import os, sys, subprocess

BUCKET = os.environ.get("BUCKET", "gs://bd-host-2026-004-mf-poc")

print(f"==> 开始在存储桶 {BUCKET} 创建客户场景 Mock 数据...")

MOCK_OBJECTS = [
    # 1. 核心大表 1: ods_can_origin_data_1s_p_t_i (60天冷热分界)
    ("datasets/ods.db/ods_can_origin_data_1s_p_t_i/metadata/v1.metadata.json", '{"table": "ods_can_origin_data_1s_p_t_i"}'),
    ("datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/country_code=31/server_dt_utc=2026-09-23/server_dt_local=2026-09-23/car_dt_utc=2026-09-23/car_dt_local=2026-09-23/vehicle_code=280/sample.parquet", "PARQUET_DATA_TODAY_HOT"),
    ("datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/country_code=31/server_dt_utc=2026-09-10/server_dt_local=2026-09-10/car_dt_utc=2026-09-10/car_dt_local=2026-09-10/vehicle_code=280/sample.parquet", "PARQUET_DATA_13DAYS_HOT"),
    ("datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/country_code=31/server_dt_utc=2026-07-01/server_dt_local=2026-07-01/car_dt_utc=2026-07-01/car_dt_local=2026-07-01/vehicle_code=280/sample.parquet", "PARQUET_DATA_84DAYS_COLD"),

    # 2. 核心大表 2: ods_can_parse_1s_p_t_i (90天冷热分界)
    ("datasets/ods.db/ods_can_parse_1s_p_t_i/metadata/v1.metadata.json", '{"table": "ods_can_parse_1s_p_t_i"}'),
    ("datasets/ods.db/ods_can_parse_1s_p_t_i/data/country_code=1242/server_dt_utc=2026-09-23/server_dt_local=2026-09-23/car_dt_utc=2026-09-23/car_dt_local=2026-09-23/vehicle_code=425/sample.parquet", "PARQUET_DATA_TODAY_HOT"),
    ("datasets/ods.db/ods_can_parse_1s_p_t_i/data/country_code=1242/server_dt_utc=2026-07-15/server_dt_local=2026-07-15/car_dt_utc=2026-07-15/car_dt_local=2026-07-15/vehicle_code=425/sample.parquet", "PARQUET_DATA_70DAYS_HOT_FOR_90D"),
    ("datasets/ods.db/ods_can_parse_1s_p_t_i/data/country_code=1242/server_dt_utc=2026-05-01/server_dt_local=2026-05-01/car_dt_utc=2026-05-01/car_dt_local=2026-05-01/vehicle_code=425/sample.parquet", "PARQUET_DATA_145DAYS_COLD"),

    # 3. Kyuubi & Spark 运行依赖与配置文件
    ("user/spark/kyuubi-spark-sql-engine_2.12-1.9.3.jar", "JAR_MOCK_CONTENT"),
    ("user/spark/pod-security-template3.yaml", "apiVersion: v1\nkind: Pod\nmetadata:\n  name: mock"),
    ("user/spark/executorTemplate3-exec.yaml", "apiVersion: v1\nkind: Pod\nmetadata:\n  name: mock-exec"),
    ("flink_jar/iceberg-hive-runtime-1.4.2.jar", "ICEBERG_JAR_MOCK"),

    # 4. 日志与上传临时目录初始占位
    ("spark-job-history/.keep", "HISTORY_INIT"),
    ("spark-tmp/.keep", "TMP_INIT"),
]

for path, content in MOCK_OBJECTS:
    target_uri = f"{BUCKET}/{path}"
    cmd = ["gcloud", "storage", "cp", "-", target_uri]
    res = subprocess.run(cmd, input=content, text=True, capture_output=True, shell=(sys.platform == "win32"))
    if res.returncode == 0:
        print(f"[OK] Uploaded: {path}")
    else:
        print(f"[ERROR] Failed: {path} -> {res.stderr}")

print("\nMock data created successfully!")
