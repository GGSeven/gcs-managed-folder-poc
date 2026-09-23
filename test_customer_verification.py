#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动化全链路验证客户定制场景：
1. 验证 表1 (60天阈值): 热数据(0天/13天)放行，冷数据(84天)强拦截403
2. 验证 表2 (90天阈值): 热数据(0天/70天)放行，冷数据(145天)强拦截403
3. 验证 Spark/Kyuubi 辅助目录: user/spark、flink_jar 只读放行，spark-job-history、spark-tmp 写入放行
4. 验证 cold-reader: 冷数据正常读取
"""
import os, sys, subprocess

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

PROJECT_ID = os.environ.get("PROJECT_ID", "bd-host-2026-004")
BUCKET = os.environ.get("BUCKET", "gs://bd-host-2026-004-mf-poc")
HOT_SA = f"iceberg-hot-reader@{PROJECT_ID}.iam.gserviceaccount.com"
COLD_SA = f"iceberg-cold-reader@{PROJECT_ID}.iam.gserviceaccount.com"

print("=========================================================================")
print("🔍 开始自动化验证客户定制场景隔离策略")
print(f"   项目: {PROJECT_ID} | 存储桶: {BUCKET}")
print("=========================================================================\n")

def test_read(path, sa, expect_allow):
    uri = f"{BUCKET}/{path}"
    cmd = ["gcloud", "storage", "cat", uri, f"--impersonate-service-account={sa}"]
    res = subprocess.run(cmd, capture_output=True, text=True, shell=(sys.platform == "win32"))
    actual_allow = (res.returncode == 0)
    passed = (actual_allow == expect_allow)
    result_str = "ALLOW" if actual_allow else "DENY (403)"
    expect_str = "ALLOW" if expect_allow else "DENY (403)"
    status = "✔ [PASS]" if passed else "✘ [FAIL]"
    print(f"  {status} 读: {path}")
    print(f"         身份: {sa.split('@')[0]} | 预期: {expect_str} | 实际: {result_str}", flush=True)
    return passed

def test_write(path, sa, expect_allow):
    uri = f"{BUCKET}/{path}"
    import tempfile
    with tempfile.NamedTemporaryFile("w", delete=False) as tf:
        tf.write("TEST_DATA_VERIFY")
        tf_path = tf.name
    try:
        cmd = ["gcloud", "storage", "cp", tf_path, uri, f"--impersonate-service-account={sa}"]
        res = subprocess.run(cmd, capture_output=True, text=True, shell=(sys.platform == "win32"))
    finally:
        try:
            os.remove(tf_path)
        except Exception:
            pass
    actual_allow = (res.returncode == 0)
    passed = (actual_allow == expect_allow)
    result_str = "ALLOW" if actual_allow else "DENY (403)"
    expect_str = "ALLOW" if expect_allow else "DENY (403)"
    status = "✔ [PASS]" if passed else "✘ [FAIL]"
    print(f"  {status} 写: {path}")
    print(f"         身份: {sa.split('@')[0]} | 预期: {expect_str} | 实际: {result_str}", flush=True)
    return passed

all_passed = True

print("【测试项 1】验证表 1: ods_can_origin_data_1s_p_t_i (60 天分界)")
p1 = test_read("datasets/ods.db/ods_can_origin_data_1s_p_t_i/metadata/v1.metadata.json", HOT_SA, True)
p2 = test_read("datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/country_code=31/server_dt_utc=2026-09-23/server_dt_local=2026-09-23/car_dt_utc=2026-09-23/car_dt_local=2026-09-23/vehicle_code=280/sample.parquet", HOT_SA, True)
p3 = test_read("datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/country_code=31/server_dt_utc=2026-09-10/server_dt_local=2026-09-10/car_dt_utc=2026-09-10/car_dt_local=2026-09-10/vehicle_code=280/sample.parquet", HOT_SA, True)
p4 = test_read("datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/country_code=31/server_dt_utc=2026-07-01/server_dt_local=2026-07-01/car_dt_utc=2026-07-01/car_dt_local=2026-07-01/vehicle_code=280/sample.parquet", HOT_SA, False)
all_passed = all_passed and p1 and p2 and p3 and p4

print("\n【测试项 2】验证表 2: ods_can_parse_1s_p_t_i (90 天分界)")
p5 = test_read("datasets/ods.db/ods_can_parse_1s_p_t_i/metadata/v1.metadata.json", HOT_SA, True)
p6 = test_read("datasets/ods.db/ods_can_parse_1s_p_t_i/data/country_code=1242/server_dt_utc=2026-09-23/server_dt_local=2026-09-23/car_dt_utc=2026-09-23/car_dt_local=2026-09-23/vehicle_code=425/sample.parquet", HOT_SA, True)
p7 = test_read("datasets/ods.db/ods_can_parse_1s_p_t_i/data/country_code=1242/server_dt_utc=2026-07-15/server_dt_local=2026-07-15/car_dt_utc=2026-07-15/car_dt_local=2026-07-15/vehicle_code=425/sample.parquet", HOT_SA, True)
p8 = test_read("datasets/ods.db/ods_can_parse_1s_p_t_i/data/country_code=1242/server_dt_utc=2026-05-01/server_dt_local=2026-05-01/car_dt_utc=2026-05-01/car_dt_local=2026-05-01/vehicle_code=425/sample.parquet", HOT_SA, False)
all_passed = all_passed and p5 and p6 and p7 and p8

print("\n【测试项 3】验证 Spark/Kyuubi 辅助目录放行 (解决启动 403 阻碍)")
p9 = test_read("user/spark/kyuubi-spark-sql-engine_2.12-1.9.3.jar", HOT_SA, True)
p10 = test_read("flink_jar/iceberg-hive-runtime-1.4.2.jar", HOT_SA, True)
p11 = test_write("spark-job-history/test_app.log", HOT_SA, True)
p12 = test_write("spark-tmp/test_upload.tmp", HOT_SA, True)
all_passed = all_passed and p9 and p10 and p11 and p12

print("\n【测试项 4】验证 cold-reader 历史审计查询放行")
p13 = test_read("datasets/ods.db/ods_can_origin_data_1s_p_t_i/data/country_code=31/server_dt_utc=2026-07-01/server_dt_local=2026-07-01/car_dt_utc=2026-07-01/car_dt_local=2026-07-01/vehicle_code=280/sample.parquet", COLD_SA, True)
p14 = test_read("datasets/ods.db/ods_can_parse_1s_p_t_i/data/country_code=1242/server_dt_utc=2026-05-01/server_dt_local=2026-05-01/car_dt_utc=2026-05-01/car_dt_local=2026-05-01/vehicle_code=425/sample.parquet", COLD_SA, True)
all_passed = all_passed and p13 and p14

print("\n=========================================================================")
if all_passed:
    print("🎉 恭喜！所有 14 项安全与业务隔离用例全部 [PASS] 通过！")
else:
    print("✘ 注意：存在未通过的用例，请检查上述详情。")
print("=========================================================================")
