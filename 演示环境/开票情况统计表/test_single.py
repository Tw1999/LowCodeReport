# 测试单个项目单个科目
import pymysql
import datetime

conn = pymysql.connect(
    host='8.137.109.26',
    port=9030,
    user='root',
    password='tw369.com',
    database='erp_statistics',
    charset='utf8mb4'
)

def db_query(sql, args):
    cursor = conn.cursor()
    print(f"\n执行SQL,参数: {args}")
    cursor.execute(sql, args)
    result = cursor.fetchall()
    cursor.close()
    return result

comm_id = '0591cfbd-d915-48d6-9831-7f6201cd4d3e'
corp_cost_id = '07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a'  # 水费,有7116条记录

year_start = '2025-01-01 00:00:00'
year_end = '2025-12-31 23:59:59'

print("=== 测试1: 直接查询,不使用IN ===")
sql1 = """
SELECT
    f.comm_id,
    f.corp_cost_id,
    COUNT(*) as cnt,
    SUM(CASE WHEN f.fee_date < %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_begin_due
FROM tidb_sync_combine.tb_charge_fee f
WHERE f.comm_id = %s
  AND f.corp_cost_id = %s
GROUP BY f.comm_id, f.corp_cost_id
"""
result1 = db_query(sql1, (year_start, comm_id, corp_cost_id))
print(f"结果: {len(result1)} 行")
if result1:
    print(f"  {result1[0]}")

print("\n=== 测试2: 使用IN,单个值 ===")
sql2 = """
SELECT
    f.comm_id,
    f.corp_cost_id,
    COUNT(*) as cnt,
    SUM(CASE WHEN f.fee_date < %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_begin_due
FROM tidb_sync_combine.tb_charge_fee f
WHERE f.comm_id IN (%s)
  AND f.corp_cost_id IN (%s)
GROUP BY f.comm_id, f.corp_cost_id
"""
result2 = db_query(sql2, (year_start, comm_id, corp_cost_id))
print(f"结果: {len(result2)} 行")
if result2:
    print(f"  {result2[0]}")

print("\n=== 测试3: 使用IN,两个科目 ===")
corp_cost_id2 = '09fccd77-0878-4e25-a817-0fb03c305dc1'
sql3 = """
SELECT
    f.comm_id,
    f.corp_cost_id,
    COUNT(*) as cnt,
    SUM(CASE WHEN f.fee_date < %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_begin_due
FROM tidb_sync_combine.tb_charge_fee f
WHERE f.comm_id IN (%s)
  AND f.corp_cost_id IN (%s, %s)
GROUP BY f.comm_id, f.corp_cost_id
"""
result3 = db_query(sql3, (year_start, comm_id, corp_cost_id, corp_cost_id2))
print(f"结果: {len(result3)} 行")
for row in result3:
    print(f"  {row}")

conn.close()
