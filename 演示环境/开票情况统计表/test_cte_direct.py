# 直接测试CTE - 使用完整的项目和科目列表
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
    cursor.execute(sql, args)
    result = cursor.fetchall()
    cursor.close()
    return result

# 使用全部10个项目
comm_ids = ['0591cfbd-d915-48d6-9831-7f6201cd4d3e','0d8400d6-d728-4f72-9e6c-08bb9bf5828d','21bea8c6-46e8-4d5f-9ead-cc1ff8e3b90e','48b34bc1-4274-4be5-afac-3f252eda05d8','4eb68e2f-d1f4-486d-b7bf-d0403f8a76e7','73b796a2-74a9-4406-af1c-15ee0218e82e','ac3ece62-af2d-45b7-b644-7a60961f8abd','e1c078af-62ac-4923-9267-7f99d5dffb6a','e236f7bb-626a-4288-83bc-405d4a44545c','e5c4dfcb-e1db-443a-ab47-bf18eb48a478']

# 只用3个科目测试
corp_cost_ids = ['07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a','6cf57fd4-f314-4038-99bb-3af473d0efb5','b9e595b4-004b-4b4b-8582-5bff131fb62d']

year_start = '2025-01-01 00:00:00'
year_end = '2025-12-31 23:59:59'
end_date = '2025-12-31 23:59:59'

comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))

print(f"项目数: {len(comm_ids)}, 科目数: {len(corp_cost_ids)}")
print(f"comm_placeholders: {comm_placeholders[:50]}...")
print(f"cost_placeholders: {cost_placeholders[:50]}...")

sql = f"""
SELECT
    f.comm_id,
    f.corp_cost_id,
    SUM(CASE WHEN f.fee_date < %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_begin_due,
    SUM(CASE WHEN f.fee_date >= %s AND f.fee_date <= %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_to_month_due,
    SUM(CASE WHEN f.fee_date >= %s AND f.fee_date <= %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_due
FROM tidb_sync_combine.tb_charge_fee f
WHERE f.comm_id IN ({comm_placeholders})
  AND f.corp_cost_id IN ({cost_placeholders})
GROUP BY f.comm_id, f.corp_cost_id
"""

args = []
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(year_start)
args.append(year_start)
args.append(end_date)
args.append(year_start)
args.append(year_end)

print(f"\n参数数量: {len(args)}")
print("所有参数:")
for i, arg in enumerate(args, 1):
    print(f"  {i}. {arg}")

# 打印SQL(用参数替换%s)
test_sql = sql
for arg in args:
    test_sql = test_sql.replace('%s', f"'{arg}'", 1)
print(f"\n实际执行的SQL(前500字符):")
print(test_sql[:500])

result = db_query(sql, tuple(args))
print(f"\n查询结果: {len(result)} 行")
for i, row in enumerate(result[:10], 1):
    print(f"{i}. comm_id:{row[0][:8]}..., corp_cost_id:{row[1][:8]}..., year_begin_due:{row[2]}, year_to_month_due:{row[3]}, year_due:{row[4]}")

conn.close()
