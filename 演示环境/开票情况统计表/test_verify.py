# 验证实际存在的 comm_id 和 corp_cost_id 组合
import pymysql

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

comm_id = '0591cfbd-d915-48d6-9831-7f6201cd4d3e'
corp_cost_id1 = '07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a'
corp_cost_id2 = '09fccd77-0878-4e25-a817-0fb03c305dc1'

print("=== 检查项目ID ===")
print(f"comm_id: {comm_id}")

print("\n=== 检查tb_charge_fee表中实际存在的科目 ===")
sql1 = """
SELECT corp_cost_id, COUNT(*) as cnt
FROM tidb_sync_combine.tb_charge_fee
WHERE comm_id = %s
  AND is_delete = 0
GROUP BY corp_cost_id
ORDER BY cnt DESC
LIMIT 10
"""
result1 = db_query(sql1, (comm_id,))
print(f"找到 {len(result1)} 个不同的科目:")
for row in result1:
    print(f"  corp_cost_id: {row[0]}, 记录数: {row[1]}")

print("\n=== 检查我们要查询的科目是否存在 ===")
sql2 = """
SELECT corp_cost_id, COUNT(*) as cnt, SUM(due_amount) as total
FROM tidb_sync_combine.tb_charge_fee
WHERE comm_id = %s
  AND corp_cost_id IN (%s, %s)
  AND is_delete = 0
GROUP BY corp_cost_id
"""
result2 = db_query(sql2, (comm_id, corp_cost_id1, corp_cost_id2))
print(f"查询结果: {len(result2)} 行")
for row in result2:
    print(f"  corp_cost_id: {row[0]}, 记录数: {row[1]}, 总应收: {row[2]}")

print("\n=== 检查科目表中的科目名称 ===")
sql3 = """
SELECT id, cost_name
FROM erp_base.tb_base_charge_cost
WHERE id IN (%s, %s)
  AND is_delete = 0
"""
result3 = db_query(sql3, (corp_cost_id1, corp_cost_id2))
print(f"科目信息:")
for row in result3:
    print(f"  {row[1]} ({row[0]})")

print("\n=== 测试简单的费用查询 ===")
sql4 = """
SELECT COUNT(*) as cnt, SUM(due_amount) as total
FROM tidb_sync_combine.tb_charge_fee
WHERE comm_id = %s
  AND is_delete = 0
"""
result4 = db_query(sql4, (comm_id,))
print(f"该项目所有费用: 记录数={result4[0][0]}, 总应收={result4[0][1]}")

conn.close()
