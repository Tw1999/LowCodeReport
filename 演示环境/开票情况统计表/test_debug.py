# 调试脚本 - 逐步检查每个CTE
import pymysql
import datetime

# 模拟 params - 使用1个项目和2个科目测试
params = {
    "comm_ids": ['0591cfbd-d915-48d6-9831-7f6201cd4d3e'],
    "corp_cost_ids": ['07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a','09fccd77-0878-4e25-a817-0fb03c305dc1'],
    "start_date": "2001-1-1",
    "end_date": "2025-12-31",
    "contract_type": None
}

# 数据库连接
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

# 解析入参
comm_ids = params.get("comm_ids", [])
corp_cost_ids = params.get("corp_cost_ids", [])
contract_type = params.get("contract_type")
start_date = params.get("start_date")
end_date = params.get("end_date")

# 计算关键时间点
if ' ' in start_date:
    a_time = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
else:
    a_time = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    start_date = a_time.strftime('%Y-%m-%d') + ' 00:00:00'

if ' ' in end_date:
    b_time = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
else:
    b_time = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    end_date = b_time.strftime('%Y-%m-%d') + ' 23:59:59'

year_start = f"{b_time.year}-01-01 00:00:00"
year_end = f"{b_time.year}-12-31 23:59:59"
month_start = f"{b_time.year}-{b_time.month:02d}-01 00:00:00"

comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))

print("=== 测试1: fee_agg CTE ===")
sql1 = f'''
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
'''
args1 = []
args1.extend(comm_ids)
args1.extend(corp_cost_ids)
args1.append(year_start)
args1.append(year_start)
args1.append(end_date)
args1.append(year_start)
args1.append(year_end)
result1 = db_query(sql1, tuple(args1))
print(f"fee_agg 返回 {len(result1)} 行")
for row in result1:
    print(f"  comm_id: {row[0][:8]}..., corp_cost_id: {row[1][:8]}..., year_begin_due: {row[2]}, year_to_month_due: {row[3]}, year_due: {row[4]}")

print("\n=== 测试2: receipt_agg CTE ===")
sql2 = f'''
SELECT
    d.comm_id,
    d.corp_cost_id,
    SUM(CASE WHEN d.deal_date < %s AND d.fee_date < %s AND d.deal_type IN ('减免', '减免红冲', '预存冲抵', '预存冲抵红冲', '代扣', '托收确认', '实收', '实收红冲') AND d.is_delete = 0 THEN d.deal_amount ELSE 0 END) AS year_begin_paid,
    SUM(CASE WHEN d.deal_date > %s AND d.deal_date <= %s AND d.deal_type IN ('托收', '代扣', '实收', '实收红冲', '预存', '预存红冲') AND d.is_delete = 0 THEN d.deal_amount ELSE 0 END) AS month_invoice
FROM tidb_sync_combine.tb_charge_receipts_detail d
WHERE d.comm_id IN ({comm_placeholders})
  AND d.corp_cost_id IN ({cost_placeholders})
GROUP BY d.comm_id, d.corp_cost_id
'''
args2 = []
args2.extend(comm_ids)
args2.extend(corp_cost_ids)
args2.append(year_start)
args2.append(year_start)
args2.append(month_start)
args2.append(end_date)
result2 = db_query(sql2, tuple(args2))
print(f"receipt_agg 返回 {len(result2)} 行")
for row in result2:
    print(f"  comm_id: {row[0][:8]}..., corp_cost_id: {row[1][:8]}..., year_begin_paid: {row[2]}, month_invoice: {row[3]}")

print("\n=== 测试3: 主查询 CROSS JOIN ===")
sql3 = f'''
SELECT o.Id, o.Name, c.id, c.cost_name
FROM erp_base.rf_organize o
CROSS JOIN erp_base.tb_base_charge_cost c
WHERE o.Id IN ({comm_placeholders})
  AND c.id IN ({cost_placeholders})
  AND o.OrganType = 6
  AND o.Is_Delete = 0
  AND o.Status = 0
  AND c.is_delete = 0
ORDER BY o.Name, c.sort, c.cost_name
'''
args3 = []
args3.extend(comm_ids)
args3.extend(corp_cost_ids)
result3 = db_query(sql3, tuple(args3))
print(f"主查询基础表 返回 {len(result3)} 行")
for row in result3:
    print(f"  项目: {row[1]}, 科目: {row[3]}")

print("\n=== 测试4: 完整查询(简化) ===")
sql4 = f'''
WITH
fee_agg AS (
    SELECT
        f.comm_id,
        f.corp_cost_id,
        SUM(CASE WHEN f.fee_date < %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_begin_due
    FROM tidb_sync_combine.tb_charge_fee f
    WHERE f.comm_id IN ({comm_placeholders})
      AND f.corp_cost_id IN ({cost_placeholders})
    GROUP BY f.comm_id, f.corp_cost_id
)
SELECT
    o.Name AS project_name,
    c.cost_name AS cost_name,
    COALESCE(f.year_begin_due, 0) AS year_begin_due
FROM erp_base.rf_organize o
CROSS JOIN erp_base.tb_base_charge_cost c
LEFT JOIN fee_agg f ON o.Id = f.comm_id AND c.id = f.corp_cost_id
WHERE o.Id IN ({comm_placeholders})
  AND c.id IN ({cost_placeholders})
  AND o.OrganType = 6
  AND o.Is_Delete = 0
  AND o.Status = 0
  AND c.is_delete = 0
ORDER BY o.Name, c.sort, c.cost_name
'''
args4 = []
# fee_agg WHERE
args4.extend(comm_ids)
args4.extend(corp_cost_ids)
# fee_agg CASE
args4.append(year_start)
# 主查询 WHERE
args4.extend(comm_ids)
args4.extend(corp_cost_ids)
result4 = db_query(sql4, tuple(args4))
print(f"完整查询 返回 {len(result4)} 行")
for row in result4[:5]:
    print(f"  {row[0]} - {row[1]}: {row[2]}")

conn.close()
