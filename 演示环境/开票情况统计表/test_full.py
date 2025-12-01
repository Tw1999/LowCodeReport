# 完整测试脚本 - 测试修复后的开票情况统计表
import pymysql
import datetime

# 模拟 params - 使用少量数据测试
params = {
    "comm_ids": ['0591cfbd-d915-48d6-9831-7f6201cd4d3e'],  # 只测试1个项目
    "corp_cost_ids": ['07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a','09fccd77-0878-4e25-a817-0fb03c305dc1'],  # 只测试2个科目
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
    print(f"\n执行SQL,参数数量: {len(args)}")
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

print(f"时间参数:")
print(f"  start_date: {start_date}")
print(f"  end_date: {end_date}")
print(f"  year_start: {year_start}")
print(f"  year_end: {year_end}")
print(f"  month_start: {month_start}")

# 构建项目和科目占位符
comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))

# 构建合同类别条件
contract_condition_fee = ""
contract_condition_detail = ""
if contract_type:
    contract_condition_fee = "AND f.contract_type = %s"
    contract_condition_detail = "AND d.contract_type = %s"

# 先测试简单查询
print("\n=== 测试1: 查找项目 ===")
test_sql = f"SELECT Id, Name FROM erp_base.rf_organize WHERE Id IN ({comm_placeholders}) AND OrganType = 6 AND Is_Delete = 0 AND Status = 0"
test_result = db_query(test_sql, tuple(comm_ids))
print(f"找到项目: {len(test_result)} 个")
for row in test_result:
    print(f"  - {row[1]}")

print("\n=== 测试2: 查找科目 ===")
test_sql2 = f"SELECT id, cost_name FROM erp_base.tb_base_charge_cost WHERE id IN ({cost_placeholders}) AND is_delete = 0"
test_result2 = db_query(test_sql2, tuple(corp_cost_ids))
print(f"找到科目: {len(test_result2)} 个")
for row in test_result2:
    print(f"  - {row[1]}")

print("\n=== 测试3: 查找费用数据 ===")
test_sql3 = f"""
SELECT COUNT(*) as cnt, SUM(due_amount) as total_due
FROM tidb_sync_combine.tb_charge_fee
WHERE comm_id IN ({comm_placeholders})
  AND corp_cost_id IN ({cost_placeholders})
  AND is_delete = 0
"""
test_args3 = []
test_args3.extend(comm_ids)
test_args3.extend(corp_cost_ids)
test_result3 = db_query(test_sql3, tuple(test_args3))
print(f"费用记录数: {test_result3[0][0]}, 总应收: {test_result3[0][1]}")

print("\n=== 测试4: 查找收款明细数据 ===")
test_sql4 = f"""
SELECT COUNT(*) as cnt, SUM(deal_amount) as total_deal
FROM tidb_sync_combine.tb_charge_receipts_detail
WHERE comm_id IN ({comm_placeholders})
  AND corp_cost_id IN ({cost_placeholders})
  AND is_delete = 0
"""
test_args4 = []
test_args4.extend(comm_ids)
test_args4.extend(corp_cost_ids)
test_result4 = db_query(test_sql4, tuple(test_args4))
print(f"收款明细记录数: {test_result4[0][0]}, 总金额: {test_result4[0][1]}")

conn.close()
print("\n测试完成!")
