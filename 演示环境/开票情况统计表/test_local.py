# 本地测试脚本 - 开票情况统计表
import pymysql
import datetime

# 模拟 params
params = {
    "comm_ids": ['0591cfbd-d915-48d6-9831-7f6201cd4d3e','0d8400d6-d728-4f72-9e6c-08bb9bf5828d','21bea8c6-46e8-4d5f-9ead-cc1ff8e3b90e','48b34bc1-4274-4be5-afac-3f252eda05d8','4eb68e2f-d1f4-486d-b7bf-d0403f8a76e7','73b796a2-74a9-4406-af1c-15ee0218e82e','ac3ece62-af2d-45b7-b644-7a60961f8abd','e1c078af-62ac-4923-9267-7f99d5dffb6a','e236f7bb-626a-4288-83bc-405d4a44545c','e5c4dfcb-e1db-443a-ab47-bf18eb48a478'],
    "corp_cost_ids": ['07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a','09fccd77-0878-4e25-a817-0fb03c305dc1','15d56143-bd27-4ccd-898d-991081694fb3','1904e02c-17d4-4796-a9eb-9eb3ed5dfe2f','28f767a4-b1d6-45fd-92d5-b7668a62d1e9','2a536b39-f4a8-4f13-af61-ad63f56989be','2bb34b1c-b7f4-438d-968d-b56a1de7be64','3d55cb31-2cee-47c6-9883-615b53167d88','40709b88-ce74-48a3-a60b-45758d2efe0e','42605ba4-83f0-4de8-a742-4e64c1081566','4416974b-35ea-4f67-b655-6593cf57b10b','479e2925-a00e-4f62-b35e-cd5ed0405381','4dcaa913-d588-4f30-b03b-11c21810f0d2','5bc4c724-ecc3-4ba3-8ea7-038154f55a13','5ef3f6e7-ec98-4dde-8321-94ba79b5d22c','631eeeaf-0464-4bb2-b1f3-4f0096b11309','6c8912d4-8a96-4290-af65-3807db983925','6cf57fd4-f314-4038-99bb-3af473d0efb5','6dcedf04-1413-4449-8528-e8f017328c68','7e08fcc5-7ed8-4742-9e9e-ef3fa634a875','85580050-8858-4111-9e96-57d825ce0ffc','8c0ab63d-6023-4c89-b4f7-ab886338c033','8c9b4770-51b4-4b25-bfb5-7df38de85ebb','9002c943-2f8f-4f7f-b8f6-bdf818c12586','9c6e5462-e639-4eb7-80e8-b518edeb54ff','a32342c8-94e5-40c1-9e72-903e111aa7e5','a7c26688-f20a-11ec-9bda-00163e03b5f6','a7c2675b-f20a-11ec-9bda-00163e03b5f6','a7c26813-f20a-11ec-9bda-00163e03b5f6','a7c2689b-f20a-11ec-9bda-00163e03b5f6','a7c268c0-f20a-11ec-9bda-00163e03b5f6','a7c26a89-f20a-11ec-9bda-00163e03b5f6','a7c26bca-f20a-11ec-9bda-00163e03b5f6','a7c26bec-f20a-11ec-9bda-00163e03b5f6','a9ced350-60ce-4cf7-8c65-56745bd5b4a7','b8b235bd-3cb0-433d-9256-92b814811245','b9e595b4-004b-4b4b-8582-5bff131fb62d','c0bb1bc6-0336-4db4-be5f-a78c6fe20973','cc9ecb22-0206-4fd1-913f-04a848c42dd1','d754d22f-52c6-4e5b-81d3-7f6d4d7804e6','d93bcc98-6a3d-4603-8285-9d95a64b02ed','e70d1e38-aee1-4b37-9919-8a2d51e0dc4f','e7a382ec-6da7-42e5-80d0-876260ddcc14','ee153b99-0bfb-44e3-9e57-d85a04454a51','f6fd5e82-72ed-4052-a88c-8aafbd744bf6'],
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
    database='erp_charge',
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

print(f"开始时间: {start_date}")
print(f"结束时间: {end_date}")
print(f"年初: {year_start}")
print(f"年末: {year_end}")
print(f"月初: {month_start}")
print(f"项目数: {len(comm_ids)}")
print(f"科目数: {len(corp_cost_ids)}")

# 构建项目和科目占位符
comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))

# 构建合同类别条件
contract_condition_fee = ""
contract_condition_detail = ""
if contract_type:
    contract_condition_fee = "AND f.contract_type = %s"
    contract_condition_detail = "AND d.contract_type = %s"

# 先测试简单查询 - 检查项目是否存在
test_sql = f"SELECT Id, Name FROM erp_base.rf_organize WHERE Id IN ({comm_placeholders}) AND OrganType = 6 AND Is_Delete = 0 AND Status = 0"
test_result = db_query(test_sql, tuple(comm_ids))
print(f"\n找到的项目: {len(test_result)} 个")
for row in test_result[:5]:
    print(f"  - {row[1]} ({row[0]})")

# 测试科目
test_sql2 = f"SELECT id, cost_name FROM erp_base.tb_base_charge_cost WHERE id IN ({cost_placeholders}) AND is_delete = 0"
test_result2 = db_query(test_sql2, tuple(corp_cost_ids))
print(f"\n找到的科目: {len(test_result2)} 个")
for row in test_result2[:5]:
    print(f"  - {row[1]} ({row[0]})")

# 测试费用数据
test_sql3 = f"""
SELECT comm_id, corp_cost_id, COUNT(*) as cnt, SUM(due_amount) as total_due
FROM tb_charge_fee
WHERE comm_id IN ({comm_placeholders})
  AND corp_cost_id IN ({cost_placeholders})
  AND is_delete = 0
GROUP BY comm_id, corp_cost_id
LIMIT 5
"""
test_args3 = []
test_args3.extend(comm_ids)
test_args3.extend(corp_cost_ids)
test_result3 = db_query(test_sql3, tuple(test_args3))
print(f"\n费用数据样例: {len(test_result3)} 条")
for row in test_result3:
    print(f"  - 项目: {row[0][:8]}..., 科目: {row[1][:8]}..., 笔数: {row[2]}, 应收: {row[3]}")

# 组装参数（按SQL中%s出现顺序）
args = []

# CTE fee_agg - WHERE clause: comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)
if contract_type:
    args.append(contract_type)

# CTE fee_agg - CASE statements 中的日期参数
args.append(year_start)  # line 55: f.fee_date < %s
args.append(year_start)  # line 61: f.fee_date >= %s
args.append(end_date)    # line 61: f.fee_date <= %s
args.append(year_start)  # line 67: f.fee_date >= %s
args.append(year_end)    # line 67: f.fee_date <= %s

# CTE receipt_agg - WHERE clause: comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)
if contract_type:
    args.append(contract_type)

# CTE receipt_agg - CASE statements 中的日期参数
args.append(year_start)  # line 84: d.deal_date < %s
args.append(year_start)  # line 84: d.fee_date < %s
args.append(month_start) # line 92: d.deal_date > %s
args.append(end_date)    # line 92: d.deal_date <= %s
args.append(year_start)  # line 100: d.deal_date > %s
args.append(end_date)    # line 100: d.deal_date <= %s
args.append(month_start) # line 108: d.deal_date > %s
args.append(end_date)    # line 108: d.deal_date <= %s
args.append(year_start)  # line 116: d.deal_date > %s
args.append(end_date)    # line 116: d.deal_date <= %s

# CTE month_unpaid - WHERE clause: comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(month_start) # line 137: d.deal_date > %s
args.append(end_date)    # line 138: d.deal_date <= %s

# CTE year_unpaid - WHERE clause: comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(year_start)  # line 158: d.deal_date > %s
args.append(end_date)    # line 159: d.deal_date <= %s

# CTE prev_year_unpaid - WHERE clause: comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(year_start)  # line 179: d.deal_date < %s

# 主查询 WHERE - comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)

print(f"\n总参数数量: {len(args)}")
print(f"预期参数数量: {len(comm_ids)*6 + len(corp_cost_ids)*6 + 15} (不含contract_type)")

conn.close()
