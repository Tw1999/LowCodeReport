# 参数计数测试 - 不连接数据库
import datetime

# 模拟 params
params = {
    "comm_ids": ['id1', 'id2', 'id3', 'id4', 'id5', 'id6', 'id7', 'id8', 'id9', 'id10'],  # 10个
    "corp_cost_ids": ['c' + str(i) for i in range(1, 46)],  # 45个
    "start_date": "2001-1-1",
    "end_date": "2025-12-31",
    "contract_type": None
}

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

print(f"项目ID数量: {len(comm_ids)}")
print(f"科目ID数量: {len(corp_cost_ids)}")
print(f"合同类型: {contract_type}")
print(f"\n时间参数:")
print(f"  start_date: {start_date}")
print(f"  end_date: {end_date}")
print(f"  year_start: {year_start}")
print(f"  year_end: {year_end}")
print(f"  month_start: {month_start}")

# 构建占位符
comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))

print(f"\n占位符:")
print(f"  comm_placeholders: {len(comm_ids)} 个 %s")
print(f"  cost_placeholders: {len(corp_cost_ids)} 个 %s")

# 构建SQL(简化版,用于计数%s)
contract_condition_fee = ""
contract_condition_detail = ""
if contract_type:
    contract_condition_fee = "AND f.contract_type = %s"
    contract_condition_detail = "AND d.contract_type = %s"

sql_template = f'''
WITH
fee_agg AS (
    SELECT f.comm_id, f.corp_cost_id,
        SUM(CASE WHEN f.fee_date < %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_begin_due,
        SUM(CASE WHEN f.fee_date >= %s AND f.fee_date <= %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_to_month_due,
        SUM(CASE WHEN f.fee_date >= %s AND f.fee_date <= %s AND f.is_delete = 0 THEN f.due_amount ELSE 0 END) AS year_due
    FROM tb_charge_fee f
    WHERE f.comm_id IN ({comm_placeholders})
      AND f.corp_cost_id IN ({cost_placeholders})
      {contract_condition_fee}
    GROUP BY f.comm_id, f.corp_cost_id
),
receipt_agg AS (
    SELECT d.comm_id, d.corp_cost_id,
        SUM(CASE WHEN d.deal_date < %s AND d.fee_date < %s AND ... THEN d.deal_amount ELSE 0 END) AS year_begin_paid,
        SUM(CASE WHEN d.deal_date > %s AND d.deal_date <= %s AND ... THEN d.deal_amount ELSE 0 END) AS month_invoice,
        SUM(CASE WHEN d.deal_date > %s AND d.deal_date <= %s AND ... THEN d.deal_amount ELSE 0 END) AS year_invoice,
        SUM(CASE WHEN d.deal_date > %s AND d.deal_date <= %s AND ... THEN d.deal_amount ELSE 0 END) AS month_payment,
        SUM(CASE WHEN d.deal_date > %s AND d.deal_date <= %s AND ... THEN d.deal_amount ELSE 0 END) AS year_payment
    FROM tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      {contract_condition_detail}
    GROUP BY d.comm_id, d.corp_cost_id
),
month_unpaid AS (
    SELECT d.comm_id, d.corp_cost_id, SUM(d.deal_amount) AS amount
    FROM tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      AND d.deal_date > %s
      AND d.deal_date <= %s
      AND ...
    GROUP BY d.comm_id, d.corp_cost_id
),
year_unpaid AS (
    SELECT d.comm_id, d.corp_cost_id, SUM(d.deal_amount) AS amount
    FROM tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      AND d.deal_date > %s
      AND d.deal_date <= %s
      AND ...
    GROUP BY d.comm_id, d.corp_cost_id
),
prev_year_unpaid AS (
    SELECT d.comm_id, d.corp_cost_id, SUM(d.deal_amount) AS amount
    FROM tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      AND d.deal_date < %s
      AND ...
    GROUP BY d.comm_id, d.corp_cost_id
)
SELECT ...
FROM erp_base.rf_organize o
WHERE o.Id IN ({comm_placeholders})
  AND c.id IN ({cost_placeholders})
'''

# 计数SQL中的%s占位符
placeholder_count = sql_template.count('%s')
print(f"\nSQL中的%s占位符总数: {placeholder_count}")

# 计算args数量
args_count = 0

# fee_agg WHERE
args_count += len(comm_ids)  # comm_id IN
args_count += len(corp_cost_ids)  # corp_cost_id IN
if contract_type:
    args_count += 1
# fee_agg CASE
args_count += 5  # 5个日期参数

# receipt_agg WHERE
args_count += len(comm_ids)
args_count += len(corp_cost_ids)
if contract_type:
    args_count += 1
# receipt_agg CASE
args_count += 10  # 10个日期参数

# month_unpaid WHERE + dates
args_count += len(comm_ids)
args_count += len(corp_cost_ids)
args_count += 2  # 2个日期参数

# year_unpaid WHERE + dates
args_count += len(comm_ids)
args_count += len(corp_cost_ids)
args_count += 2  # 2个日期参数

# prev_year_unpaid WHERE + date
args_count += len(comm_ids)
args_count += len(corp_cost_ids)
args_count += 1  # 1个日期参数

# 主查询 WHERE
args_count += len(comm_ids)
args_count += len(corp_cost_ids)

print(f"预期args数量: {args_count}")
print(f"\n分解:")
print(f"  fee_agg: {len(comm_ids)} + {len(corp_cost_ids)} + {1 if contract_type else 0} + 5 = {len(comm_ids) + len(corp_cost_ids) + (1 if contract_type else 0) + 5}")
print(f"  receipt_agg: {len(comm_ids)} + {len(corp_cost_ids)} + {1 if contract_type else 0} + 10 = {len(comm_ids) + len(corp_cost_ids) + (1 if contract_type else 0) + 10}")
print(f"  month_unpaid: {len(comm_ids)} + {len(corp_cost_ids)} + 2 = {len(comm_ids) + len(corp_cost_ids) + 2}")
print(f"  year_unpaid: {len(comm_ids)} + {len(corp_cost_ids)} + 2 = {len(comm_ids) + len(corp_cost_ids) + 2}")
print(f"  prev_year_unpaid: {len(comm_ids)} + {len(corp_cost_ids)} + 1 = {len(comm_ids) + len(corp_cost_ids) + 1}")
print(f"  main query: {len(comm_ids)} + {len(corp_cost_ids)} = {len(comm_ids) + len(corp_cost_ids)}")
print(f"  合计: {6 * len(comm_ids) + 6 * len(corp_cost_ids) + 20}")
