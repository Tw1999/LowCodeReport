# 测试参数顺序 - 完整打印所有参数
import datetime

params = {
    "comm_ids": ['0591cfbd-d915-48d6-9831-7f6201cd4d3e'],
    "corp_cost_ids": ['07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a','09fccd77-0878-4e25-a817-0fb03c305dc1'],
    "start_date": "2001-1-1",
    "end_date": "2025-12-31",
    "contract_type": None
}

comm_ids = params.get("comm_ids", [])
corp_cost_ids = params.get("corp_cost_ids", [])
contract_type = params.get("contract_type")
start_date = params.get("start_date")
end_date = params.get("end_date")

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

print("时间参数:")
print(f"  year_start: {year_start}")
print(f"  year_end: {year_end}")
print(f"  month_start: {month_start}")
print(f"  end_date: {end_date}")

# 构建占位符
comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))

print(f"\ncomm_placeholders: {comm_placeholders}")
print(f"cost_placeholders: {cost_placeholders}")

# 组装参数
args = []

print("\n=== 参数构建顺序 ===")

# CTE fee_agg - WHERE clause
print(f"\n1. fee_agg WHERE clause:")
print(f"   comm_ids: {comm_ids}")
args.extend(comm_ids)
print(f"   corp_cost_ids: {corp_cost_ids}")
args.extend(corp_cost_ids)
if contract_type:
    print(f"   contract_type: {contract_type}")
    args.append(contract_type)

# CTE fee_agg - CASE statements
print(f"\n2. fee_agg CASE statements:")
print(f"   year_start (line 55): {year_start}")
args.append(year_start)
print(f"   year_start (line 61): {year_start}")
args.append(year_start)
print(f"   end_date (line 61): {end_date}")
args.append(end_date)
print(f"   year_start (line 67): {year_start}")
args.append(year_start)
print(f"   year_end (line 67): {year_end}")
args.append(year_end)

# CTE receipt_agg - WHERE clause
print(f"\n3. receipt_agg WHERE clause:")
print(f"   comm_ids: {comm_ids}")
args.extend(comm_ids)
print(f"   corp_cost_ids: {corp_cost_ids}")
args.extend(corp_cost_ids)
if contract_type:
    print(f"   contract_type: {contract_type}")
    args.append(contract_type)

# CTE receipt_agg - CASE statements
print(f"\n4. receipt_agg CASE statements:")
print(f"   year_start (line 84 deal_date): {year_start}")
args.append(year_start)
print(f"   year_start (line 84 fee_date): {year_start}")
args.append(year_start)
print(f"   month_start (line 92): {month_start}")
args.append(month_start)
print(f"   end_date (line 92): {end_date}")
args.append(end_date)
print(f"   year_start (line 100): {year_start}")
args.append(year_start)
print(f"   end_date (line 100): {end_date}")
args.append(end_date)
print(f"   month_start (line 108): {month_start}")
args.append(month_start)
print(f"   end_date (line 108): {end_date}")
args.append(end_date)
print(f"   year_start (line 116): {year_start}")
args.append(year_start)
print(f"   end_date (line 116): {end_date}")
args.append(end_date)

# CTE month_unpaid
print(f"\n5. month_unpaid WHERE + dates:")
print(f"   comm_ids: {comm_ids}")
args.extend(comm_ids)
print(f"   corp_cost_ids: {corp_cost_ids}")
args.extend(corp_cost_ids)
print(f"   month_start (line 137): {month_start}")
args.append(month_start)
print(f"   end_date (line 138): {end_date}")
args.append(end_date)

# CTE year_unpaid
print(f"\n6. year_unpaid WHERE + dates:")
print(f"   comm_ids: {comm_ids}")
args.extend(comm_ids)
print(f"   corp_cost_ids: {corp_cost_ids}")
args.extend(corp_cost_ids)
print(f"   year_start (line 158): {year_start}")
args.append(year_start)
print(f"   end_date (line 159): {end_date}")
args.append(end_date)

# CTE prev_year_unpaid
print(f"\n7. prev_year_unpaid WHERE + date:")
print(f"   comm_ids: {comm_ids}")
args.extend(comm_ids)
print(f"   corp_cost_ids: {corp_cost_ids}")
args.extend(corp_cost_ids)
print(f"   year_start (line 179): {year_start}")
args.append(year_start)

# 主查询 WHERE
print(f"\n8. 主查询 WHERE:")
print(f"   comm_ids: {comm_ids}")
args.extend(comm_ids)
print(f"   corp_cost_ids: {corp_cost_ids}")
args.extend(corp_cost_ids)

print(f"\n=== 最终参数列表 (共{len(args)}个) ===")
for i, arg in enumerate(args, 1):
    print(f"{i:3d}. {arg}")
