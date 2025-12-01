# 开票情况统计表：按项目、科目统计应收、开票、回款情况
# 主要逻辑：使用CTE分步汇总应收数据和收款明细数据，最后关联项目、科目维度表
# 性能优化：避免重复扫描大表，使用条件聚合一次性计算所有指标

import datetime

# 解析入参
comm_ids = params.get("comm_ids", [])  # 项目ID列表（多选必填）
corp_cost_ids = params.get("corp_cost_ids", [])  # 公司科目ID列表（必选）
contract_type = params.get("contract_type")  # 合同类别（非必填）
start_date = params.get("start_date")  # 统计开始时间A（格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）
end_date = params.get("end_date")  # 统计截止时间B（格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）

# 计算关键时间点
# 支持多种日期格式：YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DD 或 YYYY-M-D
# 处理开始时间A
if ' ' in start_date:  # 包含时间部分，格式为 YYYY-MM-DD HH:MM:SS
    a_time = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
else:  # 只有日期部分，格式为 YYYY-MM-DD 或 YYYY-M-D
    a_time = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    start_date = a_time.strftime('%Y-%m-%d') + ' 00:00:00'  # 补全为当天开始时间

# 处理结束时间B
if ' ' in end_date:  # 包含时间部分，格式为 YYYY-MM-DD HH:MM:SS
    b_time = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
else:  # 只有日期部分，格式为 YYYY-MM-DD 或 YYYY-M-D
    b_time = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    end_date = b_time.strftime('%Y-%m-%d') + ' 23:59:59'  # 补全为当天结束时间

year_start = f"{b_time.year}-01-01 00:00:00"  # B当年1月1日
year_end = f"{b_time.year}-12-31 23:59:59"  # B当年12月31日
month_start = f"{b_time.year}-{b_time.month:02d}-01 00:00:00"  # B当月1日

# 构建项目和科目占位符
comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))

# 构建合同类别条件
contract_condition_fee = ""
contract_condition_detail = ""
if contract_type:
    contract_condition_fee = "AND f.contract_type = %s"
    contract_condition_detail = "AND d.contract_type = %s"

# 构建SQL
sql = f'''
WITH
-- 应收数据汇总（按项目+科目）
fee_agg AS (
    SELECT
        f.comm_id,
        f.corp_cost_id,
        -- 年初往年欠费（应收部分）：费用时间 < 当年1月1日
        SUM(CASE
            WHEN f.fee_date < %s AND f.is_delete = 0
            THEN f.due_amount
            ELSE 0
        END) AS year_begin_due,
        -- 年初至本月应收：费用时间 >= 当年1月1日 AND <= B
        SUM(CASE
            WHEN f.fee_date >= %s AND f.fee_date <= %s AND f.is_delete = 0
            THEN f.due_amount
            ELSE 0
        END) AS year_to_month_due,
        -- 本年应收：费用时间 >= 当年1月1日 AND <= 当年12月31日
        SUM(CASE
            WHEN f.fee_date >= %s AND f.fee_date <= %s AND f.is_delete = 0
            THEN f.due_amount
            ELSE 0
        END) AS year_due
    FROM tidb_sync_combine.tb_charge_fee f
    WHERE f.comm_id IN ({comm_placeholders})
      AND f.corp_cost_id IN ({cost_placeholders})
      {contract_condition_fee}
    GROUP BY f.comm_id, f.corp_cost_id
),
-- 收款明细数据汇总（按项目+科目）
receipt_agg AS (
    SELECT
        d.comm_id,
        d.corp_cost_id,
        -- 年初往年欠费（已收部分）：操作时间 < 当年1月1日 AND 费用时间 < 当年1月1日
        SUM(CASE
            WHEN d.deal_date < %s AND d.fee_date < %s
                 AND d.deal_type IN ('减免', '减免红冲', '预存冲抵', '预存冲抵红冲', '代扣', '托收确认', '实收', '实收红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS year_begin_paid,
        -- 本月开票：操作时间 > 当月1日 AND <= B
        SUM(CASE
            WHEN d.deal_date > %s AND d.deal_date <= %s
                 AND d.deal_type IN ('托收', '代扣', '实收', '实收红冲', '预存', '预存红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS month_invoice,
        -- 当年累计开票：操作时间 > 当年1月1日 AND <= B
        SUM(CASE
            WHEN d.deal_date > %s AND d.deal_date <= %s
                 AND d.deal_type IN ('托收', '代扣', '实收', '实收红冲', '预存', '预存红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS year_invoice,
        -- 本月回款：操作时间 > 当月1日 AND <= B
        SUM(CASE
            WHEN d.deal_date > %s AND d.deal_date <= %s
                 AND d.deal_type IN ('托收确认', '代扣', '实收', '实收红冲', '预存', '预存红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS month_payment,
        -- 本年累计回款：操作时间 > 当年1月1日 AND <= B
        SUM(CASE
            WHEN d.deal_date > %s AND d.deal_date <= %s
                 AND d.deal_type IN ('托收确认', '代扣', '实收', '实收红冲', '预存', '预存红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS year_payment
    FROM tidb_sync_combine.tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      {contract_condition_detail}
    GROUP BY d.comm_id, d.corp_cost_id
),
-- 本月托收未确认
month_unpaid AS (
    SELECT
        d.comm_id,
        d.corp_cost_id,
        SUM(d.deal_amount) AS amount
    FROM tidb_sync_combine.tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      AND d.deal_date > %s
      AND d.deal_date <= %s
      AND d.deal_type = '托收'
      AND d.is_delete = 0
      AND NOT EXISTS (
          SELECT 1 FROM tidb_sync_combine.tb_charge_receipts_detail d2
          WHERE d2.fee_id = d.fee_id
            AND d2.deal_type = '托收确认'
            AND d2.is_delete = 0
      )
    GROUP BY d.comm_id, d.corp_cost_id
),
-- 本年托收未确认
year_unpaid AS (
    SELECT
        d.comm_id,
        d.corp_cost_id,
        SUM(d.deal_amount) AS amount
    FROM tidb_sync_combine.tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      AND d.deal_date > %s
      AND d.deal_date <= %s
      AND d.deal_type = '托收'
      AND d.is_delete = 0
      AND NOT EXISTS (
          SELECT 1 FROM tidb_sync_combine.tb_charge_receipts_detail d2
          WHERE d2.fee_id = d.fee_id
            AND d2.deal_type = '托收确认'
            AND d2.is_delete = 0
      )
    GROUP BY d.comm_id, d.corp_cost_id
),
-- 往年托收未确认
prev_year_unpaid AS (
    SELECT
        d.comm_id,
        d.corp_cost_id,
        SUM(d.deal_amount) AS amount
    FROM tidb_sync_combine.tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      AND d.deal_date < %s
      AND d.deal_type = '托收'
      AND d.is_delete = 0
      AND NOT EXISTS (
          SELECT 1 FROM tidb_sync_combine.tb_charge_receipts_detail d2
          WHERE d2.fee_id = d.fee_id
            AND d2.deal_type = '托收确认'
            AND d2.is_delete = 0
      )
    GROUP BY d.comm_id, d.corp_cost_id
)
-- 主查询：关联项目、科目、汇总数据
SELECT
    COALESCE(p.Name, '') AS area_name,
    o.Name AS project_name,
    c.cost_name AS cost_name,
    COALESCE(f.year_begin_due, 0) - COALESCE(r.year_begin_paid, 0) AS year_begin_debt,
    COALESCE(f.year_to_month_due, 0) AS year_to_month_due,
    COALESCE(f.year_due, 0) AS year_due,
    COALESCE(pyu.amount, 0) AS prev_year_invoice_unpaid,
    COALESCE(r.month_invoice, 0) AS month_invoice,
    COALESCE(r.year_invoice, 0) AS year_invoice,
    COALESCE(r.month_payment, 0) AS month_payment,
    COALESCE(r.year_payment, 0) AS year_payment,
    COALESCE(mu.amount, 0) AS month_invoice_unpaid,
    COALESCE(yu.amount, 0) AS year_invoice_unpaid,
    o.Id AS comm_id,
    c.id AS corp_cost_id
FROM erp_base.rf_organize o
LEFT JOIN erp_base.rf_organize p ON o.ParentId = p.Id
CROSS JOIN erp_base.tb_base_charge_cost c
LEFT JOIN fee_agg f ON o.Id = f.comm_id AND c.id = f.corp_cost_id
LEFT JOIN receipt_agg r ON o.Id = r.comm_id AND c.id = r.corp_cost_id
LEFT JOIN month_unpaid mu ON o.Id = mu.comm_id AND c.id = mu.corp_cost_id
LEFT JOIN year_unpaid yu ON o.Id = yu.comm_id AND c.id = yu.corp_cost_id
LEFT JOIN prev_year_unpaid pyu ON o.Id = pyu.comm_id AND c.id = pyu.corp_cost_id
WHERE o.Id IN ({comm_placeholders})
  AND c.id IN ({cost_placeholders})
  AND o.OrganType = 6
  AND o.Is_Delete = 0
  AND o.Status = 0
  AND c.is_delete = 0
ORDER BY o.Name, c.sort, c.cost_name
'''

# 组装参数（按SQL中%s出现顺序 - SELECT中的%s先于WHERE中的%s）
args = []

# CTE fee_agg - SELECT中的CASE statements 日期参数 (在WHERE之前!)
args.append(year_start)  # line 55: f.fee_date < %s
args.append(year_start)  # line 61: f.fee_date >= %s
args.append(end_date)    # line 61: f.fee_date <= %s
args.append(year_start)  # line 67: f.fee_date >= %s
args.append(year_end)    # line 67: f.fee_date <= %s

# CTE fee_agg - WHERE clause: comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)
if contract_type:
    args.append(contract_type)

# CTE receipt_agg - SELECT中的CASE statements 日期参数 (在WHERE之前!)
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

# CTE receipt_agg - WHERE clause: comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)
if contract_type:
    args.append(contract_type)

# CTE month_unpaid - WHERE中的日期参数 (在SELECT之后,不需要调整)
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(month_start) # line 137: d.deal_date > %s
args.append(end_date)    # line 138: d.deal_date <= %s

# CTE year_unpaid - WHERE中的日期参数
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(year_start)  # line 158: d.deal_date > %s
args.append(end_date)    # line 159: d.deal_date <= %s

# CTE prev_year_unpaid - WHERE中的日期参数
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(year_start)  # line 179: d.deal_date < %s

# 主查询 WHERE - comm_id IN (...), corp_cost_id IN (...)
args.extend(comm_ids)
args.extend(corp_cost_ids)

# 执行查询
dataRows = db_query(sql, tuple(args))

# 返回结果
set_result(rows=dataRows, message="查询成功")
