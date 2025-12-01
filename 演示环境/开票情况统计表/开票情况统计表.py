# 开票情况统计表：按项目、科目统计应收、开票、回款情况
# 主要逻辑：使用CTE分步汇总应收数据和收款明细数据，最后关联项目、科目维度表
# 性能优化：避免重复扫描大表，使用条件聚合一次性计算所有指标

import datetime

# 解析入参
comm_ids = params.get("comm_ids", [])  # 项目ID列表（多选必填）
corp_cost_ids = params.get("corp_cost_ids", [])  # 公司科目ID列表（必选）

# 确保是列表格式（支持多种输入格式：列表、元组、逗号分隔的字符串）
if isinstance(comm_ids, str):
    comm_ids = [x.strip() for x in comm_ids.split(',') if x.strip()]
elif not isinstance(comm_ids, (list, tuple)):
    comm_ids = list(comm_ids) if comm_ids else []
elif isinstance(comm_ids, tuple):
    comm_ids = list(comm_ids)

if isinstance(corp_cost_ids, str):
    corp_cost_ids = [x.strip() for x in corp_cost_ids.split(',') if x.strip()]
elif not isinstance(corp_cost_ids, (list, tuple)):
    corp_cost_ids = list(corp_cost_ids) if corp_cost_ids else []
elif isinstance(corp_cost_ids, tuple):
    corp_cost_ids = list(corp_cost_ids)

# 清理ID列表中可能存在的引号（生产环境可能传入带引号的值）
comm_ids = [str(x).strip().strip("'").strip('"') for x in comm_ids]
corp_cost_ids = [str(x).strip().strip("'").strip('"') for x in corp_cost_ids]

contract_type = params.get("contract_type")  # 合同类别（非必填）
start_date = params.get("start_date")  # 统计开始时间A（格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）
end_date = params.get("end_date")  # 统计截止时间B（格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）

# 计算关键时间点
# 支持多种日期格式：YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DD 或 YYYY-M-D
# 处理结束时间B
if ' ' in end_date:
    b_time = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
else:
    b_time = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    end_date = b_time.strftime('%Y-%m-%d') + ' 23:59:59'

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
fee_agg AS (
    SELECT
        f.comm_id,
        f.corp_cost_id,
        SUM(CASE
            WHEN f.fee_date < %s AND f.is_delete = 0
            THEN f.due_amount
            ELSE 0
        END) AS year_begin_due,
        SUM(CASE
            WHEN f.fee_date >= %s AND f.fee_date <= %s AND f.is_delete = 0
            THEN f.due_amount
            ELSE 0
        END) AS year_to_month_due,
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
receipt_agg AS (
    SELECT
        d.comm_id,
        d.corp_cost_id,
        SUM(CASE
            WHEN d.deal_date < %s AND d.fee_date < %s
                 AND d.deal_type IN ('减免', '减免红冲', '预存冲抵', '预存冲抵红冲', '代扣', '托收确认', '实收', '实收红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS year_begin_paid,
        SUM(CASE
            WHEN d.deal_date > %s AND d.deal_date <= %s
                 AND d.deal_type IN ('托收', '代扣', '实收', '实收红冲', '预存', '预存红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS month_invoice,
        SUM(CASE
            WHEN d.deal_date > %s AND d.deal_date <= %s
                 AND d.deal_type IN ('托收', '代扣', '实收', '实收红冲', '预存', '预存红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS year_invoice,
        SUM(CASE
            WHEN d.deal_date > %s AND d.deal_date <= %s
                 AND d.deal_type IN ('托收确认', '代扣', '实收', '实收红冲', '预存', '预存红冲')
                 AND d.is_delete = 0
            THEN d.deal_amount
            ELSE 0
        END) AS month_payment,
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
SELECT
    COALESCE(p.Name, '') AS 区域名称,
    o.Name AS 项目名称,
    c.cost_name AS 科目名称,
    COALESCE(f.year_begin_due, 0) - COALESCE(r.year_begin_paid, 0) AS 年初往年欠费,
    COALESCE(f.year_to_month_due, 0) AS 年初至本月应收,
    COALESCE(f.year_due, 0) AS 本年应收,
    COALESCE(pyu.amount, 0) AS 往年开票未回款,
    COALESCE(r.month_invoice, 0) AS 本月开票,
    COALESCE(r.year_invoice, 0) AS 当年累计开票,
    COALESCE(r.month_payment, 0) AS 本月回款,
    COALESCE(r.year_payment, 0) AS 本年累计回款,
    COALESCE(mu.amount, 0) AS 本月开票未回款,
    COALESCE(yu.amount, 0) AS 本年开票未回款
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

# 组装参数（按SQL中%s出现顺序）
args = []

# CTE fee_agg - SELECT中的CASE statements
args.append(year_start)  # year_begin_due: f.fee_date < %s
args.append(year_start)  # year_to_month_due: f.fee_date >= %s
args.append(end_date)    # year_to_month_due: f.fee_date <= %s
args.append(year_start)  # year_due: f.fee_date >= %s
args.append(year_end)    # year_due: f.fee_date <= %s

# CTE fee_agg - WHERE clause
args.extend(comm_ids)
args.extend(corp_cost_ids)
if contract_type:
    args.append(contract_type)

# CTE receipt_agg - SELECT中的CASE statements
args.append(year_start)  # year_begin_paid: d.deal_date < %s
args.append(year_start)  # year_begin_paid: d.fee_date < %s
args.append(month_start) # month_invoice: d.deal_date > %s
args.append(end_date)    # month_invoice: d.deal_date <= %s
args.append(year_start)  # year_invoice: d.deal_date > %s
args.append(end_date)    # year_invoice: d.deal_date <= %s
args.append(month_start) # month_payment: d.deal_date > %s
args.append(end_date)    # month_payment: d.deal_date <= %s
args.append(year_start)  # year_payment: d.deal_date > %s
args.append(end_date)    # year_payment: d.deal_date <= %s

# CTE receipt_agg - WHERE clause
args.extend(comm_ids)
args.extend(corp_cost_ids)
if contract_type:
    args.append(contract_type)

# CTE month_unpaid
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(month_start) # d.deal_date > %s
args.append(end_date)    # d.deal_date <= %s

# CTE year_unpaid
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(year_start)  # d.deal_date > %s
args.append(end_date)    # d.deal_date <= %s

# CTE prev_year_unpaid
args.extend(comm_ids)
args.extend(corp_cost_ids)
args.append(year_start)  # d.deal_date < %s

# 主查询 WHERE
args.extend(comm_ids)
args.extend(corp_cost_ids)

# 执行查询
dataRows = db_query(sql, tuple(args))
set_result(rows=dataRows, message="查询成功")
