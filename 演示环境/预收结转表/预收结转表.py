# 预收结转表：统计项目各科目的预收款项余额及本月结转、开票、预收情况

import datetime

# 解析参数
comm_ids = params.get("comm_ids", [])  # 项目列表
corp_cost_ids = params.get("corp_cost_ids", [])  # 公司科目列表
stat_month = params.get("stat_month", "")  # 统计月份，格式：YYYY-MM
is_exit = params.get("is_exit", "")  # 是否退出（1是0否，空不限）

# 参数校验
if not comm_ids or not corp_cost_ids or not stat_month:
    set_result(rows=[], message="参数错误：项目名称、收费科目、统计月份为必填项")
    exit()

# 处理多选参数格式
if isinstance(comm_ids, str):
    comm_ids = [x.strip() for x in comm_ids.split(',') if x.strip()]
elif isinstance(comm_ids, tuple):
    comm_ids = list(comm_ids)
comm_ids = [str(x).strip().strip("'").strip('"') for x in comm_ids]

if isinstance(corp_cost_ids, str):
    corp_cost_ids = [x.strip() for x in corp_cost_ids.split(',') if x.strip()]
elif isinstance(corp_cost_ids, tuple):
    corp_cost_ids = list(corp_cost_ids)
corp_cost_ids = [str(x).strip().strip("'").strip('"') for x in corp_cost_ids]

# 计算统计月份的时间范围
stat_date = datetime.datetime.strptime(stat_month + "-01", '%Y-%m-%d')
month_start = stat_date.strftime('%Y-%m-%d 00:00:00')
if stat_date.month == 12:
    month_end = stat_date.replace(year=stat_date.year + 1, month=1, day=1)
else:
    month_end = stat_date.replace(month=stat_date.month + 1, day=1)
month_end_str = (month_end - datetime.timedelta(seconds=1)).strftime('%Y-%m-%d 23:59:59')

# 计算本年度范围
year_start = stat_date.strftime('%Y-01-01 00:00:00')
year_end = stat_date.strftime('%Y-12-31 23:59:59')

# 计算次年开始时间
next_year_start = stat_date.replace(year=stat_date.year + 1, month=1, day=1).strftime('%Y-%m-%d 00:00:00')

# 构建占位符
comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))

# 构建SQL - 使用CTE一次性计算所有指标
sql = f'''
WITH base_data AS (
    SELECT
        d.comm_id,
        d.corp_cost_id,
        d.deal_type,
        d.deal_date,
        d.fee_start_date,
        d.deal_amount
    FROM tidb_sync_combine.tb_charge_receipts_detail d
    WHERE d.comm_id IN ({comm_placeholders})
      AND d.corp_cost_id IN ({cost_placeholders})
      AND d.is_delete = 0
      AND d.deal_type IN (
          '预存冲抵', '预存冲抵红冲', '托收确认', '代扣', '实收',
          '退款红冲', '退款', '实收红冲', '预存', '预存红冲', '预存退款', '预存退款红冲', '预存转入', '预存转出'
      )
),
metrics AS (
    SELECT
        comm_id,
        corp_cost_id,
        -- 月初预收余额：操作时间<统计月份1日0时 AND 费用时间>=统计月份1日0时
        SUM(CASE
            WHEN deal_date < %s
                 AND fee_start_date >= %s
                 AND deal_type IN ('预存冲抵', '预存冲抵红冲', '托收确认', '代扣', '实收', '退款红冲', '退款', '实收红冲', '预存', '预存红冲', '预存退款')
            THEN deal_amount
            ELSE 0
        END) AS prev_balance,

        -- 待摊预收款项结转本月：操作时间<统计月份1日0时 AND 费用时间>=统计月份1日0时 AND <=统计月份末
        SUM(CASE
            WHEN deal_date < %s
                 AND fee_start_date >= %s
                 AND fee_start_date <= %s
                 AND deal_type IN ('托收确认', '代扣', '实收', '退款红冲', '退款', '实收红冲')
            THEN deal_amount
            ELSE 0
        END) AS carryover_from_prev,

        -- 本月预存冲抵：操作时间>=统计月份1日0时 AND <=统计月份末 AND 费用时间>=统计月份1日0时 AND <=统计月份末
        SUM(CASE
            WHEN deal_date >= %s
                 AND deal_date <= %s
                 AND fee_start_date >= %s
                 AND fee_start_date <= %s
                 AND deal_type IN ('预存冲抵', '预存冲抵红冲', '实收红冲')
            THEN deal_amount
            ELSE 0
        END) AS curr_offset,

        -- 结转合计：待摊预收款项结转本月 + 本月预存冲抵
        -- 本月开票：操作时间>=统计月份1日0时 AND <=统计月份末 AND 收款类型=托收/代扣/实收/实收红冲/预存/预存红冲
        SUM(CASE
            WHEN deal_date >= %s
                 AND deal_date <= %s
                 AND deal_type IN ('托收', '代扣', '实收', '实收红冲', '预存', '预存红冲')
            THEN deal_amount
            ELSE 0
        END) AS curr_invoice,

        -- 本月预收本年分摊：操作时间>=统计月份1日0时 AND <=统计月份末 AND 费用时间>=次年1月1日0时 AND <=本年12月31日
        SUM(CASE
            WHEN deal_date >= %s
                 AND deal_date <= %s
                 AND fee_start_date >= %s
                 AND fee_start_date <= %s
                 AND deal_type IN ('托收确认', '代扣', '实收', '退款红冲', '退款', '实收红冲')
            THEN deal_amount
            ELSE 0
        END) AS curr_prepay_year,

        -- 本月预收以后年度：操作时间>=统计月份1日0时 AND <=统计月份末 AND 费用时间>=次年1月1日
        SUM(CASE
            WHEN deal_date >= %s
                 AND deal_date <= %s
                 AND fee_start_date >= %s
                 AND deal_type IN ('托收确认', '代扣', '实收', '退款红冲', '退款', '实收红冲')
            THEN deal_amount
            ELSE 0
        END) AS curr_prepay_future,

        -- 本月预存往来（无期限）：操作时间>=统计月份1日0时 AND <=统计月份末 AND 业务类型=预存相关
        SUM(CASE
            WHEN deal_date >= %s
                 AND deal_date <= %s
                 AND deal_type IN ('预存', '预存退款', '预存红冲', '预存退款红冲', '预存转入', '预存转出')
            THEN deal_amount
            ELSE 0
        END) AS curr_prepay_balance
    FROM base_data
    GROUP BY comm_id, corp_cost_id
)
SELECT
    o.Name AS 区域名称,
    p.Name AS 项目名称,
    c.cost_name AS 科目名称,
    m.prev_balance AS 月初预收余额,
    m.carryover_from_prev AS 待摊预收款项结转本月,
    m.curr_offset AS 本月预存冲抵,
    (m.carryover_from_prev + m.curr_offset) AS 结转合计,
    m.curr_invoice AS 本月开票,
    m.curr_prepay_year AS 本月预收本年分摊,
    m.curr_prepay_future AS 本月预收以后年度,
    m.curr_prepay_balance AS 本月预存往来,
    (m.prev_balance - m.carryover_from_prev - m.curr_offset + m.curr_prepay_year + m.curr_prepay_future + m.curr_prepay_balance) AS 月末预收余额
FROM metrics m
LEFT JOIN erp_base.rf_organize p ON m.comm_id = p.Id AND p.OrganType = 6 AND p.Is_Delete = 0
LEFT JOIN erp_base.rf_organize o ON p.ParentId = o.Id AND o.Is_Delete = 0
LEFT JOIN erp_base.tb_base_charge_cost c ON m.corp_cost_id = c.id AND c.is_delete = 0
WHERE 1=1
'''

# 构建参数列表（严格按照SQL中%s出现顺序）
args = []

# 1. WHERE comm_id IN
args.extend(comm_ids)

# 2. WHERE corp_cost_id IN
args.extend(corp_cost_ids)

# 3. 月初预收余额 - deal_date < %s
args.append(month_start)

# 4. 月初预收余额 - fee_start_date >= %s
args.append(month_start)

# 5. 待摊预收款项结转本月 - deal_date < %s
args.append(month_start)

# 6. 待摊预收款项结转本月 - fee_start_date >= %s
args.append(month_start)

# 7. 待摊预收款项结转本月 - fee_start_date <= %s
args.append(month_end_str)

# 8. 本月预存冲抵 - deal_date >= %s
args.append(month_start)

# 9. 本月预存冲抵 - deal_date <= %s
args.append(month_end_str)

# 10. 本月预存冲抵 - fee_start_date >= %s
args.append(month_start)

# 11. 本月预存冲抵 - fee_start_date <= %s
args.append(month_end_str)

# 12. 本月开票 - deal_date >= %s
args.append(month_start)

# 13. 本月开票 - deal_date <= %s
args.append(month_end_str)

# 14. 本月预收本年分摊 - deal_date >= %s
args.append(month_start)

# 15. 本月预收本年分摊 - deal_date <= %s
args.append(month_end_str)

# 16. 本月预收本年分摊 - fee_start_date >= %s
args.append(year_start)

# 17. 本月预收本年分摊 - fee_start_date <= %s
args.append(year_end)

# 18. 本月预收以后年度 - deal_date >= %s
args.append(month_start)

# 19. 本月预收以后年度 - deal_date <= %s
args.append(month_end_str)

# 20. 本月预收以后年度 - fee_start_date >= %s
args.append(next_year_start)

# 21. 本月预存往来 - deal_date >= %s
args.append(month_start)

# 22. 本月预存往来 - deal_date <= %s
args.append(month_end_str)

# 处理是否退出筛选条件
if is_exit == '1':
    # 只统计退出项目
    sql += ' AND p.Status = 1'
elif is_exit == '0':
    # 不统计退出项目
    sql += ' AND (p.Status = 0 OR (p.Status = 1 AND o.time_stamp < %s))'
    args.append(month_start)

sql += ' ORDER BY o.Name, p.Name, c.cost_name'

# 执行查询
dataRows = db_query(sql, tuple(args))
set_result(rows=dataRows, message="查询成功")
