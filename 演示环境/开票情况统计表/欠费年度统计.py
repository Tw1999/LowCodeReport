# 根据需求文档，实现欠费年度统计报表，按项目、科目统计各年度欠费金额并支持横向年度对比
# 思路：从费用应收表计算应收金额，从收费明细表统计实收、冲抵、减免等冲销金额，计算净欠费金额，按年度动态列展示

# 解析入参
comm_id = params.get("comm_id")
start_date = params.get("start_date")
end_date = params.get("end_date")
corp_cost_id = params.get("corp_cost_id")
deal_date_end = params.get("deal_date_end")  # 收款截至时间
resource_status = params.get("resource_status")  # 交付状态

# 查询涉及的所有年度
year_sql = '''
SELECT DISTINCT DATE_FORMAT(fee_date, '%%Y') AS annual
FROM tidb_sync_combine.tb_charge_fee f
WHERE f.is_delete = 0
    AND f.comm_id = %s
    AND f.fee_date >= %s
    AND f.fee_date <= %s
'''

args = [comm_id, start_date, end_date]
if corp_cost_id:
    year_sql += '''
        AND f.corp_cost_id = %s
    '''
    args.append(corp_cost_id)

year_sql += '''
ORDER BY annual DESC
'''

year_rows = db_query(year_sql, tuple(args))
years = [row['annual'] for row in year_rows if row.get('annual')]

if not years:
    set_result(rows=[], message="暂无数据")
else:
    # 动态生成年度列
    year_columns = []
    year_case_list = []
    for i, year in enumerate(years):
        col_name = f"nd_debts_amount{i+1}"
        year_columns.append(col_name)
        year_case_list.append(f"IFNULL(MAX(CASE WHEN LEFT(c.fee_month, 4) = '{year}' THEN c.欠费金额 END), 0) AS {col_name}")
    
    year_cases = ',\n        '.join(year_case_list)
    
    # 构建主查询SQL
    sql = f'''
    WITH 本期应收 AS (
        SELECT
            f.comm_id,
            f.corp_cost_id,
            DATE_FORMAT(f.fee_date, '%%Y-%%m') AS fee_month,
            SUM(f.debts_amount) AS due_amount
        FROM tidb_sync_combine.tb_charge_fee f
        WHERE f.is_delete = 0
            AND f.comm_id = %s
            AND f.fee_date >= %s
            AND f.fee_date <= %s
    '''
    
    args = [comm_id, start_date, end_date]
    if corp_cost_id:
        sql += '''
            AND f.corp_cost_id = %s
        '''
        args.append(corp_cost_id)
    if resource_status:
        sql += '''
            AND f.resource_status = %s
        '''
        args.append(resource_status)
    
    sql += '''
        GROUP BY f.comm_id, f.corp_cost_id, DATE_FORMAT(f.fee_date, '%%Y-%%m')
    ),
    费用明细分类 AS (
        SELECT
            d.comm_id,
            d.corp_cost_id,
            DATE_FORMAT(d.fee_date, '%%Y-%%m') AS fee_month,
            CASE 
                WHEN d.deal_type IN ('实收', '托收确认', '代扣') THEN d.deal_amount
                WHEN d.deal_type IN ('实收红冲') THEN -d.deal_amount
                ELSE 0 
            END AS 实收金额,
            CASE 
                WHEN d.deal_type IN ('预存冲抵') THEN d.deal_amount
                WHEN d.deal_type IN ('预存冲抵红冲') THEN -d.deal_amount
                ELSE 0 
            END AS 冲抵金额,
            CASE 
                WHEN d.deal_type IN ('减免') THEN d.deal_amount
                WHEN d.deal_type IN ('减免红冲') THEN -d.deal_amount
                ELSE 0 
            END AS 减免金额
        FROM tidb_sync_combine.tb_charge_receipts_detail d
        WHERE d.is_delete = 0
            AND d.comm_id = %s
            AND d.deal_date <= %s
    '''
    
    args.append(comm_id)
    args.append(deal_date_end + ' 23:59:59' if deal_date_end else end_date + ' 23:59:59')
    
    if corp_cost_id:
        sql += '''
            AND d.corp_cost_id = %s
        '''
        args.append(corp_cost_id)
    if resource_status:
        sql += '''
            AND d.resource_status = %s
        '''
        args.append(resource_status)
    
    sql += '''
    ),
    费用明细汇总 AS (
        SELECT 
            comm_id,
            corp_cost_id,
            fee_month,
            SUM(实收金额) AS 实收金额,
            SUM(冲抵金额) AS 冲抵金额,
            SUM(减免金额) AS 减免金额
        FROM 费用明细分类
        GROUP BY comm_id, corp_cost_id, fee_month
    ),
    欠费计算 AS (
        SELECT 
            COALESCE(a.comm_id, b.comm_id) AS comm_id,
            COALESCE(a.corp_cost_id, b.corp_cost_id) AS corp_cost_id,
            COALESCE(a.fee_month, b.fee_month) AS fee_month,
            COALESCE(a.due_amount, 0) AS 应收金额,
            COALESCE(b.实收金额, 0) AS 实收金额,
            COALESCE(b.冲抵金额, 0) AS 冲抵金额,
            COALESCE(b.减免金额, 0) AS 减免金额,
            COALESCE(a.due_amount, 0) - COALESCE(b.实收金额, 0) - COALESCE(b.冲抵金额, 0) - COALESCE(b.减免金额, 0) AS 欠费金额
        FROM 本期应收 a
        FULL OUTER JOIN 费用明细汇总 b 
            ON a.comm_id = b.comm_id 
            AND a.corp_cost_id = b.corp_cost_id 
            AND a.fee_month = b.fee_month
    )
    SELECT 
        c.comm_id,
        o.Name AS comm_name,
        o.organcode,
        c.corp_cost_id,
        cc.cost_name AS corp_cost_name,
        cc.sort,
        SUM(c.欠费金额) AS total_debts_amount,
        {year_cases}
    FROM 欠费计算 c
    LEFT JOIN pms_base.rf_organize o ON c.comm_id = o.Id AND o.OrganType = 6 AND o.Is_Delete = 0 AND o.Status = 0
    LEFT JOIN erp_base.tb_base_charge_cost cc ON c.corp_cost_id = cc.id AND cc.is_delete = 0
    WHERE c.欠费金额 > 0
    GROUP BY c.comm_id, o.Name, o.organcode, c.corp_cost_id, cc.cost_name, cc.sort
    ORDER BY o.organcode, cc.sort
    '''
    
    dataRows = db_query(sql, tuple(args))
    set_result(rows=dataRows, message="查询成功")