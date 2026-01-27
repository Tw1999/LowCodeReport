# 按收费科目、费用年度统计收款、冲抵、退款(动态年份列)
import datetime

# 参数解析
comm_ids = params.get("commId", "")  # 使用一个叫 comm_ids 的变量来处理多个 commId
cost_ids = params.get("costIds", "")
fee_date_start = params.get("feeDateStart")
fee_date_end = params.get("feeDateEnd")
deal_date_start = params.get("dealDateStart")
deal_date_end = params.get("dealDateEnd")
charge_modes = params.get("chargeModes", "")
has_refund = params.get("hasRefund", 0)

# 处理科目ID多选参数
cost_id_list = []
if cost_ids:
    if isinstance(cost_ids, str):
        cost_id_list = [x.strip().strip("'").strip('"') for x in cost_ids.split(',') if x.strip()]
    elif isinstance(cost_ids, (list, tuple)):
        cost_id_list = [str(x).strip().strip("'").strip('"') for x in cost_ids]

# 处理commId多选参数
comm_id_list = []
if comm_ids:
    if isinstance(comm_ids, str):
        comm_id_list = [x.strip().strip("'").strip('"') for x in comm_ids.split(',') if x.strip()]
    elif isinstance(comm_ids, (list, tuple)):
        comm_id_list = [str(x).strip().strip("'").strip('"') for x in comm_ids]

# 处理收款方式多选参数
charge_mode_list = []
if charge_modes:
    if isinstance(charge_modes, str):
        charge_mode_list = [x.strip().strip("'").strip('"') for x in charge_modes.split(',') if x.strip()]
    elif isinstance(charge_modes, (list, tuple)):
        charge_mode_list = [str(x).strip().strip("'").strip('"') for x in charge_modes]

# 构建deal_type条件
deal_types_base = ['实收', '代扣', '托收确认', '实收红冲', '预存冲抵', '预存冲抵红冲']
if str(has_refund) == "0" or has_refund == 0:
    deal_types_base.extend(['退款', '退款红冲'])

deal_type_placeholders = ','.join(['%s'] * len(deal_types_base))

# 第一步:查询涉及的所有年份
year_sql = f'''
SELECT DISTINCT year(b.fee_date) AS annual
FROM tidb_sync_combine.tb_charge_receipts_detail b
WHERE b.is_delete = 0
  AND b.deal_type IN ({deal_type_placeholders})
'''

year_args = []
year_args.extend(deal_types_base)

if comm_id_list:  # 修改为 comm_id_list
    comm_placeholders = ','.join(['%s'] * len(comm_id_list))
    year_sql += f" AND b.comm_id IN ({comm_placeholders})"
    year_args.extend(comm_id_list)  # 添加 comm_id_list

if cost_id_list:
    cost_placeholders = ','.join(['%s'] * len(cost_id_list))
    year_sql += f" AND b.corp_cost_id IN ({cost_placeholders})"
    year_args.extend(cost_id_list)

if deal_date_start and deal_date_end:
    year_sql += " AND b.deal_date BETWEEN %s AND %s"
    year_args.append(deal_date_start)
    year_args.append(deal_date_end)

if charge_mode_list:
    mode_placeholders = ','.join(['%s'] * len(charge_mode_list))
    year_sql += f" AND b.charge_mode IN ({mode_placeholders})"
    year_args.extend(charge_mode_list)

if fee_date_start and fee_date_end:
    year_sql += " AND b.fee_date BETWEEN %s AND %s"
    year_args.append(fee_date_start)
    year_args.append(fee_date_end)

year_sql += " ORDER BY annual"

year_rows = db_query(year_sql, tuple(year_args))
years = [row['annual'] for row in year_rows if row.get('annual')]

# 如果没有数据,返回空结果
if not years:
    set_result(rows=[], message="暂无数据")
else:
    # 第二步:动态生成年份列的CASE WHEN语句
    year_case_list = []
    for year in years:
        year_case_list.append(f'''IFNULL(SUM(
        CASE WHEN YEAR(b.fee_date) = {year} THEN
            IF(
                b.deal_type IN ('实收', '代扣', '托收确认', '预存冲抵', '退款红冲'),
                ABS(IFNULL(b.deal_amount, 0)) + ABS(IFNULL(b.latefee_amount, 0)),
                (ABS(IFNULL(b.deal_amount, 0)) + ABS(IFNULL(b.latefee_amount, 0))) * -1
            )
        END
    ), 0) AS `{year}年`''')
    
    year_columns = ',\n    '.join(year_case_list)

    # 第三步:构建最终SQL
    sql = f'''
    SELECT
        COALESCE(org.Name, '') AS org_name,
        c.Name AS comm_name,
        b.corp_cost_id,
        d.cost_name,
        IFNULL(SUM(
            IF(
                b.deal_type IN ('实收', '代扣', '托收确认', '预存冲抵', '退款红冲'),
                ABS(IFNULL(b.deal_amount, 0)) + ABS(IFNULL(b.latefee_amount, 0)),
                (ABS(IFNULL(b.deal_amount, 0)) + ABS(IFNULL(b.latefee_amount, 0))) * -1
            )
        ), 0) AS sum_amount,
        {year_columns}
    FROM tidb_sync_combine.tb_charge_receipts_detail b
    LEFT JOIN erp_base.rf_organize c ON b.comm_id = c.Id
    LEFT JOIN erp_base.rf_organize org ON c.ParentId = org.Id
    LEFT JOIN erp_base.tb_base_charge_cost d ON b.corp_cost_id = d.id
    WHERE b.is_delete = 0
      AND b.deal_type IN ({deal_type_placeholders})
    '''

    args = []
    args.extend(deal_types_base)

    if comm_id_list:  # 使用 comm_id_list
        comm_placeholders = ','.join(['%s'] * len(comm_id_list))
        sql += f" AND b.comm_id IN ({comm_placeholders})"
        args.extend(comm_id_list)

    if cost_id_list:
        cost_placeholders = ','.join(['%s'] * len(cost_id_list))
        sql += f" AND b.corp_cost_id IN ({cost_placeholders})"
        args.extend(cost_id_list)

    if deal_date_start and deal_date_end:
        sql += " AND b.deal_date BETWEEN %s AND %s"
        args.append(deal_date_start)
        args.append(deal_date_end)

    if charge_mode_list:
        mode_placeholders = ','.join(['%s'] * len(charge_mode_list))
        sql += f" AND b.charge_mode IN ({mode_placeholders})"
        args.extend(charge_mode_list)

    if fee_date_start and fee_date_end:
        sql += " AND b.fee_date BETWEEN %s AND %s"
        args.append(fee_date_start)
        args.append(fee_date_end)

    sql += '''
    GROUP BY b.comm_id, c.Name, b.corp_cost_id, d.cost_name, org.Name
    ORDER BY b.comm_id, b.corp_cost_id
    '''

    dataRows = db_query(sql, tuple(args))
    
    set_result(rows=dataRows, message="查询成功")