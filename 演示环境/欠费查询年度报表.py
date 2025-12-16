import datetime


def get_fee_statistics_by_criteria(params):
    # 参数解析
    comm_id = params.get("comm_id")
    delivery_status = params.get("delivery_status")
    corp_cost_ids = params.get("corp_cost_ids")
    fee_date_start = params.get("fee_date_start")
    fee_date_end = params.get("fee_date_end")
    receipt_end_date = params.get("receipt_end_date")
    
    if not fee_date_start or not fee_date_end:
        return {"rows": [], "years": [], "message": "费用开始时间和结束时间为必填参数"}
    
    # 处理参数
    if comm_id:
        comm_id = comm_id.strip()
    
    # 处理交付状态参数 - 如果为空转换为空字符串
    if delivery_status:
        delivery_status = delivery_status.strip()
    else:
        delivery_status = ""  # 为空转换为空字符串
    
    fee_date_start = fee_date_start.strip()
    fee_date_end = fee_date_end.strip()
    
    # 处理科目ID多选参数
    cost_id_list = []
    if corp_cost_ids:
        if isinstance(corp_cost_ids, str):
            cost_id_list = [x.strip().strip("'").strip('"') for x in corp_cost_ids.split(',') if x.strip()]
        elif isinstance(corp_cost_ids, (list, tuple)):
            cost_id_list = [str(x).strip().strip("'").strip('"') for x in corp_cost_ids]
    
    # 确保日期格式正确
    fee_date_end_with_time = ensure_end_of_day(fee_date_end)
    
    if receipt_end_date:
        receipt_end_date = receipt_end_date.strip()
        receipt_end_date_with_time = ensure_end_of_day(receipt_end_date)
    else:
        receipt_end_date_with_time = fee_date_end_with_time
    
    # 第一步:查询涉及的所有年份
    year_sql, year_args = build_year_query(
        comm_id=comm_id,
        delivery_status=delivery_status,
        cost_id_list=cost_id_list,
        fee_date_start=fee_date_start,
        fee_date_end=fee_date_end_with_time,
        receipt_end_date=receipt_end_date_with_time
    )
    
    year_rows = db_query(year_sql, tuple(year_args))
    years = [row['annual'] for row in year_rows if row.get('annual')]
    
    # 如果没有数据,返回空结果
    if not years:
        return {"rows": [], "years": [], "message": "暂无数据"}
    
    # 第二步:动态生成年份列的CASE WHEN语句
    year_case_list = []
    for year in years:
        year_case_list.append(f"SUM(CASE WHEN year(aa.fee_date) = {year} THEN aa.欠费金额含税金额 ELSE 0 END) AS `{year}年`")
    
    year_columns = ',\n        '.join(year_case_list)
    
    # 第三步:构建最终SQL
    sql, args = build_main_query_sql(
        comm_id=comm_id,
        delivery_status=delivery_status,
        cost_id_list=cost_id_list,
        fee_date_start=fee_date_start,
        fee_date_end=fee_date_end_with_time,
        receipt_end_date=receipt_end_date_with_time,
        year_columns=year_columns,
        years=years
    )
    
    # 执行查询
    dataRows = db_query(sql, tuple(args))
    
    return {
        "rows": dataRows, 
        "years": years, 
        "message": "查询成功"
    }


def ensure_end_of_day(date_str):
    if ' ' in date_str or 'T' in date_str:
        return date_str
    else:
        return f"{date_str} 23:59:59"


def build_year_query(
    comm_id,
    delivery_status,
    cost_id_list,
    fee_date_start,
    fee_date_end,
    receipt_end_date
):
    """
    构建年份查询SQL
    """
    year_args = []
    
    year_sql = '''
    SELECT DISTINCT year(fee_date) AS annual
    FROM (
        -- 当前欠费的年份
        SELECT a.fee_date
        FROM tb_charge_fee a 
        WHERE a.is_delete = 0 
            AND a.fee_date BETWEEN %s AND %s
    '''
    
    year_args.append(fee_date_start)
    year_args.append(fee_date_end)
    
    if comm_id:
        year_sql += " AND a.comm_id = %s"
        year_args.append(comm_id)
    
    # 处理delivery_status条件，如果为空字符串则不过滤
    if delivery_status and delivery_status != "":
        year_sql += " AND a.resource_status = %s"
        year_args.append(delivery_status)
    
    if cost_id_list:
        cost_placeholders = ','.join(['%s'] * len(cost_id_list))
        year_sql += f" AND a.corp_cost_id IN ({cost_placeholders})"
        year_args.extend(cost_id_list)
    
    year_sql += '''
        
        UNION
        
        -- 收款明细的年份
        SELECT rd.fee_date
        FROM tb_charge_receipts_detail rd 
        WHERE rd.is_delete = 0 
            AND rd.deal_type IN ('实收', '代扣', '托收确认', '实收红冲', '预存冲抵', '预存冲抵红冲', '减免', '减免红冲')
            AND rd.fee_date BETWEEN %s AND %s
            AND rd.deal_date <= %s
    '''
    
    year_args.append(fee_date_start)
    year_args.append(fee_date_end)
    year_args.append(receipt_end_date)
    
    if comm_id:
        year_sql += " AND rd.comm_id = %s"
        year_args.append(comm_id)
    
    # 处理delivery_status条件，如果为空字符串则不过滤
    if delivery_status and delivery_status != "":
        year_sql += " AND rd.resource_status = %s"
        year_args.append(delivery_status)
    
    if cost_id_list:
        cost_placeholders = ','.join(['%s'] * len(cost_id_list))
        year_sql += f" AND rd.corp_cost_id IN ({cost_placeholders})"
        year_args.extend(cost_id_list)
    
    year_sql += '''
        
        UNION
        
        -- 历史欠费的年份
        SELECT a.fee_date
        FROM tb_charge_fee_his a 
        WHERE a.is_delete = 0 
            AND a.fee_date BETWEEN %s AND %s
    '''
    
    year_args.append(fee_date_start)
    year_args.append(fee_date_end)
    
    if comm_id:
        year_sql += " AND a.comm_id = %s"
        year_args.append(comm_id)
    
    # 处理delivery_status条件，如果为空字符串则不过滤
    if delivery_status and delivery_status != "":
        year_sql += " AND a.resource_status = %s"
        year_args.append(delivery_status)
    
    if cost_id_list:
        cost_placeholders = ','.join(['%s'] * len(cost_id_list))
        year_sql += f" AND a.corp_cost_id IN ({cost_placeholders})"
        year_args.extend(cost_id_list)
    
    year_sql += '''
    ) years_data
    ORDER BY annual
    '''
    
    return year_sql, year_args


def build_main_query_sql(
    comm_id,
    delivery_status,
    cost_id_list,
    fee_date_start,
    fee_date_end,
    receipt_end_date,
    year_columns,
    years
):
    """
    构建主查询SQL - 按照欠费逻辑统计
    """
    args = []
    
    # 构建欠费查询条件
    fee_conditions = []
    fee_conditions.append("a.is_delete = 0")
    fee_conditions.append("a.fee_date BETWEEN %s AND %s")
    args.append(fee_date_start)
    args.append(fee_date_end)
    
    if comm_id:
        fee_conditions.append("a.comm_id = %s")
        args.append(comm_id)
    
    # 处理delivery_status条件，如果为空字符串则不过滤
    if delivery_status and delivery_status != "":
        fee_conditions.append("a.resource_status = %s")
        args.append(delivery_status)
    
    if cost_id_list:
        cost_placeholders = ','.join(['%s'] * len(cost_id_list))
        fee_conditions.append(f"a.corp_cost_id IN ({cost_placeholders})")
        args.extend(cost_id_list)
    
    fee_where_clause = " AND ".join(fee_conditions)
    
    # 构建收款冲减查询条件
    receipt_conditions = []
    receipt_conditions.append("rd.is_delete = 0")
    receipt_conditions.append("rd.deal_type IN ('实收', '代扣', '托收确认', '实收红冲', '预存冲抵', '预存冲抵红冲', '减免', '减免红冲')")
    receipt_conditions.append("rd.fee_date BETWEEN %s AND %s")
    receipt_conditions.append("rd.deal_date <= %s")
    args.append(fee_date_start)
    args.append(fee_date_end)
    args.append(receipt_end_date)
    
    if comm_id:
        receipt_conditions.append("rd.comm_id = %s")
        args.append(comm_id)
    
    # 处理delivery_status条件，如果为空字符串则不过滤
    if delivery_status and delivery_status != "":
        receipt_conditions.append("rd.resource_status = %s")
        args.append(delivery_status)
    
    if cost_id_list:
        cost_placeholders = ','.join(['%s'] * len(cost_id_list))
        receipt_conditions.append(f"rd.corp_cost_id IN ({cost_placeholders})")
        args.extend(cost_id_list)
    
    receipt_where_clause = " AND ".join(receipt_conditions)
    
    # 构建历史欠费查询条件
    his_fee_conditions = []
    his_fee_conditions.append("a.is_delete = 0")
    his_fee_conditions.append("a.fee_date BETWEEN %s AND %s")
    args.append(fee_date_start)
    args.append(fee_date_end)
    
    if comm_id:
        his_fee_conditions.append("a.comm_id = %s")
        args.append(comm_id)
    
    # 处理delivery_status条件，如果为空字符串则不过滤
    if delivery_status and delivery_status != "":
        his_fee_conditions.append("a.resource_status = %s")
        args.append(delivery_status)
    
    if cost_id_list:
        cost_placeholders = ','.join(['%s'] * len(cost_id_list))
        his_fee_conditions.append(f"a.corp_cost_id IN ({cost_placeholders})")
        args.extend(cost_id_list)
    
    his_fee_where_clause = " AND ".join(his_fee_conditions)
    
    sql = f'''
    WITH combined_data AS (
        -- 子查询1：当前欠费（正数）
        SELECT 
            bb.name AS 区域名称,
            b.name AS 项目名称,
            e.cost_name AS 收费科目,
            a.fee_date,
            a.due_amount AS 欠费金额含税金额
        FROM tb_charge_fee a 
        LEFT JOIN erp_base.rf_organize b ON a.comm_id = b.id
        LEFT JOIN erp_base.rf_organize bb ON b.parentid = bb.id
        LEFT JOIN erp_base.tb_base_charge_cost e ON a.corp_cost_id = e.id
        WHERE {fee_where_clause}
            AND a.due_amount > 0
            AND b.is_delete = 0
        
        UNION ALL 
        
        -- 子查询2：收款冲减（减少欠费）
        SELECT 
            bb.name AS 区域名称,
            b.name AS 项目名称,
            e.cost_name AS 收费科目,
            rd.fee_date,
            CASE 
                WHEN rd.deal_type IN ('实收', '代扣', '托收确认', '预存冲抵', '减免') 
                    THEN -rd.deal_amount  -- 收款减少欠费
                WHEN rd.deal_type IN ('实收红冲', '预存冲抵红冲', '减免红冲') 
                    THEN rd.deal_amount   -- 红冲增加欠费
                ELSE 0
            END AS 欠费金额含税金额
        FROM tb_charge_receipts_detail rd 
        LEFT JOIN erp_base.rf_organize b ON rd.comm_id = b.id
        LEFT JOIN erp_base.rf_organize bb ON b.parentid = bb.id
        LEFT JOIN erp_base.tb_base_charge_cost e ON rd.corp_cost_id = e.id
        WHERE {receipt_where_clause}
            AND b.is_delete = 0
        
        UNION ALL
        
        -- 子查询3：历史欠费（负数）
        SELECT 
            bb.name AS 区域名称,
            b.name AS 项目名称,
            e.cost_name AS 收费科目,
            a.fee_date,
            a.due_amount AS 欠费金额含税金额
        FROM tb_charge_fee_his a 
        LEFT JOIN erp_base.rf_organize b ON a.comm_id = b.id
        LEFT JOIN erp_base.rf_organize bb ON b.parentid = bb.id
        LEFT JOIN erp_base.tb_base_charge_cost e ON a.corp_cost_id = e.id
        WHERE {his_fee_where_clause}
            AND b.is_delete = 0
    )
    
    SELECT 
        区域名称,
        项目名称,
        收费科目,
        {year_columns},
        SUM(欠费金额含税金额) AS 欠费总额
    FROM combined_data
    GROUP BY 
        区域名称,
        项目名称,
        收费科目
    ORDER BY 
        区域名称,
        项目名称,
        收费科目
    '''
    
    return sql, args


def query_fee_statistics(params):
    return get_fee_statistics_by_criteria(params)