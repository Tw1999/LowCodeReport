import datetime

# 分页查询权限表，默认每页100条，默认第1页
page = int(params.get("page", 1))  # 页码，从1开始
page_size = int(params.get("page_size", 100))  # 每页条数，默认100
offset = (page - 1) * page_size  # 计算偏移量

# === 参数解析 ===
comm_id = params.get("comm_id", "")
cost_ids = params.get("cost_ids", "")
start_date = params.get("start_date", "")
end_date = params.get("end_date", "")
customer_name = params.get("customer_name", "").strip()
jzsj = params.get("jzsj", "")  # 截止时间

# === 处理 cost_ids 多选参数（可选）===
cost_id_list = []
if cost_ids:
    if isinstance(cost_ids, str):
        cost_id_list = [x.strip().strip("'").strip('"') for x in cost_ids.split(',') if x.strip()]
    elif isinstance(cost_ids, (list, tuple)):
        cost_id_list = [str(x).strip().strip("'").strip('"') for x in cost_ids]

# === 构建 deal_type 条件 ===
deal_types = ['实收', '代扣', '托收确认', '实收红冲', '预存冲抵', '预存冲抵红冲', '减免', '减免红冲']
deal_type_placeholders = ','.join(['%s'] * len(deal_types))

# === 第一步：查询所有涉及的科目名称（按 sort 排序）===
cost_sql = '''
SELECT DISTINCT
    cost.cost_name,
    cost.sort
FROM tidb_sync_combine.tb_charge_fee fee
LEFT JOIN tidb_sync_combine.tb_base_masterdata_customer_comm customer 
    ON lower(fee.customer_id) = lower(customer.id)
LEFT JOIN tidb_sync_combine.tb_charge_fee_his his 
    ON lower(his.id) = lower(fee.id) 
    AND his.deal_date <= %s 
    AND IFNULL(his.is_delete, 0) = 0 
    AND his.due_amount < 0
LEFT JOIN tidb_sync_combine.tb_charge_cost cost 
    ON lower(fee.cost_id) = lower(cost.id)
LEFT JOIN (
    SELECT
        fee_id,
        SUM(IFNULL(deal_amount, 0)) AS deal_amount
    FROM tidb_sync_combine.tb_charge_receipts_detail 
    WHERE
        deal_type IN ('实收', '代扣', '托收确认', '实收红冲', '预存冲抵', '预存冲抵红冲', '减免', '减免红冲') 
        AND is_delete = 0 
        AND fee_date BETWEEN %s AND %s 
        AND deal_date <= %s 
    GROUP BY fee_id 
) sk ON lower(fee.id) = lower(sk.fee_id)
WHERE
    fee.comm_id = %s and fee.is_delete = 0 AND fee.fee_date BETWEEN %s AND %s 
    AND (IFNULL(fee.due_amount, 0) + IFNULL(his.due_amount, 0) + IFNULL(sk.deal_amount, 0)) <> 0
    AND cost.cost_name IS NOT NULL
'''

cost_args = []

cost_args.extend([jzsj, start_date, end_date, jzsj, comm_id, start_date, end_date])

if customer_name:
    cost_sql += " AND customer.name LIKE %s"
    cost_args.append(f"%{customer_name}%")

if cost_id_list:
    cost_placeholders = ','.join(['%s'] * len(cost_id_list))
    cost_sql += f" AND fee.corp_cost_id IN ({cost_placeholders})"
    cost_args.extend(cost_id_list)

cost_sql += '''
ORDER BY 
    IFNULL(cost.sort, 9999),
    cost.cost_name
'''

cost_rows = db_query(cost_sql, tuple(cost_args))
cost_names = [row['cost_name'] for row in cost_rows if row.get('cost_name')]

# === 如果没有数据，返回空 ===
if not cost_names:
    set_result(rows=[], message="暂无数据", total_count=0)
else:
    # === 第二步：动态生成科目列的 CASE WHEN ===
    case_columns = []
    for name in cost_names:
        safe_name = name.replace("`", "``")
        # 金额列
        case_columns.append(
            f"SUM(CASE WHEN d.cost_name = %s THEN f.debts_amount ELSE 0 END) AS `{safe_name}`"
        )
        # 欠费期间列：拼成 "开始-结束"
        case_columns.append(
            f"""CASE 
                WHEN MIN(CASE WHEN d.cost_name = %s THEN f.fee_start_date END) IS NOT NULL 
                 AND MAX(CASE WHEN d.cost_name = %s THEN f.fee_end_date END) IS NOT NULL
                THEN CONCAT(
                    DATE_FORMAT(MIN(CASE WHEN d.cost_name = %s THEN f.fee_start_date END), '%%Y.%%m.%%d'),
                    '-',
                    DATE_FORMAT(MAX(CASE WHEN d.cost_name = %s THEN f.fee_end_date END), '%%Y.%%m.%%d')
                )
                ELSE ''
            END AS `{safe_name}欠费期间`"""
        )
    
    columns_sql = ',\n    '.join(case_columns)

    # === 第三步：构建最终 SQL ===
    sql = f'''
    SELECT
        '' area_name,
        org.Name AS comm_name,
        '' resource_group_name,
        f.comm_id,
        f.customer_id,
        f.resource_id,
        CASE 
            WHEN res.resource_type = 3 THEN '房屋'
            WHEN res.resource_type = 5 THEN '车位'
            ELSE ''
        END AS resource_type,
        res.resource_code,
        res.resource_name,
        res.calc_area,
        CASE 
            WHEN res.resource_status = '1' THEN '自持'
            WHEN res.resource_status = '2' THEN '待售'
            WHEN res.resource_status = '3' THEN '已售未收'
            WHEN res.resource_status = '4' THEN '已收'
            WHEN res.resource_status = '5' THEN '已收未装'
            WHEN res.resource_status = '6' THEN '已收装修'
            WHEN res.resource_status = '7' THEN '已装未住'
            WHEN res.resource_status = '8' THEN '入住'
            WHEN res.resource_status = '9' THEN '未售出租'
            ELSE ''
        END AS resource_status,
        customer.name AS customer_name,
        customer.mobile,
        SUM(f.debts_amount) AS 合计欠费金额,
        CONCAT( MIN(DATE_FORMAT(f.fee_start_date, '%%Y.%%m.%%d')),MAX(DATE_FORMAT(f.fee_end_date, '%%Y.%%m.%%d'))) as 合计欠费期间,
        {columns_sql}
    FROM (
        SELECT
            fee.id ,
            fee.comm_id,
            fee.customer_id, 
            fee.resource_id, 
            fee.cost_id,
            fee.corp_cost_id,
            fee.fee_date,
            fee.fee_start_date,
            fee.fee_end_date,

            ( IFNULL(fee.due_amount, 0) + IFNULL(his.due_amount, 0) + IFNULL(sk.deal_amount, 0) ) AS debts_amount
        FROM tidb_sync_combine.tb_charge_fee fee
        LEFT JOIN tidb_sync_combine.tb_charge_fee_his his 
            ON lower(his.id) = lower(fee.id) 
            AND his.deal_date <= %s 
            AND IFNULL(his.is_delete, 0) = 0 
            AND his.due_amount < 0
        LEFT JOIN (
            SELECT
                fee_id,
                SUM(IFNULL(deal_amount, 0)) AS deal_amount
            FROM tidb_sync_combine.tb_charge_receipts_detail 
            WHERE
                deal_type IN ('实收', '代扣', '托收确认', '实收红冲', '预存冲抵', '预存冲抵红冲', '减免', '减免红冲')
                AND is_delete = 0 
                AND fee_date BETWEEN %s AND %s 
                AND deal_date <= %s 
            GROUP BY fee_id 
        ) sk ON lower(fee.id) = lower(sk.fee_id)
        WHERE
            fee.comm_id = %s and fee.is_delete = 0 and fee.fee_date BETWEEN %s AND %s 
            AND (IFNULL(fee.due_amount, 0) + IFNULL(his.due_amount, 0) + IFNULL(sk.deal_amount, 0)) <> 0
    ) f
    LEFT JOIN tidb_sync_combine.tb_base_masterdata_resource res 
        ON lower(f.resource_id) = lower(res.id)
    LEFT JOIN tidb_sync_combine.tb_base_masterdata_customer_comm customer 
        ON lower(f.customer_id) = lower(customer.id)
    LEFT JOIN tidb_sync_combine.tb_charge_cost d  
        ON f.cost_id = d.id
    LEFT JOIN tidb_sync_combine.rf_organize org 
        ON lower(org.Id) = lower(f.comm_id)
    WHERE 1=1 
    '''

    args = []
    # 1. 先为每个科目添加 5 个参数：金额1个 + 期间4个
    for name in cost_names:
        args.append(name)  # 金额
        args.extend([name, name, name, name])  # 期间用4次

    # 2. 添加主查询条件参数
    
    args.extend([jzsj, start_date, end_date, jzsj, comm_id, start_date, end_date])

    # 3. 添加筛选条件
    if customer_name:
        sql += " AND customer.name LIKE %s"
        args.append(f"%{customer_name}%")
    
    if cost_id_list:
        cost_placeholders = ','.join(['%s'] * len(cost_id_list))
        sql += f" AND f.corp_cost_id IN ({cost_placeholders})"
        args.extend(cost_id_list)

    sql += '''
    GROUP BY
        org.Name,
        res.resource_type,
        res.resource_code,
        res.resource_name,
        res.calc_area,
        res.resource_status,
        customer.name,
        customer.mobile,
        f.comm_id,
        f.customer_id,
        f.resource_id
    '''

    # === 先执行COUNT查询获取总条数(在添加分页参数前) ===
    # 由于主查询有GROUP BY，需要用子查询包装来统计分组后的记录数
    count_sql = f"SELECT COUNT(*) as total_count FROM ({sql}) as count_table"

    try:
        count_result = db_query(count_sql, tuple(args))
        total_count = count_result[0]['total_count'] if count_result else 0
    except Exception as e:
        print(f"COUNT查询失败: {str(e)}")
        total_count = 0

    # === 添加ORDER BY和分页(必须在COUNT查询之后) ===
    sql += '''
    ORDER BY
        res.resource_code,
        customer.name
        LIMIT %s OFFSET %s
    '''

    args.append(page_size)
    args.append(offset)

    dataRows = db_query(sql, tuple(args))
    print("Final Args:\n", args)
    set_result(rows=dataRows, message=f"查询成功，共{total_count}条数据，当前第{page}页", total_count=total_count)