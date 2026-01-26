# 需求：费用明细查询报表，修正票据表不存在的问题
# 思路：移除不存在的tb_charge_receipts表关联，从其他表中获取票据相关信息
# 分页查询权限表，默认每页100条，默认第1页
page = int(params.get("page", 1))  # 页码，从1开始
page_size = int(params.get("page_size", 100))  # 每页条数，默认100
offset = (page - 1) * page_size  # 计算偏移量

# 解析入参
comm_id = params.get("comm_id", "")
start_date = params.get("start_date")
end_date = params.get("end_date")
customer_id = params.get("customer_id")
resource_id = params.get("resource_id")
corp_cost_id = params.get("corp_cost_id", "") 
deal_type_in = params.get("deal_type_in")

# 调试模式参数
# 确保debug_mode是布尔值，支持多种格式的true值
debug_mode =0 
# 调试信息：打印debug_mode值，方便调试


# 新增筛选条件参数
project_name = params.get("project_name")  # 项目名称
cost_name = params.get("cost_name")        # 收费科目名称
stan_name = params.get("stan_name", "")    # 标准名称
# query_scope = params.get("query_scope")    # 查询范围：费用时间/收退款时间
charge_mode = params.get("charge_mode")    # 收退款方式
deal_user = params.get("deal_user")        # 收退款人
bank_name = params.get("bank_name")        # 收款银行
fee_start_date = params.get("fee_start_date")  # 费用开始时间
fee_end_date = params.get("fee_end_date")      # 费用结束时间
bill_sign_start = params.get("bill_sign_start") # 收退款票号起始
bill_sign_end = params.get("bill_sign_end")     # 收退款票号结束
change_bill_sign_start = params.get("change_bill_sign_start") # 换票票号起始
change_bill_sign_end = params.get("change_bill_sign_end")     # 换票票号结束
delivery_status = params.get("delivery_status", "")

# 新增：查询范围参数（全部，实收，预存，退款）
query_range = params.get("query_range", "1")  # 默认查询全部

# 组装SQL - 移除不存在的票据表关联
sql = '''
SELECT
    d.id,
    d.comm_id,
    o1.Name AS area_name,
    o2.Name AS comm_name,
    CASE res.resource_type
    WHEN 3 THEN '房屋'
    WHEN 5 THEN '车位'
    ELSE '房屋'
    END AS resource_type,
    resgroup.resource_code as resource_group,
    res.resource_code,
    res.resource_name,
    res.calc_area,
    CASE res.resource_status
        WHEN 1 THEN '自持'
        WHEN 2 THEN '待售'
        WHEN 3 THEN '已售未收'
        WHEN 4 THEN '已收'
        WHEN 5 THEN '已收未装'
        WHEN 6 THEN '已收装修'
        WHEN 7 THEN '已装未住'
        WHEN 8 THEN '入住'
		WHEN 9 THEN '未售出租'
        ELSE '其他状态'
    END AS resource_status,
    cus.name AS customerName,
    cus.tel AS mobile, 
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cc.business_type,'7','报事业务'),'6','装修业务'),'5','多经业务'),'4','租赁业务'),'3','能耗业务'),'2','车位业务'),'1','基础业务') AS busi_type,
    cc.cost_name AS costName,
    stan.stan_name,
    round(stan.stan_price,2) stan_price,
    stan.latefee_rate,
    f.fee_date,
    f.fee_due_date,
    f.fee_start_date,
    f.fee_end_date,
    CAST(GET_JSON_DOUBLE(f.fee_description, '$.start') AS DECIMAL(18,4)) AS fee_description_start,
    CAST(GET_JSON_DOUBLE(f.fee_description, '$.end') AS DECIMAL(18,4)) AS fee_description_end,
    round(f.calc_amount1,2)  calc_amount1,
    round(f.calc_amount2,2) calc_amount2,
    f.tax_rate,
    d.deal_type,
    d.charge_mode,
    users.name as deal_user,
    d.deal_date,
    usepiao.bill_sign,
    usepiao.change_bill_sign,
    d.deal_memo AS bill_memo,
    flowpay.bank_name,
    flowpay.bank_account,
    flowpay.refund_account_name,
    flowpay.refund_bank,
    flowpay.refund_account,
    IF(d.deal_type IN ('实收','托收确认','预存','代扣','实收红冲','预存红冲'),
       IF(d.deal_type IN ('实收','托收确认','预存','代扣'),
          ABS(IFNULL(d.deal_amount,0)),
          ABS(IFNULL(d.deal_amount,0)) * -1),
       NULL) AS paidAmount,
    IF(d.deal_type IN ('实收','托收确认','预存','代扣','实收红冲','预存红冲'),
       IF(d.deal_type IN ('实收','托收确认','预存','代扣'),
          ABS(IFNULL(d.deal_amount,0)) - ABS(IFNULL(d.deal_notax_amount,0)),
          (ABS(IFNULL(d.deal_amount,0)) - ABS(IFNULL(d.deal_notax_amount,0))) * -1),
       NULL) AS paidTax,
    IF(d.deal_type IN ('实收','托收确认','预存','代扣','实收红冲','预存红冲'),
       IF(d.deal_type IN ('实收','托收确认','预存','代扣'),
          ABS(IFNULL(d.deal_notax_amount,0)),
          ABS(IFNULL(d.deal_notax_amount,0)) * -1),
       NULL) AS paidNotaxAmount,
    IF(d.deal_type IN ('实收','托收确认','预存','代扣','实收红冲','预存红冲'),
       IF(d.deal_type IN ('实收','托收确认','预存','代扣'),
          ABS(IFNULL(d.latefee_amount,0)),
          ABS(IFNULL(d.latefee_amount,0)) * -1),
       NULL) AS latefeeAmount,
    IF(d.deal_type IN ('实收','托收确认','预存','代扣','实收红冲','预存红冲'),
       IF(d.deal_type IN ('实收','托收确认','预存','代扣'),
          ABS(IFNULL(d.latefee_amount,0)) - ABS(IFNULL(d.latefee_notax_amount,0)),
          (ABS(IFNULL(d.latefee_amount,0)) - ABS(IFNULL(d.latefee_notax_amount,0))) * -1),
       NULL) AS latefeeTax,
    IF(d.deal_type IN ('实收','托收确认','预存','代扣','实收红冲','预存红冲'),
       IF(d.deal_type IN ('实收','托收确认','预存','代扣'),
          ABS(IFNULL(d.latefee_notax_amount,0)),
          ABS(IFNULL(d.latefee_notax_amount,0)) * -1),
       NULL) AS latefeeNotaxAmount,
    IF(d.deal_type IN ('退款','预存退款','退款红冲','预存退款红冲'),
       IF(d.deal_type IN ('退款','预存退款'),
          ABS(IFNULL(d.deal_amount,0)),
          ABS(IFNULL(d.deal_amount,0)) * -1),
       NULL) AS refundPaidAmount,
    IF(d.deal_type IN ('退款','预存退款','退款红冲','预存退款红冲'),
       IF(d.deal_type IN ('退款','预存退款'),
          ABS(IFNULL(d.deal_amount,0)) - ABS(IFNULL(d.deal_notax_amount,0)),
          (ABS(IFNULL(d.deal_amount,0)) - ABS(IFNULL(d.deal_notax_amount,0))) * -1),
       NULL) AS refundPaidTaxAmount,
    IF(d.deal_type IN ('退款','预存退款','退款红冲','预存退款红冲'),
       IF(d.deal_type IN ('退款','预存退款'),
          ABS(IFNULL(d.deal_notax_amount,0)),
          ABS(IFNULL(d.deal_notax_amount,0)) * -1),
       NULL) AS refundPaidNotaxAmount,
    IF(d.deal_type IN ('退款','预存退款','退款红冲','预存退款红冲'),
       IF(d.deal_type IN ('退款','预存退款'),
          ABS(IFNULL(d.latefee_amount,0)),
          ABS(IFNULL(d.latefee_amount,0)) * -1),
       NULL) AS refundLatefeeAmount,
    IF(d.deal_type IN ('退款','预存退款','退款红冲','预存退款红冲'),
       IF(d.deal_type IN ('退款','预存退款'),
          ABS(IFNULL(d.latefee_amount,0)) - ABS(IFNULL(d.latefee_notax_amount,0)),
          (ABS(IFNULL(d.latefee_amount,0)) - ABS(IFNULL(d.latefee_notax_amount,0))) * -1),
       NULL) AS refundLatefeeTax,
    IF(d.deal_type IN ('退款','预存退款','退款红冲','预存退款红冲'),
       IF(d.deal_type IN ('退款','预存退款'),
          ABS(IFNULL(d.latefee_notax_amount,0)),
          ABS(IFNULL(d.latefee_notax_amount,0)) * -1),
       NULL) AS refundLatefeeNotaxAmount
FROM tidb_sync_combine.tb_charge_receipts_detail d
LEFT JOIN tidb_sync_combine.tb_charge_fee f ON d.fee_id = f.id AND f.is_delete = 0
LEFT JOIN erp_base.rf_organize o2 ON LOWER(d.comm_id) = LOWER(o2.Id) AND o2.Is_Delete = 0 AND o2.Status = 0
LEFT JOIN erp_base.rf_organize o1 ON LOWER(o2.ParentId) = LOWER(o1.Id) AND o1.Is_Delete = 0 AND o1.Status = 0
LEFT JOIN erp_base.tb_base_masterdata_customer_comm cus ON d.customer_id = cus.id AND cus.is_delete = 0
LEFT JOIN erp_base.tb_base_masterdata_resource res ON d.resource_id = res.id AND res.is_delete = 0
LEFT JOIN erp_base.tb_base_masterdata_resource resgroup on res.resource_group = resgroup.id
LEFT JOIN erp_base.tb_base_charge_cost cc ON d.corp_cost_id = cc.id AND cc.is_delete = 0
LEFT JOIN erp_base.tb_base_charge_comm_stan stan ON d.stan_id = stan.id AND stan.is_delete = 0
LEFT JOIN tidb_sync_combine.tb_base_charge_bill_use usepiao on d.pay_flow_id = usepiao.pay_flow_id
LEFT JOIN tidb_sync_combine.tb_charge_trade_flow_pay flowpay on d.pay_flow_id = flowpay.id
LEFT JOIN erp_base.rf_user users on d.deal_user = users.id
WHERE d.is_delete = 0
'''

args = []

# 添加测试用的固定条件（实际使用时应该移除或修改）
#sql += " AND d.comm_id='0591cfbd-d915-48d6-9831-7f6201cd4d3e' AND d.fee_start_date>='2024-11-01'"

# 时间范围筛选 - 根据查询范围参数决定使用哪个时间字段
if  fee_start_date and fee_end_date:
    # 按费用时间筛选
    sql += " AND f.fee_date BETWEEN %s AND %s"
    args.append(fee_start_date)
    args.append(fee_end_date)
if start_date and end_date:
    # 默认按收退款时间筛选
    sql += " AND d.deal_date BETWEEN %s AND %s"
    args.append(start_date)
    args.append(end_date)

 # 新增：查询范围筛选条件 - 根据deal_type的值过滤
if query_range != "1"  :
    if query_range == "2":
        # 实收相关的deal_type值，这里需要根据实际情况调整
        sql += " AND d.deal_type IN ('实收')"
    elif query_range == "3":
        # 预存相关的deal_type值，这里需要根据实际情况调整
        sql += " AND d.deal_type IN ('预存')"
    elif query_range == "4":
        # 退款相关的deal_type值，这里需要根据实际情况调整
        sql += " AND d.deal_type IN ('退款')"

# 处理项目ID参数

# 确保是列表格式（支持多种输入格式：列表、元组、逗号分隔的字符串）
if isinstance(comm_id, str):
    comm_ids = [x.strip() for x in comm_id.split(',') if x.strip()]
elif isinstance(comm_id, tuple):
    comm_ids = list(comm_id)
elif isinstance(comm_id, list):
    comm_ids = comm_id
else:
    comm_ids = list(comm_id) if comm_id else []

# 清理ID列表中可能存在的引号（生产环境可能传入带引号的值）
comm_ids = [str(x).strip().strip("'").strip('"') for x in comm_ids]

# 构建项目和科目占位符
if comm_ids:
    comm_placeholders = ','.join(['%s'] * len(comm_ids))
    sql += f" AND d.comm_id IN ({comm_placeholders})"
    args.extend(comm_ids)


# 动态条件：项目名称（模糊匹配）
if project_name:
    sql += " AND o2.Name LIKE %s"
    args.append(f"%{project_name}%")

# 动态条件：客户ID
if customer_id:
    sql += " AND d.customer_id = %s"
    args.append(customer_id)

# 动态条件：资源ID
if resource_id:
    sql += " AND d.resource_id = %s"
    args.append(resource_id)

# 处理公司科目ID参数

# 处理公司科目ID参数
if isinstance(corp_cost_id, str):
    corp_cost_ids = [x.strip() for x in corp_cost_id.split(',') if x.strip()]
elif isinstance(corp_cost_id, tuple):
    corp_cost_ids = list(corp_cost_id)
elif isinstance(corp_cost_id, list):
    corp_cost_ids = corp_cost_id
else:
    corp_cost_ids = list(corp_cost_id) if corp_cost_id else []

# 清理ID列表中可能存在的引号
corp_cost_ids = [str(x).strip().strip("'").strip('"') for x in corp_cost_ids]

# 构建科目占位符
if corp_cost_ids:
    cost_placeholders = ','.join(['%s'] * len(corp_cost_ids))
    sql += f" AND d.corp_cost_id IN ({cost_placeholders})"
    args.extend(corp_cost_ids)

# 动态条件：收费科目名称（模糊匹配）
if cost_name:
    sql += " AND cc.cost_name LIKE %s"
    args.append(f"%{cost_name}%")

# 处理动态条件：标准名称
if stan_name:
    if isinstance(stan_name, str):
        stan_names = [name.strip() for name in stan_name.split(',') if name.strip()]
    elif isinstance(stan_name, (list, tuple)):
        stan_names = [str(name).strip() for name in stan_name]
    else:
        stan_names = []
    
    if stan_names:
        placeholder = ','.join(['%s'] * len(stan_names))
        sql += f" AND stan.stan_name IN ({placeholder})"  # 注意：这里应该是stan.stan_name，不是d.stan_name
        args.extend(stan_names)

# 动态条件：收退款方式
if charge_mode:
    if isinstance(charge_mode, str):
        modes = [mode.strip() for mode in charge_mode.split(',') if mode.strip()]
    elif isinstance(charge_mode, (list, tuple)):
        modes = [str(mode).strip() for mode in charge_mode]
    else:
        modes = []
    
    if modes:
        placeholders = ','.join(['%s'] * len(modes))
        sql += f" AND d.charge_mode IN ({placeholders})"
        args.extend(modes)

# 动态条件：收退款人（模糊匹配）
if deal_user:
    sql += " AND users.name LIKE %s"
    args.append(f"%{deal_user}%")

# 动态条件：收款银行
if bank_name:
    sql += " AND flowpay.bank_name LIKE %s"
    args.append(f"%{bank_name}%")

# 动态条件：费用时间范围（补充条件）
if fee_start_date :
    sql += " AND f.fee_date >= %s"
    args.append(fee_start_date)
if fee_end_date :
    sql += " AND f.fee_date <= %s"
    args.append(fee_end_date)

# 动态条件：收退款票号
if bill_sign_start and bill_sign_end:
    sql += " AND usepiao.bill_sign BETWEEN %s AND %s"
    args.append(bill_sign_start)
    args.append(bill_sign_end)
elif bill_sign_start:
    sql += " AND usepiao.bill_sign >= %s"
    args.append(bill_sign_start)
elif bill_sign_end:
    sql += " AND usepiao.bill_sign <= %s"
    args.append(bill_sign_end)

# 动态条件：换票票号
if change_bill_sign_start and change_bill_sign_end:
    sql += " AND usepiao.change_bill_sign BETWEEN %s AND %s"
    args.append(change_bill_sign_start)
    args.append(change_bill_sign_end)
elif change_bill_sign_start:
    sql += " AND usepiao.change_bill_sign >= %s"
    args.append(change_bill_sign_start)
elif change_bill_sign_end:
    sql += " AND usepiao.change_bill_sign <= %s"
    args.append(change_bill_sign_end)

# 动态条件：交付状态（根据实际字段调整）
if delivery_status:
    # 这里需要根据实际表结构调整字段名
    # 假设有一个 delivery_status 字段
    sql += " AND d.delivery_status = %s"
    args.append(delivery_status)

# 默认交易类型
default_types = ['实收','托收确认','预存','退款','预存退款','代扣','实收红冲','预存红冲','预存退款红冲','退款红冲']
placeholders = ','.join(['%s'] * len(default_types))
sql += f" AND d.deal_type IN ({placeholders})"
args.extend(default_types)

# 排序和分页（必须在所有WHERE条件之后）
sql += " ORDER BY d.deal_date DESC, d.id DESC LIMIT %s OFFSET %s"
args.append(page_size)
args.append(offset)

# 执行查询
try:
    dataRows = db_query(sql, tuple(args))
    
    # 调试模式处理
    if debug_mode:
        # 调试信息：打印SQL和参数，方便调试
        print(f"=== SQL执行前调试信息 ===")
        print(f"原始SQL: {sql}")
        print(f"参数列表: {args}")
        print(f"参数数量: {len(args)}")
        print(f"SQL中%s数量: {sql.count('%s')}")
        print(f"=====================")
        
        # 将参数值替换到SQL语句中，生成可直接执行的SQL
        executable_sql = sql
        temp_args = list(args)  # 创建参数列表副本，避免修改原始列表
        
        # 循环替换所有%s占位符
        while "%s" in executable_sql and temp_args:
            arg = temp_args.pop(0)  # 取出第一个参数
            # 根据参数类型添加引号
            if isinstance(arg, str):
                # 处理字符串类型，添加单引号并转义内部单引号
                escaped_arg = arg.replace("'", "''")
                executable_sql = executable_sql.replace("%s", f"'{escaped_arg}'", 1)
            elif isinstance(arg, (int, float)):
                # 数字类型直接替换
                executable_sql = executable_sql.replace("%s", str(arg), 1)
            else:
                # 其他类型转换为字符串后处理
                str_arg = str(arg)
                escaped_arg = str_arg.replace("'", "''")
                executable_sql = executable_sql.replace("%s", f"'{escaped_arg}'", 1)
        
        # 调试信息：打印替换后的SQL
        print(f"=== SQL执行后调试信息 ===")
        print(f"替换后SQL: {executable_sql}")
        print(f"剩余参数: {temp_args}")
        print(f"=====================")
        
        # 调试模式下返回SQL和参数
        debug_message = f"查询成功（调试模式）\nSQL: {executable_sql}\n返回行数: {len(dataRows)}"
        set_result(rows=dataRows, message=debug_message)
    else:
        # 正常模式下只返回结果
        set_result(rows=dataRows, message="查询成功")
except Exception as e:
    # 执行失败时的处理
    print(f"=== SQL执行异常 ===")
    print(f"异常信息: {str(e)}")
    print(f"debug_mode: {debug_mode}")
    print(f"================")
    
    if debug_mode:
        # 调试信息：打印SQL和参数，方便调试
        print(f"=== 异常时SQL和参数信息 ===")
        print(f"原始SQL: {sql}")
        print(f"参数列表: {args}")
        print(f"参数数量: {len(args)}")
        print(f"SQL中%s数量: {sql.count('%s')}")
        print(f"=========================")
        
        # 将参数值替换到SQL语句中，生成可直接执行的SQL
        executable_sql = sql
        temp_args = list(args)  # 创建参数列表副本，避免修改原始列表
        
        # 循环替换所有%s占位符
        while "%s" in executable_sql and temp_args:
            arg = temp_args.pop(0)  # 取出第一个参数
            # 根据参数类型添加引号
            if isinstance(arg, str):
                # 处理字符串类型，添加单引号并转义内部单引号
                escaped_arg = arg.replace("'", "''")
                executable_sql = executable_sql.replace("%s", f"'{escaped_arg}'", 1)
            elif isinstance(arg, (int, float)):
                # 数字类型直接替换
                executable_sql = executable_sql.replace("%s", str(arg), 1)
            else:
                # 其他类型转换为字符串后处理
                str_arg = str(arg)
                escaped_arg = str_arg.replace("'", "''")
                executable_sql = executable_sql.replace("%s", f"'{escaped_arg}'", 1)
        
        # 调试信息：打印替换后的SQL
        print(f"=== 异常时替换后SQL ===")
        print(f"替换后SQL: {executable_sql}")
        print(f"剩余参数: {temp_args}")
        print(f"=====================")
        
        # 调试模式下返回错误信息和完整SQL
        error_message = f"查询失败: {str(e)}\nSQL: {executable_sql}\n异常详情: {repr(e)}"
        set_result(rows=[], message=error_message)
    else:
        # 正常模式下只返回错误信息
        set_result(rows=[], message=f"查询失败: {str(e)}")
    raise