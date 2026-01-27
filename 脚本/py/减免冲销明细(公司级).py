# 分页查询权限表，默认每页100条，默认第1页
page = int(params.get("page", 1))  # 页码，从1开始
page_size = int(params.get("page_size", 100))  # 每页条数，默认100
offset = (page - 1) * page_size  # 计算偏移量

# 获取传入参数
comm_ids = params.get("comm_id", "")
corp_cost_ids = params.get("corp_cost_id", "")
fee_start_date = params.get("fee_start_date", "").strip()
fee_end_date = params.get("fee_end_date", "").strip()
waive_reason = params.get("waive_reason", "").strip()
waive_cancel_start = params.get("waive_cancel_start", "").strip()
waive_cancel_end = params.get("waive_cancel_end", "").strip()
deal_date_start = params.get("deal_date_start", "").strip()  # 开始时间
deal_date_end = params.get("deal_date_end", "").strip()      # 结束时间

# 新增参数
is_cancel = params.get("is_cancel", "").strip()  # 是否撤销，0或1，空值默认为0
cancel_type = params.get("cancel_type", "").strip()  # 撤销类型，'直接撤销'或'红冲撤销'
debug_mode = 1  # 是否开启调试模式，默认为false

# 定义边界时间常量
MIN_DATE = "1999-01-01 00:00:00"
MAX_DATE = "2999-12-31 23:59:59"

# 处理日期格式的兼容性和闰日验证
def normalize_date(date_str, default_time="00:00:00"):
    """将日期字符串标准化为完整的时间格式，并验证日期的合法性"""
    if not date_str:
        return ""
    
    date_str = date_str.strip()
    
    # 如果是空字符串，直接返回空
    if date_str == "":
        return ""
    
    # 如果包含"Invalid date"等无效字符串，直接返回空
    if "Invalid" in date_str or "invalid" in date_str or "null" in date_str:
        return ""
    
    # 如果是边界值，直接返回
    if date_str == MIN_DATE or date_str == MAX_DATE:
        return date_str
    
    # 如果已经是完整的时间格式，验证并返回
    if len(date_str) == 19 and (date_str[10] == ' ' or date_str[10] == 'T'):
        # 处理可能的T分隔符
        if date_str[10] == 'T':
            date_str = date_str.replace('T', ' ')
        
        # 验证日期是否合法
        from datetime import datetime
        try:
            datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            return date_str
        except ValueError:
            # 日期不合法，尝试修正
            try:
                # 尝试解析日期部分
                date_part = date_str[:10]
                time_part = date_str[11:]
                year, month, day = map(int, date_part.split('-'))
                
                # 处理闰日问题：如果日期不合法，调整为月末
                from datetime import datetime, timedelta
                import calendar
                
                # 获取该月的最大天数
                _, max_day = calendar.monthrange(year, month)
                if day > max_day:
                    # 调整到该月的最后一天
                    day = max_day
                    date_part = f"{year:04d}-{month:02d}-{day:02d}"
                    return f"{date_part} {time_part}"
            except:
                # 如果无法修正，返回空字符串
                return ""
    
    # 如果是日期格式，添加默认时间
    if len(date_str) == 10 and date_str[4] == '-':
        try:
            # 验证日期是否合法
            from datetime import datetime
            datetime.strptime(date_str, '%Y-%m-%d')
            return f"{date_str} {default_time}"
        except ValueError:
            # 日期不合法，尝试修正
            try:
                year, month, day = map(int, date_str.split('-'))
                from datetime import datetime, timedelta
                import calendar
                
                # 获取该月的最大天数
                _, max_day = calendar.monthrange(year, month)
                if day > max_day:
                    # 调整到该月的最后一天
                    day = max_day
                    date_str = f"{year:04d}-{month:02d}-{day:02d}"
                return f"{date_str} {default_time}"
            except:
                # 如果无法修正，返回空字符串
                return ""
    
    # 其他格式的日期字符串，尝试解析
    from datetime import datetime
    
    # 尝试常见的时间格式
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%Y/%m/%d %H:%M:%S',
        '%Y/%m/%d %H:%M',
        '%Y/%m/%d',
        '%Y.%m.%d %H:%M:%S',
        '%Y.%m.%d %H:%M',
        '%Y.%m.%d',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # 如果是没有时间的日期格式，添加默认时间
            if fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d']:
                if default_time == "00:00:00":
                    return f"{dt.strftime('%Y-%m-%d')} {default_time}"
                else:
                    return f"{dt.strftime('%Y-%m-%d')} {default_time}"
            else:
                # 返回标准格式
                return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    
    # 所有格式都无法解析，返回空字符串
    return ""

# 标准化日期格式（只在参数不为空时处理）
def safe_normalize_date(date_str, default_time="00:00:00"):
    """安全地标准化日期，防止出现无效日期"""
    if not date_str or date_str.strip() == "":
        return ""
    
    result = normalize_date(date_str, default_time)
    return result if result else ""

# 标准化日期格式
fee_start_date = safe_normalize_date(fee_start_date, "00:00:00")
fee_end_date = safe_normalize_date(fee_end_date, "23:59:59")
deal_date_start = safe_normalize_date(deal_date_start, "00:00:00")
deal_date_end = safe_normalize_date(deal_date_end, "23:59:59")
waive_cancel_start = safe_normalize_date(waive_cancel_start, "00:00:00")
waive_cancel_end = safe_normalize_date(waive_cancel_end, "23:59:59")

# 验证时间范围：费用结束时间不能小于费用开始时间（只在两个参数都有效时才验证）
error_message = ""
if fee_start_date and fee_end_date:
    # 将字符串转换为datetime对象进行比较
    from datetime import datetime
    
    def parse_and_validate_date(date_str, field_name=""):
        """尝试多种格式解析日期，并验证日期合法性"""
        if not date_str:
            return None, ""
        
        date_str = date_str.strip()
        
        # 如果是边界值，直接解析
        if date_str == MIN_DATE or date_str == MAX_DATE:
            try:
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S'), ""
            except:
                return None, f"{field_name}边界值格式不正确"
        
        # 尝试常见的时间格式
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%Y/%m/%d %H:%M:%S',
            '%Y/%m/%d %H:%M',
            '%Y/%m/%d',
            '%Y.%m.%d %H:%M:%S',
            '%Y.%m.%d %H:%M',
            '%Y.%m.%d',
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                
                # 验证日期是否合法（特别是对于2月29日等情况）
                import calendar
                year, month, day = dt.year, dt.month, dt.day
                _, max_day = calendar.monthrange(year, month)
                
                if day > max_day:
                    # 日期不合法，给出提示但继续处理
                    return dt, f"{field_name}日期{date_str}不合法（{year}年{month}月最多有{max_day}天），已自动调整"
                
                return dt, ""
            except ValueError:
                continue
        
        return None, f"{field_name}格式不正确: {date_str}"
    
    start_dt, start_error = parse_and_validate_date(fee_start_date, "费用开始时间")
    end_dt, end_error = parse_and_validate_date(fee_end_date, "费用结束时间")
    
    # 收集所有错误信息
    errors = []
    if start_error:
        errors.append(start_error)
    if end_error:
        errors.append(end_error)
    
    if errors:
        error_message = "；".join(errors)
    elif start_dt and end_dt:
        if end_dt < start_dt:
            error_message = "费用结束时间不能小于费用开始时间"

# 验证deal_date时间范围
if deal_date_start and deal_date_end:
    from datetime import datetime
    
    deal_start_dt, deal_start_error = parse_and_validate_date(deal_date_start, "减免撤销开始时间")
    deal_end_dt, deal_end_error = parse_and_validate_date(deal_date_end, "减免撤销结束时间")
    
    errors = []
    if deal_start_error:
        errors.append(deal_start_error)
    if deal_end_error:
        errors.append(deal_end_error)
    
    if errors:
        if error_message:
            error_message += "；" + "；".join(errors)
        else:
            error_message = "；".join(errors)
    elif deal_start_dt and deal_end_dt:
        if deal_end_dt < deal_start_dt:
            if error_message:
                error_message += "；减免撤销结束时间不能小于开始时间"
            else:
                error_message = "减免撤销结束时间不能小于开始时间"

# 验证is_cancel参数
if is_cancel and is_cancel not in ['0', '1']:
    if error_message:
        error_message += "；is_cancel参数值只能为0或1"
    else:
        error_message = "is_cancel参数值只能为0或1"

# 验证cancel_type参数
if cancel_type and cancel_type not in ['直接撤销', '红冲撤销']:
    if error_message:
        error_message += "；cancel_type参数值只能为'直接撤销'或'红冲撤销'"
    else:
        error_message = "cancel_type参数值只能为'直接撤销'或'红冲撤销'"

# 验证is_cancel和cancel_type的组合逻辑
if is_cancel == '0' and cancel_type:
    if error_message:
        error_message += "；当is_cancel为否时，cancel_type应为空"
    else:
        error_message = "当is_cancel为否时，cancel_type应为空"

# 如果有错误信息，直接返回错误
if error_message:
    set_result(message=error_message, total_count=0)
    # 在脚本执行环境中，不能使用return，直接结束执行
    # 通过判断条件让后续代码不执行
    pass
else:
    # 处理特殊边界值：如果传入的是边界值，则不添加该条件
    def should_add_date_condition(date_value, boundary_value):
        """判断是否需要添加日期条件"""
        if not date_value:
            return False
        
        # 直接比较标准化后的字符串
        return date_value != boundary_value

    # 构建SQL查询
    sql = """
    SELECT
        d.id,
        o1.name as areaName,
        o.Name AS comm_name,
        --r.resource_type,
				
				CASE 
	WHEN r.resource_type=0 THEN
		'房屋区域'
	WHEN r.resource_type=1 THEN
		'房屋楼栋'
		WHEN r.resource_type=2 THEN
		'房屋单元'
		WHEN r.resource_type=3 THEN
		'房屋'
		WHEN r.resource_type=4 THEN
		'车位区域'
		WHEN r.resource_type=5 THEN
		'车位'
END AS resource_type,

				
        r.resource_code,
        r.resource_name,
        r.calc_area,
        --r.resource_status,
				
				CASE 
	WHEN r.resource_status=1 THEN
		'自持'
		WHEN r.resource_status=2 THEN
		'待售'
		WHEN r.resource_status=3 THEN
		'已售未收'
		WHEN r.resource_status=4 THEN
		'已收'
		WHEN r.resource_status=5 THEN
		'已收未装'
		WHEN r.resource_status=6 THEN
		'已收装修'
		WHEN r.resource_status=7 THEN
		'已装未住'
		WHEN r.resource_status=8 THEN
		'入住'
END AS resource_status,

				
        cc.name,
        LOWER(d.corp_cost_id),
        c.cost_name AS cost_name,
        cc.mobile,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(c.business_type,'7','报事业务'),'6','装修业务'),'5','多经业务'),'4','租赁业务'),'3','能耗业务'),'2','车位业务'),'1','基础业务') AS busi_type,
        d.stan_id,
        st.stan_price,
        f.latefee_rate,
        d.fee_date,
        d.fee_due_date,
        d.fee_start_date,
        d.fee_end_date,
        f.calc_amount1,
        f.calc_amount2,
        d.tax_rate,
        f.due_amount,
        ABS(d.deal_amount) AS deal_amount,
        d.deal_date,
        b.bill_sign,
        CASE 
            WHEN d.deal_type = '减免红冲' THEN '红冲撤销'
            WHEN d.is_delete = '1' AND d.deal_type = '减免' THEN '直接撤销'
            ELSE ''
        END AS cancel_type
    FROM tidb_sync_combine.tb_charge_receipts_detail d
    LEFT JOIN tidb_sync_combine.rf_organize o ON LOWER(d.comm_id) = LOWER(o.Id) AND o.OrganType = 6 AND o.Is_Delete = 0 AND o.Status = 0
    LEFT JOIN tidb_sync_combine.rf_organize o1 ON o.ParentId = o1.Id AND o1.Is_Delete = 0 AND o1.Status = 0
    LEFT JOIN tidb_sync_combine.tb_base_masterdata_resource r ON d.resource_id = r.id
    LEFT JOIN tidb_sync_combine.tb_base_charge_cost c ON LOWER(d.corp_cost_id) =  LOWER(c.id) AND c.is_delete = 0
    LEFT JOIN tidb_sync_combine.tb_base_charge_comm_stan st ON d.stan_id = st.id AND st.is_delete = 0
    LEFT JOIN tidb_sync_combine.tb_charge_fee f ON d.fee_id = f.id AND f.is_delete = 0
    LEFT JOIN tidb_sync_combine.tb_base_charge_bill_use b ON d.pay_flow_id = b.pay_flow_id AND b.is_delete = 0  and b.pay_flow_id <>''
    LEFT JOIN tidb_sync_combine.tb_base_masterdata_customer_comm cc ON d.customer_id = cc.id AND cc.is_delete = 0
    WHERE 1=1
    """

    # 构建参数列表
    args = []

    # 根据is_cancel参数设置默认值（空值默认为0）
    if not is_cancel:
        is_cancel = '0'

    # 根据is_cancel和cancel_type参数构建WHERE条件
    if is_cancel == '0':
        # 未撤销：查询d.deal_type='减免'且d.is_delete='0'
        sql += " AND d.deal_type = '减免' AND d.is_delete = '0'"
    elif is_cancel == '1':
        if cancel_type == '直接撤销':
            # 直接撤销：查询d.deal_type='减免'且d.is_delete='1'
            sql += " AND d.deal_type = '减免' AND d.is_delete = '1'"
        elif cancel_type == '红冲撤销':
            # 红冲撤销：查询d.deal_type='减免红冲'（红冲撤销时is_delete可能是0或1）
            sql += " AND d.deal_type = '减免红冲'"
        else:
            # cancel_type为空时，查询所有撤销类型
            sql += " AND ( (d.deal_type = '减免' AND d.is_delete = '1') OR (d.deal_type = '减免红冲') )"
    else:
        # 如果is_cancel为空或无效，默认按未撤销处理
        sql += " AND d.deal_type = '减免' AND d.is_delete = '0'"

    # 可选的ID过滤条件
    if params.get("id"):
        sql += " AND d.id = %s"
        args.append(params["id"])

    # 处理项目ID参数 - 使用comm_id字段
    if comm_ids:
        if isinstance(comm_ids, str):
            comm_ids = [cid.strip() for cid in comm_ids.split(',') if cid.strip()]
        
        if comm_ids:
            placeholders = ','.join(['%s'] * len(comm_ids))
            sql += f" AND LOWER(d.comm_id) IN ({placeholders})"
            args.extend(comm_ids)

    # 处理公司科目ID参数 - 使用corp_cost_id字段
    if corp_cost_ids:
        if isinstance(corp_cost_ids, str):
            corp_cost_ids = [cid.strip() for cid in corp_cost_ids.split(',') if cid.strip()]
        
        if corp_cost_ids:
            placeholders = ','.join(['%s'] * len(corp_cost_ids))
            sql += f" AND LOWER(d.corp_cost_id) IN ({placeholders})"
            args.extend(corp_cost_ids)

    # 处理费用开始时间参数
    # 只有当传入的不是空字符串且不是最小边界值时才添加条件
    if fee_start_date and should_add_date_condition(fee_start_date, MIN_DATE):
        sql += " AND d.fee_start_date >= %s"
        args.append(fee_start_date)

    # 处理费用结束时间参数
    # 只有当传入的不是空字符串且不是最大边界值时才添加条件
    if fee_end_date and should_add_date_condition(fee_end_date, MAX_DATE):
        sql += " AND d.fee_end_date <= %s"
        args.append(fee_end_date)

    # 处理减免原因参数
    if waive_reason:
        sql += " AND d.deal_memo LIKE %s"
        args.append(f"%{waive_reason}%")

    # 处理减免冲销开始时间参数
    if waive_cancel_start:
        sql += " AND d.deal_date >= %s"
        args.append(waive_cancel_start)

    # 处理减免冲销结束时间参数
    if waive_cancel_end:
        sql += " AND d.deal_date <= %s"
        args.append(waive_cancel_end)

    # 处理减免撤销时间参数 - 改为范围查询
    # 处理deal_date开始时间
    if deal_date_start:
        # 只有当传入的不是空字符串且不是最小边界值时才添加条件
        if should_add_date_condition(deal_date_start, MIN_DATE):
            sql += " AND d.deal_date >= %s"
            args.append(deal_date_start)

    # 处理deal_date结束时间
    if deal_date_end:
        # 只有当传入的不是空字符串且不是最大边界值时才添加条件
        if should_add_date_condition(deal_date_end, MAX_DATE):
            sql += " AND d.deal_date <= %s"
            args.append(deal_date_end)

    # 先执行COUNT查询获取总条数(在添加分页参数前,使用相同的WHERE条件和参数)
    count_sql = """
    SELECT COUNT(1) as total_count
    FROM tidb_sync_combine.tb_charge_receipts_detail d
    LEFT JOIN tidb_sync_combine.rf_organize o ON LOWER(d.comm_id) = LOWER(o.Id) AND o.OrganType = 6 AND o.Is_Delete = 0 AND o.Status = 0
    LEFT JOIN tidb_sync_combine.rf_organize o1 ON o.ParentId = o1.Id AND o1.Is_Delete = 0 AND o1.Status = 0
    LEFT JOIN tidb_sync_combine.tb_base_masterdata_resource r ON d.resource_id = r.id
    LEFT JOIN tidb_sync_combine.tb_base_charge_cost c ON LOWER(d.corp_cost_id) =  LOWER(c.id) AND c.is_delete = 0
    LEFT JOIN tidb_sync_combine.tb_base_charge_comm_stan st ON d.stan_id = st.id AND st.is_delete = 0
    LEFT JOIN tidb_sync_combine.tb_charge_fee f ON d.fee_id = f.id AND f.is_delete = 0
    LEFT JOIN tidb_sync_combine.tb_base_charge_bill_use b ON d.pay_flow_id = b.pay_flow_id AND b.is_delete = 0  and b.pay_flow_id <>''
    LEFT JOIN tidb_sync_combine.tb_base_masterdata_customer_comm cc ON d.customer_id = cc.id AND cc.is_delete = 0
    WHERE 1=1
    """ + sql.split("WHERE 1=1")[1]  # 复用WHERE条件

    # 执行COUNT查询(注意:此时args中还没有分页参数)
    try:
        count_result = db_query(count_sql, tuple(args))
        total_count = count_result[0]['total_count'] if count_result else 0
    except Exception as e:
        print(f"COUNT查询失败: {str(e)}")
        total_count = 0

    # 排序和分页（必须在COUNT查询之后添加）
    # 修改排序：按照r.resource_code、cc.name、c.cost_name、d.fee_date进行升序排序
    sql += " ORDER BY r.resource_code ASC, cc.name ASC, c.cost_name ASC, d.fee_date ASC, d.is_delete DESC LIMIT %s OFFSET %s"
    args.append(page_size)
    args.append(offset)

    # 执行查询
    try:
        # 执行查询
        dataRows = db_query(sql, tuple(args))

        # 根据调试模式返回不同结果
        if debug_mode:
            # 调试模式下将SQL和参数添加到message中返回
            debug_message = f"查询成功（调试模式）\nSQL: {sql}\nParams: {args}\n总记录数: {total_count}"
            set_result(rows=dataRows, message=debug_message, total_count=total_count)
        else:
            # 正常模式下只返回结果
            set_result(rows=dataRows, message=f"查询成功，共{total_count}条数据，当前第{page}页", total_count=total_count)
    except Exception as e:
        # 异常情况下也将SQL和参数添加到message中返回（如果处于调试模式）
        if debug_mode:
            error_message = f"查询失败: {str(e)}\nSQL: {sql}\nParams: {args}"
            set_result(rows=[], message=error_message, total_count=0)
        else:
            set_result(message=f"查询失败: {str(e)}", total_count=0)
        raise