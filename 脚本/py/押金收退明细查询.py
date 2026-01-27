# 基础 SQL 语句（主体部分）
base_sql = """
SELECT
    cc.name as Parent_name,
    c.`Name` AS comm_name,
    b.cost_name,
    CASE f.resource_attr
        WHEN 1 THEN '房屋'
        WHEN 2 THEN '车位'
    END AS resource_attr_name,
    CONCAT_WS('-', tttt.resource_name, ttt.resource_name, tt.resource_name) AS group_resource_nam, 
    f.resource_code,
    f.resource_name,
    f.calc_area,
    CASE f.resource_status 
        WHEN 1 THEN '自持'
        WHEN 2 THEN '待售'
        WHEN 3 THEN '已售未收'
        WHEN 4 THEN '已收'
        WHEN 5 THEN '已收未装'
        WHEN 6 THEN '已收装修'
        WHEN 7 THEN '已装未住'
        WHEN 8 THEN '入住'
        ELSE '其他状态'
    END AS resource_status_name,
    g.`name` AS customer_name,
    g.mobile,
    case
        h.busi_type
            WHEN 1 THEN '基础业务'
            WHEN 2 THEN '车位业务'
            WHEN 3 THEN '能耗业务'
            WHEN 4 THEN '租赁业务'
            WHEN 5 THEN '多经业务'
            WHEN 6 THEN '装修业务'
            WHEN 7 THEN '报事业务'
            WHEN 8 THEN '多经业务'
            ELSE '基础业务'
    end as busi_type,
    h.fee_date,
    h.fee_due_date,
    h.fee_start_date,
    h.fee_end_date,
    h.tax_rate,
    (SUM(CASE
        a.deal_type 
        WHEN '实收' THEN ABS(IFNULL(a.deal_amount,0)) 
        WHEN '托收确认' THEN ABS(IFNULL(a.deal_amount,0)) 
        WHEN '代扣' THEN ABS(IFNULL(a.deal_amount,0)) 
        WHEN '预存冲抵' THEN ABS(IFNULL(a.deal_amount,0)) 
        ELSE 0 
    END) - IFNULL(j.实收红冲,0) - IFNULL(j.预存冲抵红冲,0)) AS payment_offset_amount,
    GROUP_CONCAT(DISTINCT i.charge_mode) AS charge_mode,
    GROUP_CONCAT(DISTINCT n.name) AS pay_user,
    GROUP_CONCAT(DISTINCT i.pay_date) AS pay_date,
    GROUP_CONCAT(DISTINCT a.deal_type) AS deal_type,
    GROUP_CONCAT(DISTINCT a.deal_date) AS deal_date,
    GROUP_CONCAT(DISTINCT m.bill_sign) AS bill_sign,
    (IFNULL(j.退款,0) - IFNULL(j.退款红冲,0)) AS refund_amount,
    j.refund_bill_sign,
    j.refund_date,
    ((SUM(CASE
        a.deal_type 
        WHEN '实收' THEN ABS(IFNULL(a.deal_amount,0)) 
        WHEN '托收确认' THEN ABS(IFNULL(a.deal_amount,0)) 
        WHEN '代扣' THEN ABS(IFNULL(a.deal_amount,0)) 
        WHEN '预存冲抵' THEN ABS(IFNULL(a.deal_amount,0)) 
        ELSE 0 
    END) - IFNULL(j.实收红冲,0) - IFNULL(j.预存冲抵红冲,0)) - (IFNULL(j.退款,0) - IFNULL(j.退款红冲,0))) AS unrefunded_amount 
FROM tidb_sync_combine.tb_charge_receipts_detail a
LEFT JOIN tidb_sync_combine.tb_charge_cost b ON a.cost_id = b.id 
LEFT JOIN erp_base.rf_organize c ON a.comm_id = c.Id 
LEFT JOIN erp_base.tb_base_masterdata_resource f ON a.resource_id = f.id AND a.comm_id = f.comm_id 
LEFT JOIN erp_base.tb_base_masterdata_customer_comm g ON a.customer_id = g.id AND a.comm_id = g.comm_id 
LEFT JOIN tidb_sync_combine.tb_charge_fee h ON a.fee_id = h.id AND a.comm_id = h.comm_id 
LEFT JOIN tidb_sync_combine.tb_charge_trade_flow_pay i ON a.pay_flow_id = i.id
LEFT JOIN erp_base.rf_organize cc ON c.ParentId = cc.Id
LEFT JOIN erp_base.tb_base_masterdata_resource tt ON ifnull(f.parent_id,'') = tt.id  -- 一级资源组
LEFT JOIN erp_base.tb_base_masterdata_resource ttt ON ifnull(tt.parent_id,'') = ttt.id  -- 二级资源组
LEFT JOIN erp_base.tb_base_masterdata_resource tttt ON ifnull(ttt.parent_id,'') = tttt.id  -- 二级资源组
LEFT JOIN (
    SELECT
        a.fee_id,
        SUM(IF(a.deal_type='实收红冲',ABS(IFNULL(a.deal_amount,0)),0)) AS 实收红冲,
        SUM(IF(a.deal_type='预存冲抵红冲',ABS(IFNULL(a.deal_amount,0)),0)) AS 预存冲抵红冲,
        SUM(IF(a.deal_type='退款' AND (a.deal_date = '' OR a.deal_date IS NULL),ABS(IFNULL(a.deal_amount,0)),0)) AS 退款,
        SUM(IF(a.deal_type='退款红冲' AND (a.deal_date = '' OR a.deal_date IS NULL),ABS(IFNULL(a.deal_amount,0)),0)) AS 退款红冲,
        GROUP_CONCAT(DISTINCT IF(a.deal_type = '退款' AND (a.deal_date = '' OR a.deal_date IS NULL), m.bill_sign, NULL)) AS refund_bill_sign,
        MAX(IF(a.deal_type = '退款' AND (a.deal_date = '' OR a.deal_date IS NULL), a.deal_date, NULL)) AS refund_date
    FROM tidb_sync_combine.tb_charge_receipts_detail a
    LEFT JOIN tidb_sync_combine.tb_charge_cost b ON a.cost_id = b.id 
    LEFT JOIN erp_base.tb_base_masterdata_resource f ON a.resource_id = f.id AND a.comm_id = f.comm_id 
    LEFT JOIN erp_base.tb_base_masterdata_customer_comm g ON a.customer_id = g.id AND a.comm_id = g.comm_id 
    LEFT JOIN tidb_sync_combine.tb_base_charge_bill_use m ON a.pay_flow_id = m.pay_flow_id
    WHERE
        a.is_delete = 0 
        AND a.deal_type IN ('实收红冲', '预存冲抵红冲', '退款', '退款红冲') 
        AND b.cost_type IN (15, 16) 
    GROUP BY a.fee_id
) j ON a.fee_id = j.fee_id
LEFT JOIN tidb_sync_combine.tb_base_charge_bill_relation_trade k ON a.pay_flow_id = k.pay_flow_id
LEFT JOIN tidb_sync_combine.tb_base_charge_bill_use m ON k.bill_use_id = m.id AND m.bill_use_case = 1 
LEFT JOIN erp_base.rf_user n ON f.house_keeper = n.Id
WHERE
    a.is_delete = 0 
    AND a.deal_type IN ('实收', '托收确认', '代扣', '预存冲抵') 
    AND b.cost_type IN (15, 16) 
"""

# 初始化条件列表和参数列表
where_conditions = []
args = []

# 分页参数处理（StarRocks 支持 LIMIT offset, size 或 LIMIT size OFFSET offset）
page = int(params.get("page", 1))  # 页码，从1开始
page_size = int(params.get("page_size", 100))  # 每页条数，默认100
# 校验分页参数合法性，避免负数/0值
page = 1 if page < 1 else page
page_size = 100 if page_size < 1 else page_size
offset = (page - 1) * page_size  # 计算偏移量

# 1. 处理 comm_id (IN 查询)
comm_id = params.get('comm_id')
if comm_id:
    # 兼容单个值或多个值（以逗号分隔）
    comm_ids = comm_id.split(',') if isinstance(comm_id, str) else comm_id
    # 生成 IN 条件的占位符（%s）
    placeholders = ', '.join(['%s'] * len(comm_ids))
    where_conditions.append(f"a.comm_id IN ({placeholders})")
    args.extend(comm_ids)

# 2. 处理 resource_attr_name (精准查询)
resource_attr = params.get('resource_attr')
if resource_attr:
    try:
        # 转换为整数并校验有效值
        attr_val = int(resource_attr)
        if attr_val not in (1, 2):
            raise Exception("resource_attr 有效值为 1（房屋）或 2（车位）")
        where_conditions.append("f.resource_attr = %s")
        args.append(attr_val)
    except ValueError:
        raise Exception("resource_attr 必须是数字（1 或 2）")

# 3. 处理 resource_class (IN 查询)
resource_class = params.get('resource_class')
if resource_class:
    resource_classes = resource_class.split(',') if isinstance(resource_class, str) else resource_class
    placeholders = ', '.join(['%s'] * len(resource_classes))
    where_conditions.append(f"f.resource_class IN ({placeholders})")
    args.extend(resource_classes)

# 4. 处理 resource_status (IN 查询)
resource_status = params.get('resource_status')
if resource_status:
    resource_statuses = resource_status.split(',') if isinstance(resource_status, str) else resource_status
    placeholders = ', '.join(['%s'] * len(resource_statuses))
    where_conditions.append(f"f.resource_status IN ({placeholders})")
    args.extend(resource_statuses)

# 5. 处理 corp_cost_id (IN 查询)
corp_cost_id = params.get('corp_cost_id')
if corp_cost_id:
    corp_cost_ids = corp_cost_id.split(',') if isinstance(corp_cost_id, str) else corp_cost_id
    placeholders = ', '.join(['%s'] * len(corp_cost_ids))
    where_conditions.append(f"b.corp_cost_id IN ({placeholders})")
    args.extend(corp_cost_ids)

# 6. 处理 deal_date (范围查询，格式需为 'yyyy-mm-dd'，支持 start 和 end 拼接)
deal_date = params.get('deal_date')
if deal_date:
    # 假设 deal_date 传入格式为 'start_date,end_date'（如 '2025-01-01,2025-12-31'）
    deal_date_range = deal_date.split(',')
    if len(deal_date_range) == 2:
        where_conditions.append("a.deal_date BETWEEN %s AND %s")
        args.extend(deal_date_range)

# 7. 处理 refund_date (小于等于查询，时间格式：yyyy-mm-dd 或 yyyy-mm-dd HH:MM:SS)
refund_date = params.get('refund_date')
if refund_date:
    # 步骤1：校验时间格式合法性（支持 yyyy-mm-dd 或 yyyy-mm-dd HH:MM:SS）
    import re
    date_pattern = r'^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$'
    if not re.match(date_pattern, refund_date):
        raise ValueError(f"refund_date 格式错误，需为 'yyyy-mm-dd' 或 'yyyy-mm-dd HH:MM:SS'，当前值：{refund_date}")
    
    # 步骤2：处理 NULL 值（保留无退款的记录 + 退款日期<=指定值的记录）
    # 若仅需过滤有退款且日期<=指定值的记录，可去掉 OR j.refund_date IS NULL
    where_conditions.append("(j.refund_date IS NULL OR j.refund_date <= %s)")
    args.append(refund_date)

# ========== 修复：重新构建总条数查询 SQL ==========
# 方法优化：不拆分原SQL，直接手动构建COUNT查询（更稳定）
count_sql = """
SELECT COUNT(DISTINCT a.fee_id) AS total_count 
FROM tidb_sync_combine.tb_charge_receipts_detail a
LEFT JOIN tidb_sync_combine.tb_charge_cost b ON a.cost_id = b.id 
LEFT JOIN erp_base.rf_organize c ON a.comm_id = c.Id 
LEFT JOIN erp_base.tb_base_masterdata_resource f ON a.resource_id = f.id AND a.comm_id = f.comm_id 
LEFT JOIN erp_base.tb_base_masterdata_customer_comm g ON a.customer_id = g.id AND a.comm_id = g.comm_id 
LEFT JOIN tidb_sync_combine.tb_charge_fee h ON a.fee_id = h.id AND a.comm_id = h.comm_id 
LEFT JOIN tidb_sync_combine.tb_charge_trade_flow_pay i ON a.pay_flow_id = i.id
LEFT JOIN erp_base.rf_organize cc ON c.ParentId = cc.Id
LEFT JOIN (
    SELECT
        a.fee_id,
        SUM(IF(a.deal_type='实收红冲',ABS(IFNULL(a.deal_amount,0)),0)) AS 实收红冲,
        SUM(IF(a.deal_type='预存冲抵红冲',ABS(IFNULL(a.deal_amount,0)),0)) AS 预存冲抵红冲,
        SUM(IF(a.deal_type='退款' AND (a.deal_date = '' OR a.deal_date IS NULL),ABS(IFNULL(a.deal_amount,0)),0)) AS 退款,
        SUM(IF(a.deal_type='退款红冲' AND (a.deal_date = '' OR a.deal_date IS NULL),ABS(IFNULL(a.deal_amount,0)),0)) AS 退款红冲,
        GROUP_CONCAT(DISTINCT IF(a.deal_type = '退款' AND (a.deal_date = '' OR a.deal_date IS NULL), m.bill_sign, NULL)) AS refund_bill_sign,
        MAX(IF(a.deal_type = '退款' AND (a.deal_date = '' OR a.deal_date IS NULL), a.deal_date, NULL)) AS refund_date
    FROM tidb_sync_combine.tb_charge_receipts_detail a
    LEFT JOIN tidb_sync_combine.tb_charge_cost b ON a.cost_id = b.id 
    LEFT JOIN erp_base.tb_base_masterdata_resource f ON a.resource_id = f.id AND a.comm_id = f.comm_id 
    LEFT JOIN erp_base.tb_base_masterdata_customer_comm g ON a.customer_id = g.id AND a.comm_id = g.comm_id 
    LEFT JOIN tidb_sync_combine.tb_base_charge_bill_use m ON a.pay_flow_id = m.pay_flow_id
    WHERE
        a.is_delete = 0 
        AND a.deal_type IN ('实收红冲', '预存冲抵红冲', '退款', '退款红冲') 
        AND b.cost_type IN (15, 16) 
    GROUP BY a.fee_id
) j ON a.fee_id = j.fee_id
LEFT JOIN tidb_sync_combine.tb_base_charge_bill_relation_trade k ON a.pay_flow_id = k.pay_flow_id
LEFT JOIN tidb_sync_combine.tb_base_charge_bill_use m ON k.bill_use_id = m.id AND m.bill_use_case = 1 
LEFT JOIN erp_base.rf_user n ON f.house_keeper = n.Id
WHERE
    a.is_delete = 0 
    AND a.deal_type IN ('实收', '托收确认', '代扣', '预存冲抵') 
    AND b.cost_type IN (15, 16) 
"""
# 拼接自定义WHERE条件（避免重复加AND）
if where_conditions:
    count_sql += " AND " + " AND ".join(where_conditions)
# 结尾不加多余分号，避免语法错误
count_sql = count_sql.strip()

# ========== 原分页查询SQL拼接 ==========
# 拼接 WHERE 条件
if where_conditions:
    base_sql += " AND " + " AND ".join(where_conditions)

# 补充 GROUP BY 和 ORDER BY（移除原末尾的分号，为分页留位置）
base_sql += """
GROUP BY
    a.fee_id,
    cc.name,
    c.`Name`,
    b.cost_name,
    f.resource_attr,
    f.resource_group,
    f.resource_code,
    f.resource_name,
    f.calc_area,
    f.resource_status,
    g.`name`,
    g.mobile,
    h.busi_type,
    h.fee_date,
    h.fee_due_date,
    h.fee_start_date,
    h.fee_end_date,
    h.tax_rate,
    j.实收红冲,
    j.预存冲抵红冲,
    j.退款,
    j.退款红冲,
    j.refund_bill_sign,
    j.refund_date,
    tttt.resource_name,
    ttt.resource_name,
    tt.resource_name
ORDER BY h.fee_date ASC,a.fee_id ASC
"""

# 拼接分页逻辑（StarRocks 推荐使用 LIMIT offset, size 格式，更兼容）
base_sql += f"LIMIT {offset}, {page_size}"  # 移除分号，避免重复

try:
    # ========== 执行总条数查询 ==========
    count_result = db_query(count_sql, tuple(args))
    total_count = count_result[0]['total_count'] if count_result and len(count_result) > 0 else 0  # 增加空值判断
    
    # 执行原分页查询
    rows = db_query(base_sql, tuple(args))
    
    # 返回结果（包含总条数、当前页数据、分页信息）
    set_result(
        rows=rows, 
        total_count=total_count,  # 新增总条数字段
        message=f"查询成功，共{total_count}条数据，当前第{page}页，每页{page_size}条"
    )
except Exception as e:
    # 捕获异常并返回错误信息
    set_result(
        rows=None, 
        total_count=0,  # 异常时总条数置0
        message=f"查询失败：{str(e)}"
    ) 