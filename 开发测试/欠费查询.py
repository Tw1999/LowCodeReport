import datetime

# 解析入参（实际环境中由外部传入）
params = {}
comm_ids = params.get("comm_ids", "").split(",") if params.get("comm_ids") else []
corp_cost_ids = params.get("corp_cost_ids", "").split(",") if params.get("corp_cost_ids") else []
start_date = params.get("start_date", "")
end_date = params.get("end_date", "")
resource_status =  params.get("resource_status", "").split(",") if params.get("resource_status") else []
jzsj = params.get("jzsj", "")  # 截止时间
debug = str(params.get("debug", "0")).strip()

comm_ids = "0591cfbd-d915-48d6-9831-7f6201cd4d3e,0d8400d6-d728-4f72-9e6c-08bb9bf5828d,21bea8c6-46e8-4d5f-9ead-cc1ff8e3b90e,48b34bc1-4274-4be5-afac-3f252eda05d8,4eb68e2f-d1f4-486d-b7bf-d0403f8a76e7,73b796a2-74a9-4406-af1c-15ee0218e82e,ac3ece62-af2d-45b7-b644-7a60961f8abd,e1c078af-62ac-4923-9267-7f99d5dffb6a,e236f7bb-626a-4288-83bc-405d4a44545c,e5c4dfcb-e1db-443a-ab47-bf18eb48a478".split(",")
corp_cost_ids = "07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a,09fccd77-0878-4e25-a817-0fb03c305dc1,15d56143-bd27-4ccd-898d-991081694fb3,1904e02c-17d4-4796-a9eb-9eb3ed5dfe2f,28f767a4-b1d6-45fd-92d5-b7668a62d1e9,2a536b39-f4a8-4f13-af61-ad63f56989be,2bb34b1c-b7f4-438d-968d-b56a1de7be64,3d55cb31-2cee-47c6-9883-615b53167d88,40709b88-ce74-48a3-a60b-45758d2efe0e,42605ba4-83f0-4de8-a742-4e64c1081566,4416974b-35ea-4f67-b655-6593cf57b10b,479e2925-a00e-4f62-b35e-cd5ed0405381,4dcaa913-d588-4f30-b03b-11c21810f0d2,5bc4c724-ecc3-4ba3-8ea7-038154f55a13,5ef3f6e7-ec98-4dde-8321-94ba79b5d22c,631eeeaf-0464-4bb2-b1f3-4f0096b11309,6c8912d4-8a96-4290-af65-3807db983925,6cf57fd4-f314-4038-99bb-3af473d0efb5,6dcedf04-1413-4449-8528-e8f017328c68,7e08fcc5-7ed8-4742-9e9e-ef3fa634a875,85580050-8858-4111-9e96-57d825ce0ffc,8c0ab63d-6023-4c89-b4f7-ab886338c033,8c9b4770-51b4-4b25-bfb5-7df38de85ebb,9002c943-2f8f-4f7f-b8f6-bdf818c12586,9c6e5462-e639-4eb7-80e8-b518edeb54ff,a32342c8-94e5-40c1-9e72-903e111aa7e5,a7c26688-f20a-11ec-9bda-00163e03b5f6,a7c2675b-f20a-11ec-9bda-00163e03b5f6,a7c26813-f20a-11ec-9bda-00163e03b5f6,a7c2689b-f20a-11ec-9bda-00163e03b5f6,a7c268c0-f20a-11ec-9bda-00163e03b5f6,a7c26a89-f20a-11ec-9bda-00163e03b5f6,a7c26bca-f20a-11ec-9bda-00163e03b5f6,a7c26bec-f20a-11ec-9bda-00163e03b5f6,a9ced350-60ce-4cf7-8c65-56745bd5b4a7,b8b235bd-3cb0-433d-9256-92b814811245,b9e595b4-004b-4b4b-8582-5bff131fb62d,c0bb1bc6-0336-4db4-be5f-a78c6fe20973,cc9ecb22-0206-4fd1-913f-04a848c42dd1,d754d22f-52c6-4e5b-81d3-7f6d4d7804e6,d93bcc98-6a3d-4603-8285-9d95a64b02ed,e70d1e38-aee1-4b37-9919-8a2d51e0dc4f,e7a382ec-6da7-42e5-80d0-876260ddcc14,ee153b99-0bfb-44e3-9e57-d85a04454a51,f6fd5e82-72ed-4052-a88c-8aafbd744bf6".split(",")
start_date = "2000-01-01"
end_date = "2025-12-31"
resource_status  = "1,2,3,4,5,6,7,8".split(",") 
jzsj = "2025-12-31"
debug = "1"

print("到了")
# 日期处理函数（仅使用标准库）
def parse_date(date_str):
    """解析日期字符串为datetime对象，仅使用datetime标准库"""
    if not date_str:
        return datetime.datetime.now()
    try:
        if ' ' in date_str:
            return datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        else:
            # 如果没有具体时间，默认设置为当天的23:59:59
            return datetime.datetime.strptime(date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
    except (ValueError, TypeError):
        return datetime.datetime.now()

# 处理日期参数
start_date = parse_date(start_date)
end_date = parse_date(end_date) 
jzsj = parse_date(end_date).strftime('%Y-%m-%d %H:%M:%S') 

comm_ids_condition = ""
# 处理项目ID参数 
if comm_ids:
    comm_ids_str = "', '".join([cid.strip() for cid in comm_ids if cid.strip()]) 
    comm_ids_condition = f"AND f.comm_id IN ('{comm_ids_str}')"
# 处理科目ID参数
corp_cost_ids_condition = ""
if corp_cost_ids:
    corp_cost_ids_str = "', '".join([cid.strip() for cid in corp_cost_ids if cid.strip()])
    corp_cost_ids_condition = f"AND f.corp_cost_id IN ('{corp_cost_ids_str}')"
     
# 处理资源状态ID参数
resource_status_condition = ""
if resource_status:
    resource_status_str = "', '".join([cid.strip() for cid in resource_status if cid.strip()])
    resource_status_condition = f"AND f.resource_status IN ('{resource_status_str}')"
 

# -------------------------- 第一步：查询有金额的年度 --------------------------
detect_sql = f"""
SELECT DISTINCT
    DATE_FORMAT(f.fee_date, '%%Y') AS debtsyear
FROM tidb_sync_combine.tb_charge_fee f
LEFT JOIN tidb_sync_combine.tb_charge_fee_his his ON his.id = f.id
    AND IFNULL(his.is_delete, 0) = 0
    AND his.due_amount < 0
LEFT JOIN tidb_sync_combine.tb_charge_cost cost ON f.cost_id = cost.id
LEFT JOIN (
    SELECT
        fee_id,
        SUM(IFNULL(deal_amount, 0)) AS deal_amount
    FROM tidb_sync_combine.tb_charge_receipts_detail f
    WHERE 1=1
        AND deal_type IN ('实收', '代扣', '托收确认', '实收红冲', '预存冲抵', '预存冲抵红冲')
        AND is_delete = 0
        {comm_ids_condition}
        AND f.fee_date BETWEEN %s AND %s AND f.deal_date <= %s GROUP BY fee_id
) sk ON f.id = sk.fee_id
WHERE 1=1
    {comm_ids_condition}
    AND f.fee_date BETWEEN %s AND %s
    AND (IFNULL(f.due_amount, 0) + IFNULL(his.due_amount, 0) + IFNULL(sk.deal_amount, 0)) <> 0
    AND cost.cost_name IS NOT NULL
ORDER BY debtsyear DESC;
"""

sub_query_params=[]
# 补充子查询的日期参数到sub_query_params start_date_sql, end_date_sql, jzsj_sql,start_date_sql, end_date_sql
sub_query_params.append(start_date) 
sub_query_params.append(end_date) 
sub_query_params.append(jzsj) 
sub_query_params.append(start_date) 
sub_query_params.append(end_date) 
debtsyearstr=""
case_clauses_str = ""

def format_sql(sql_template, args):
    """简单参数替换，便于调试展示最终SQL"""
    formatted_sql = sql_template
    # 把 f-string 中为转义留下的 %% 先还原成占位符，再补齐
    formatted_sql = formatted_sql.replace("%%", "%")
    for arg in args:
        if isinstance(arg, datetime.datetime):
            val = arg.strftime('%Y-%m-%d %H:%M:%S')
        else:
            val = arg
        if arg is None:
            replacement = "NULL"
        elif isinstance(val, (int, float)):
            replacement = str(val)
        else:
            replacement = f"'{val}'"
        formatted_sql = formatted_sql.replace("%s", replacement, 1)
    return formatted_sql
 
try:
    # 执行查询获取有金额的年度
    detect_rows = db_query(detect_sql, tuple(sub_query_params))
    year_count = len(detect_rows)
    print(year_count)
    
    # -------------------------- 动态构建年度列的CASE WHEN子句 --------------------------
    case_clauses = []
    
    for i in range(year_count):
        row_tuple = detect_rows[i]  # 得到('2025',)这样的元组
        debtsyear = row_tuple[0]    # 提取元组的第一个元素，得到'2025'
        case_clauses.append(f"SUM(CASE WHEN DATE_FORMAT(f.fee_date, '%%Y') = {debtsyear} THEN (IFNULL(f.due_amount, 0) + IFNULL(his.due_amount, 0) + IFNULL(sk.deal_amount, 0)) ELSE 0 END) AS `{debtsyear}年`")
    # 兜底：无年度时显示0
    if not case_clauses:
        case_clauses.append("SUM(0) AS `未知年度`")

    # print(case_clauses)
    # 拼接CASE WHEN子句
    case_clauses_str = ",\n    ".join(case_clauses)
    # print(f"\n共发现 {year_count} 个有金额的年度")
    # print("动态列语句：", case_clauses_str)

    # -------------------------- 第二步：构建主SQL（年度行转列） --------------------------
    main_sql = f"""
SELECT
    org.`Name` AS area_name,
    comm.NAME AS comm_name,
    f.comm_id,
    cost.cost_name,
    SUM(IFNULL(f.due_amount, 0) + IFNULL(his.due_amount, 0) + IFNULL(sk.deal_amount, 0)) AS total_amount,
    {case_clauses_str}
FROM tidb_sync_combine.tb_charge_fee f
LEFT JOIN tidb_sync_combine.rf_organize comm ON comm.id = f.comm_id
LEFT JOIN tidb_sync_combine.rf_organize org ON comm.ParentId = org.id
LEFT JOIN tidb_sync_combine.tb_charge_cost cost ON f.cost_id = cost.id
LEFT JOIN tidb_sync_combine.tb_charge_fee_his his ON his.id = f.id
    AND IFNULL(his.is_delete, 0) = 0
    AND his.due_amount < 0
LEFT JOIN (
    SELECT
        fee_id,
        SUM(IFNULL(deal_amount, 0)) AS deal_amount
    FROM tidb_sync_combine.tb_charge_receipts_detail f
    WHERE 1=1
        AND deal_type IN ('实收', '代扣', '托收确认', '实收红冲', '预存冲抵')
        AND is_delete = 0
        {comm_ids_condition}
        AND f.fee_date BETWEEN %s AND %s AND f.deal_date <= %s GROUP BY fee_id
) sk ON f.id = sk.fee_id
WHERE 1=1
    {comm_ids_condition} AND f.fee_date BETWEEN %s AND %s
    AND cost.cost_name IS NOT NULL
GROUP BY
    org.id, org.`Name`,
    comm.id, comm.NAME,
    f.comm_id,
    cost.cost_name
ORDER BY
    org.id,
    f.comm_id;
"""

    # 整理主查询的参数列表：主查询参数 + CASE WHEN的年度参数 + 子查询日期参数
    main_params = []
    main_params.append(start_date) 
    main_params.append(end_date) 
    main_params.append(jzsj) 
    main_params.append(start_date) 
    main_params.append(end_date) 
   

    # 执行主查询
    data_rows = db_query(main_sql, tuple(main_params))
    if debug == "1":
        formatted_main_sql = format_sql(main_sql, main_params)
        formatted_main_sql = " ".join(formatted_main_sql.split())
        set_result(rows=[{"debug_sql": formatted_main_sql}], message=f"查询成功，共显示{year_count}个年度")
    else:
        set_result(rows=data_rows, message=f"查询成功，共显示{year_count}个年度")

except Exception as e:
    set_result(rows=[], message=f"查询失败：{str(e)}")
