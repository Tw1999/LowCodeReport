# 收缴率报表
# import sys
import datetime
 

# 解析参数
comm_ids = params.get("comm_ids", [])  # 项目列表
corp_cost_ids = params.get("corp_cost_ids", [])  # 公司科目列表
fee_start_date = params.get("fee_start_date", "")  # 费用开始时间
fee_end_date = params.get("fee_end_date", "")  # 费用结束时间
deal_start_date = params.get("deal_start_date", "")  # 收款开始时间
deal_end_date = params.get("deal_end_date", "")  # 收款结束时间



# 处理多选参数格式
if isinstance(comm_ids, str):
    comm_ids = [x.strip() for x in comm_ids.split(',') if x.strip()]
elif isinstance(comm_ids, tuple):
    comm_ids = list(comm_ids)
comm_ids = [str(x).strip().strip("'").strip('"') for x in comm_ids]


if corp_cost_ids :
    if isinstance(corp_cost_ids, str):
        corp_cost_ids = [x.strip() for x in corp_cost_ids.split(',') if x.strip()]
    elif isinstance(corp_cost_ids, tuple):
        corp_cost_ids = list(corp_cost_ids)
    corp_cost_ids = [str(x).strip().strip("'").strip('"') for x in corp_cost_ids]


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

# 处理日期参数 start_date_obj = parse_date(start_date)  # 重命名避免覆盖原始字符串

# 转换为时间格式
fee_start_date = parse_date(fee_start_date)  
fee_end_date = parse_date(fee_end_date)   
deal_start_date = parse_date(deal_start_date)   
deal_end_date = parse_date(deal_end_date)   


# 获取年初时间
year_start = fee_start_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)



# 获取当年12月31号
year_end = fee_start_date.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=0)

# 构建占位符
comm_placeholders = ','.join(['%s'] * len(comm_ids))
cost_placeholders = ''
if corp_cost_ids :
    cost_placeholders = ' AND a.corp_cost_id IN (' + ','.join(['%s'] * len(corp_cost_ids)) + ')'


# 构建SqL - 使用CTE一次性计算所有指标

sql = f'''
WITH 

developers_data AS (
    select distinct a.comm_id,case a.is_developers when 1 then 1 else 0 end as is_developers from erp_base.tb_base_masterdata_customer_comm  a where  is_delete=0 AND a.comm_id IN ({comm_placeholders})  
),

cost_data AS (
    select   distinct
			a.comm_id,
			a.cost_name,
			corp_cost_id,
			corp.sort
		from tidb_sync_combine.tb_charge_cost a
		LEFT JOIN erp_base.tb_base_charge_cost corp  ON corp.id = a.corp_cost_id 
		WHERE 1 = 1   and  ifnull(a.is_delete,0) = 0 and ifnull(corp.cost_name,'') != ''
		AND a.comm_id IN ({comm_placeholders})
		{cost_placeholders}
		
),

ncqf_ys AS (
    SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.due_amount) AS amount
				
    FROM tidb_sync_combine.view_charge_fee a
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
		    
            AND a.is_delete = 0
			AND a.fee_date < %s
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

ncqf_wnhc AS (
    SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.due_amount) AS amount
				
    FROM tidb_sync_combine.view_charge_fee_his a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
			AND a.is_delete = 0
			AND a.fee_date < %s
			AND a.deal_date < %s
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

ncqf_wnsk AS (
    SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.deal_amount) * -1 AS amount
    FROM tidb_sync_combine.view_receipts_detail a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.fee_id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
            AND a.is_delete = 0
			AND a.fee_date < %s
			AND a.deal_date < %s
            AND a.deal_type IN ('实收', '代扣','托收确认','实收红冲','预存冲抵', '预存冲抵红冲','减免','减免红冲')
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

ncqfdn_sk AS (
    SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.deal_amount) * -1 AS amount
    FROM tidb_sync_combine.view_receipts_detail a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.fee_id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
            AND a.is_delete = 0
			AND a.fee_date < %s
			AND a.deal_date >= %s
			AND a.deal_date <= %s
            AND a.deal_type IN ('实收', '代扣','托收确认','实收红冲','预存冲抵', '预存冲抵红冲','减免','减免红冲')
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

ncqf_dnhc AS (
    SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.due_amount) AS amount
    FROM tidb_sync_combine.view_charge_fee_his a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		   
			AND a.is_delete = 0
			AND a.fee_date < %s
			AND a.deal_date >= %s
			AND a.deal_date <= %s
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),


bhys_bzbd AS(
		SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        ifnull (ifnull((sum((case when (a.calc_model like '%%按定额每月计费%%') then a.calc_amount end)) * 12),0) 
				+ 
				ifnull((sum((case when (a.calc_model like '%%按计费面积*单价每月计费%%') then (a.calc_amount * a.calc_number) end)) * 12),0),0)
				as amount
		from tidb_sync_combine.view_stan_setting a
		where 1=1
		AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		
        AND a.is_delete = 0
				GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

bhys_cjz AS(
		SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
				ifnull(sum(a.year_due_amount),0) as amount
		from tidb_sync_combine.view_fee_contract a
		
		where 1=1
		 AND a.comm_id IN ({comm_placeholders})  
     {cost_placeholders}
        AND a.is_delete = 0
				AND a.cont_type in (
				'1e020e48-f9d3-4e05-ad04-1185aca63444',
				'30cb55d1-c327-4f9d-956b-2b06384a4e1c',
				'66690bdb-9454-4701-a0c3-bf9ed6abf198',
				'675adeb6-5624-40dd-bbb2-48b2e031fb38',
				'a5be91b3-46cb-4b14-b743-cb32c1503b2d'
				)
				GROUP BY a.comm_id, corp_cost_id,a.is_developers
		
),

bhys_bgz AS(
		SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        ifnull(sum(a.due_amount),0) as amount
		from tidb_sync_combine.view_charge_fee a
		where a.busi_id in (select b.id from tidb_sync_combine.view_fee_contract b where b.cont_type in (
		'1cba1445-3393-4cd5-8130-c0f271f86dd7',
		'89450d3f-1a13-4d04-a69c-f64c6c4dd460',
		'c1fef210-12fa-4ea5-934c-3946ca4ca840',
		'c676849a-3c99-417a-a2cd-f7bd9765241a',
		'f22fd9df-7d4c-426a-a024-e568fbc79c61'
		))
		AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		
    AND a.is_delete = 0
		AND a.fee_date <= %s
		AND a.fee_date >= %s
		GROUP BY a.comm_id, corp_cost_id,a.is_developers
),

ljys_ys AS(
		SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
				ifnull(sum(a.due_amount),0) as amount
		from tidb_sync_combine.view_charge_fee a
		where 1=1
		AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		
        AND a.is_delete = 0
				AND a.fee_date <= %s
				AND a.fee_date >= %s
				GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

ljys_hc AS (
    SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.due_amount) AS amount
				
    FROM tidb_sync_combine.view_charge_fee_his a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
			AND a.is_delete = 0
			AND a.fee_date <= %s
			AND a.fee_date >= %s
			AND a.deal_date <= %s
			AND a.deal_date >= %s
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

dyys_ys AS(
		SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
				ifnull(sum(a.due_amount),0) as amount
		from tidb_sync_combine.view_charge_fee a
		where 1=1
		AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		
        AND a.is_delete = 0
				AND a.fee_date <= %s
				AND a.fee_date >= %s
				GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

dyys_hc AS (
    SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.due_amount) AS amount
    FROM tidb_sync_combine.view_charge_fee_his a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
			AND a.is_delete = 0
			AND a.fee_date <= %s
			AND a.fee_date >= %s
			AND a.deal_date <= %s
			AND a.deal_date >= %s
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

ljys_sk AS(
		SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.deal_amount) * -1 AS amount
    FROM tidb_sync_combine.view_receipts_detail a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.fee_id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
            AND a.is_delete = 0
			
			AND a.fee_date >= %s
			AND a.fee_date <= %s
			AND a.deal_date >= %s
			AND a.deal_date <= %s
            AND a.deal_type IN ('实收', '代扣','托收确认','实收红冲','预存冲抵', '预存冲抵红冲','减免','减免红冲')
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

ssft_sk AS(
		SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.deal_amount) * -1 AS amount
    FROM tidb_sync_combine.view_receipts_detail a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.fee_id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
            AND a.is_delete = 0
			AND a.deal_date >= %s
			AND a.deal_date <= %s
            AND a.deal_type IN ('实收', '代扣','托收确认','实收红冲','预存冲抵', '预存冲抵红冲','减免','减免红冲')
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
),

dyys_sk AS(
		SELECT
        a.comm_id,
        a.corp_cost_id,
				case a.is_developers when 1 then 1 else 0 end as is_developers,
        SUM(a.deal_amount) * -1 AS amount
    FROM tidb_sync_combine.view_receipts_detail a
    INNER JOIN tidb_sync_combine.view_charge_fee b ON a.fee_id = b.id
		WHERE 1=1
			AND a.comm_id IN ({comm_placeholders})  {cost_placeholders}
		    
            AND a.is_delete = 0
			AND a.fee_date >= %s
			AND a.fee_date <= %s
			AND a.deal_date >= %s
			AND a.deal_date <= %s
            AND a.deal_type IN ('实收', '代扣','托收确认','实收红冲','预存冲抵', '预存冲抵红冲','减免','减免红冲')
    GROUP BY a.comm_id, a.corp_cost_id,a.is_developers
)
SELECT
    zzjg.Province AS 城市,
    zzjg.name AS 项目名称,
		dic.title as 业务线,
		case zzjg.CommKind when 1 then '住宅' when 2 then '商办' when 3 then '商办' else '平台' end as 业态,
		cost_data.cost_name AS 科目名称,
		case base.is_developers when 1 then '开发商' else '业主' end as 欠费主体,
		(IFNULL(ncqf_ys.amount,0) - IFNULL(ncqf_wnhc.amount,0) - IFNULL(ncqf_wnsk.amount,0)) as 以前年度欠费,
		ifnull(ncqfdn_sk.amount,0) as 实收以前年度欠费,
		round((IFNULL(bhys_bzbd.amount,0) + IFNULL(bhys_cjz.amount,0) + IFNULL(bhys_bgz.amount,0)),2) as 饱和应收,
		ifnull(ljys_ys.amount,0) as 累计应收,
		round((CASE WHEN  (IFNULL(ncqf_ys.amount,0) - IFNULL(ncqf_wnhc.amount,0) - IFNULL(ncqf_wnsk.amount,0)) != 0 
		THEN
			(ifnull(ncqfdn_sk.amount,0)) / (IFNULL(ncqf_ys.amount,0) - IFNULL(ncqf_wnhc.amount,0) - IFNULL(ncqf_wnsk.amount,0))
            * 100 ELSE 0 END),2) AS 清欠率,
		round((CASE WHEN  (IFNULL(dyys_ys.amount,0) - IFNULL(dyys_hc.amount,0)) != 0 
		THEN
			(ifnull(dyys_sk.amount,0)) / (IFNULL(dyys_ys.amount,0) - IFNULL(dyys_hc.amount,0))
            * 100 ELSE 0 END),2) AS 当月收缴率,
		round((CASE WHEN  (IFNULL(ljys_ys.amount,0) - IFNULL(ljys_hc.amount,0)) != 0 
		THEN
			(ifnull(ssft_sk.amount,0)) / (IFNULL(ljys_ys.amount,0) - IFNULL(ljys_hc.amount,0))
            * 100 ELSE 0 END),2) AS 当期收缴率,
		round((CASE WHEN  (IFNULL(bhys_bzbd.amount,0) + IFNULL(bhys_cjz.amount,0) + IFNULL(bhys_bgz.amount,0)) != 0 
		THEN
			(ifnull(ljys_sk.amount,0)) / (IFNULL(bhys_bzbd.amount,0) + IFNULL(bhys_cjz.amount,0) + IFNULL(bhys_bgz.amount,0))
            * 100 ELSE 0 END),2) AS 年度收缴率
FROM developers_data base
LEFT JOIN erp_base.rf_organize zzjg on base.comm_id=zzjg.id 
LEFT JOIN erp_base.rf_organize_expand exp on base.comm_id=exp.organize_id
LEFT JOIN cost_data on base.comm_id=cost_data.comm_id
LEFT JOIN erp_base.rf_dictionary dic on exp.comm_format=dic.id
LEFT JOIN ncqf_ys ON cost_data.comm_id = ncqf_ys.comm_id AND cost_data.corp_cost_id = ncqf_ys.corp_cost_id and base.is_developers=ncqf_ys.is_developers
LEFT JOIN ncqf_wnhc ON cost_data.comm_id = ncqf_wnhc.comm_id AND cost_data.corp_cost_id = ncqf_wnhc.corp_cost_id and base.is_developers=ncqf_wnhc.is_developers
LEFT JOIN ncqf_wnsk ON cost_data.comm_id = ncqf_wnsk.comm_id AND cost_data.corp_cost_id = ncqf_wnsk.corp_cost_id and base.is_developers=ncqf_wnsk.is_developers
LEFT JOIN ncqf_dnhc ON cost_data.comm_id = ncqf_dnhc.comm_id AND cost_data.corp_cost_id = ncqf_dnhc.corp_cost_id and base.is_developers=ncqf_dnhc.is_developers
LEFT JOIN ncqfdn_sk ON cost_data.comm_id = ncqfdn_sk.comm_id AND cost_data.corp_cost_id = ncqfdn_sk.corp_cost_id and base.is_developers=ncqfdn_sk.is_developers
LEFT JOIN bhys_bzbd ON cost_data.comm_id = bhys_bzbd.comm_id AND cost_data.corp_cost_id = bhys_bzbd.corp_cost_id and base.is_developers=bhys_bzbd.is_developers
LEFT JOIN bhys_cjz ON cost_data.comm_id = bhys_cjz.comm_id AND cost_data.corp_cost_id = bhys_cjz.corp_cost_id and base.is_developers=bhys_cjz.is_developers
LEFT JOIN bhys_bgz ON cost_data.comm_id = bhys_bgz.comm_id AND cost_data.corp_cost_id = bhys_bgz.corp_cost_id and base.is_developers=bhys_bgz.is_developers
LEFT JOIN ljys_ys ON cost_data.comm_id = ljys_ys.comm_id AND cost_data.corp_cost_id = ljys_ys.corp_cost_id and base.is_developers=ljys_ys.is_developers
LEFT JOIN dyys_ys ON cost_data.comm_id = dyys_ys.comm_id AND cost_data.corp_cost_id = dyys_ys.corp_cost_id and base.is_developers=dyys_ys.is_developers
LEFT JOIN ljys_sk ON cost_data.comm_id = ljys_sk.comm_id AND cost_data.corp_cost_id = ljys_sk.corp_cost_id and base.is_developers=ljys_sk.is_developers
LEFT JOIN ssft_sk ON cost_data.comm_id = ssft_sk.comm_id AND cost_data.corp_cost_id = ssft_sk.corp_cost_id and base.is_developers=ssft_sk.is_developers
LEFT JOIN dyys_sk ON cost_data.comm_id = dyys_sk.comm_id AND cost_data.corp_cost_id = dyys_sk.corp_cost_id and base.is_developers=dyys_sk.is_developers
LEFT JOIN dyys_hc ON cost_data.comm_id = dyys_hc.comm_id AND cost_data.corp_cost_id = dyys_hc.corp_cost_id and base.is_developers=dyys_hc.is_developers
LEFT JOIN ljys_hc ON cost_data.comm_id = ljys_hc.comm_id AND cost_data.corp_cost_id = ljys_hc.corp_cost_id and base.is_developers=ljys_hc.is_developers
WHERE 1=1
'''
# 构建参数列表（严格按照SqL中%s出现顺序）
args = []

# 1. 基础cte

# 是否开发商
args.extend(comm_ids)

# 科目
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)
		

# 年初欠费-应收
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_start)


# 年初欠费-往年红冲
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_start)
args.append(year_start)

# 年初欠费-往年收款
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_start)
args.append(year_start)

# 年初欠费-当年收款
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_start)
args.append(year_start)
args.append(year_end)

# 年初欠费-当年红冲
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_start)
args.append(year_start)
args.append(year_end)

# 饱和应收
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)
		
# 饱和应收-酬金制合同
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)
		
# 饱和应收-包干制合同
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_end)
args.append(year_start)


# 累计应收
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_end)
args.append(year_start)

# 累计应收-红冲
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_end)
args.append(year_start)
args.append(deal_end_date)
args.append(deal_start_date)


# 当月应收
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(fee_end_date)
args.append(fee_start_date)

# 当月应收-红冲
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(fee_end_date)
args.append(fee_start_date)
args.append(deal_end_date)
args.append(deal_start_date)

# 累计应收-收款
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_start)
args.append(year_end)
args.append(deal_start_date)
args.append(deal_end_date)

# 实收分摊-收款
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(year_start)
args.append(year_end)

# 当月应收-收款
args.extend(comm_ids)
if cost_placeholders:
    args.extend(corp_cost_ids)

args.append(fee_end_date)
args.append(fee_start_date)
args.append(deal_start_date)
args.append(deal_end_date)


# db_query(sql, tuple(args))

# 生成调试SqL（将%s替换为实际参数值）
debug_sql = sql
for arg in args:
    if arg is None:
        debug_sql = debug_sql.replace('%s', 'NULL', 1)
    elif isinstance(arg, (int, float)):
        debug_sql = debug_sql.replace('%s', str(arg), 1)
    else:
        debug_sql = debug_sql.replace('%s', f"'{arg}'", 1)

debug = "0"

if debug == "1": 
    set_result(rows=[{"debug_sql": debug_sql}], message=f"查询成功!")
else:
    data_rows = db_query(sql, tuple(args))
    set_result(rows=data_rows, message=f"查询成功!")
