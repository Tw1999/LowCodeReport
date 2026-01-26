 
sql ="""  
    
SELECT
    BB.name as 项目名称, 
    comm_id as commid,
    fwxz,
    corp_cost_id,
    cost_name,
    stan_price,
    count1,
    calc_area,
    YDBHYS,
    NDBHYS,
    DNBHYS 
from (
    SELECT 
        a.comm_id,
        ifnull(f.title,'未设房屋性质') as fwxz,  
        c.corp_cost_id, 
        c.cost_name,
       ROUND( e.stan_price,4) as stan_price,
        count(*) as count1,
        SUM(ROUND(b.calc_area,2)) as calc_area,
        ROUND(SUM(ROUND(b.calc_area,2) * e.stan_price),2) as YDBHYS,
         ROUND(SUM(ROUND(b.calc_area,2) * e.stan_price),2)  * 12 as NDBHYS,
        ROUND(SUM(
            CASE 
                WHEN 
                YEAR(b.pay_begin_date) = YEAR(NOW())
                AND IFNULL(orgEx.exit_date,'2099-12-31 23:59:59') >  DATE_FORMAT(NOW(),'%%Y-12-31 23:59:59') 
                THEN
                   ((ROUND(b.calc_area,2) * e.stan_price) * ((DAY(LAST_DAY(b.pay_begin_date))-DAY(b.pay_begin_date)+1)/DAY(LAST_DAY(b.pay_begin_date))))
                    +
                   ((ROUND(b.calc_area,2) * e.stan_price) * (12-MONTH(b.pay_begin_date)))
										
                WHEN 
                YEAR(b.pay_begin_date) = YEAR(NOW())
                AND YEAR(IFNULL(orgEx.exit_date,'2099-12-31 23:59:59')) =  YEAR(NOW())
                THEN
                   ((ROUND(b.calc_area,2) * e.stan_price) * ((DAY(LAST_DAY(b.pay_begin_date))-DAY(b.pay_begin_date)+1)/DAY(LAST_DAY(b.pay_begin_date))))
                    +
                   ((ROUND(b.calc_area,2) * e.stan_price) * (MONTH(IFNULL(orgEx.exit_date,'2099-12-31 23:59:59'))-MONTH(b.pay_begin_date)-1))
                    +
                   ((ROUND(b.calc_area,2) * e.stan_price) * (DAY(IFNULL(orgEx.exit_date,'2099-12-31 23:59:59'))/DAY(LAST_DAY(IFNULL(orgEx.exit_date,'2099-12-31 23:59:59')))))
										
                WHEN 
                IFNULL(b.pay_begin_date,DATE_SUB(NOW(),INTERVAL 1 YEAR)) < DATE_FORMAT(NOW(),'%%Y-01-01 00:00:00') 
                AND IFNULL(orgEx.exit_date,'2099-12-31 23:59:59') >  DATE_FORMAT(NOW(),'%%Y-12-31 23:59:59')
                THEN
                    ROUND(ROUND(b.calc_area,2) * e.stan_price,2) * 12
                WHEN 
                IFNULL(b.pay_begin_date,DATE_SUB(NOW(),INTERVAL 1 YEAR)) < DATE_FORMAT(NOW(),'%%Y-01-01 00:00:00') 
                AND YEAR(IFNULL(orgEx.exit_date,'2099-12-31 23:59:59')) =  YEAR(NOW())
                THEN
                   ((ROUND(b.calc_area,2) * e.stan_price) * (MONTH(IFNULL(orgEx.exit_date,'2099-12-31 23:59:59'))-1))
                    +
                   ((ROUND(b.calc_area,2) * e.stan_price) * ((DAY(IFNULL(orgEx.exit_date,'2099-12-31 23:59:59')))/DAY(LAST_DAY(IFNULL(orgEx.exit_date,'2099-12-31 23:59:59')))))
                ELSE
                    0
            END
        ),2) as DNBHYS
    FROM 
        tidb_sync_combine.tb_charge_fee_stan_setting a
        LEFT JOIN tidb_sync_combine.tb_base_masterdata_resource b ON a.resource_id = b.id
        LEFT JOIN tidb_sync_combine.tb_charge_cost c ON a.cost_id = c.id
        LEFT JOIN erp_base.tb_base_charge_comm_stan e ON a.stan_id = e.id
        LEFT JOIN erp_base.rf_dictionary f ON b.property_rights = f.id
        LEFT JOIN erp_base.rf_organize_expand orgEx ON a.comm_id = orgEx.organize_id 
    WHERE 1=1 
        AND a.is_delete = 0
        AND b.resource_type = 3
        AND e.id is not null
        AND IFNULL(orgEx.exit_date,'2099-12-31 23:59:59') >=  DATE_FORMAT(NOW(),'%%Y-01-01 00:00:00')
        AND IFNULL(b.pay_begin_date,DATE_SUB(NOW(),INTERVAL 1 YEAR)) <= DATE_FORMAT(NOW(),'%%Y-12-31 23:59:59')
    GROUP BY
        a.comm_id, 
        ifnull(f.title,'未设房屋性质'),  
        c.corp_cost_id, 
        c.cost_name,
        ROUND(e.stan_price,4)
) BL
inner join erp_base.rf_organize BB on BL.comm_id=BB.id 
WHERE 1=1 and BB.organType=6 and BB.is_delete=0 and BB.type=1 
"""

args = []

# 分页查询权限表，默认每页100条，默认第1页
page = int(params.get("page", 1))  # 页码，从1开始
page_size = int(params.get("page_size", 20))  # 每页条数，默认100
offset = (page - 1) * page_size  # 计算偏移量


# 1. 添加comm_ids参数（支持多个）
if params.get("comm_id"):
    comm_ids = params.get("comm_id")
    
    # 如果是字符串，按逗号分隔
    if isinstance(comm_ids, str):
        comm_ids = [cid.strip() for cid in comm_ids.split(',') if cid.strip()]
    
    # 如果是列表或元组，直接使用
    if comm_ids:
        # 构造IN语句的占位符
        placeholders = ','.join(['%s'] * len(comm_ids))
        sql += f" AND BL.comm_id IN ({placeholders})"
        args.extend(comm_ids)

# 2. 添加corp_cost_id参数（支持多个）
if params.get("corp_cost_id"):
    corp_cost_ids = params.get("corp_cost_id")
    
    # 如果是字符串，按逗号分隔
    if isinstance(corp_cost_ids, str):
        corp_cost_ids = [cid.strip() for cid in corp_cost_ids.split(',') if cid.strip()]
    
    # 如果是列表或元组，直接使用
    if corp_cost_ids:
        # 构造IN语句的占位符
        placeholders = ','.join(['%s'] * len(corp_cost_ids))
        sql += f" AND BL.corp_cost_id IN ({placeholders})"
        args.extend(corp_cost_ids)

# 添加排序
sql += " ORDER BY BB.name, fwxz,BL.cost_name, BL.stan_price LIMIT %s OFFSET %s"
 

# 添加分页参数
args.append(page_size)
args.append(offset)

# 添加调试信息
print("生成的SQL（部分）:", sql[:500] + "..." if len(sql) > 500 else sql)
print("参数列表:", args)


# 执行查询
dataRows = db_query(sql, tuple(args))

# 返回结果
set_result(rows=dataRows, message=f"查询成功，第{page}页，每页{page_size}条\nSQL:"+sql)