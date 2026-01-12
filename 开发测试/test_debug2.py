# 更简单的调试方式：逐步缩小范围

# 1. 先查所有 deal_type 包含减免的记录
sql1 = """
SELECT id, deal_type, is_delete, deal_date
FROM tb_charge_receipts_detail
WHERE deal_type LIKE '%减免%'
LIMIT 50
"""

# 2. 查询 deal_type = '减免' 的记录（注意：可能字段值前后有空格）
sql2 = """
SELECT id, deal_type, is_delete, deal_date,
       CASE WHEN deal_type = '减免' THEN 'Y' ELSE 'N' END as exact_match
FROM tb_charge_receipts_detail
WHERE deal_type LIKE '%减免%'
LIMIT 50
"""

# 3. 查询 is_delete = 1 的记录
sql3 = """
SELECT id, deal_type, is_delete, deal_date
FROM tb_charge_receipts_detail
WHERE is_delete = 1
LIMIT 50
"""

# 4. 查询组合条件（使用 TRIM 去除可能的空格）
sql4 = """
SELECT id, deal_type, is_delete, deal_date,
       CONCAT('[', deal_type, ']') as deal_type_with_bracket
FROM tb_charge_receipts_detail
WHERE TRIM(deal_type) = '减免' AND is_delete = 1
LIMIT 50
"""

# 5. 最宽松的查询：包含减免且is_delete=1
sql5 = """
SELECT id, deal_type, is_delete, deal_date
FROM tb_charge_receipts_detail
WHERE deal_type LIKE '%减免%' AND is_delete = 1
LIMIT 50
"""

try:
    r1 = db_query(sql1, ())
    r2 = db_query(sql2, ())
    r3 = db_query(sql3, ())
    r4 = db_query(sql4, ())
    r5 = db_query(sql5, ())

    msg = f"""
调试结果：

1. deal_type包含'减免'的记录: {len(r1)} 条
   前3条: {r1[:3] if r1 else '无'}

2. deal_type包含'减免'且判断是否精确匹配: {len(r2)} 条
   前3条: {r2[:3] if r2 else '无'}

3. is_delete=1 的记录: {len(r3)} 条
   前3条: {r3[:3] if r3 else '无'}

4. TRIM(deal_type)='减免' AND is_delete=1: {len(r4)} 条
   前3条: {r4[:3] if r4 else '无'}

5. deal_type LIKE '%减免%' AND is_delete=1: {len(r5)} 条
   前3条: {r5[:3] if r5 else '无'}
"""

    # 返回第5个查询的结果（最宽松的条件）
    set_result(rows=r5, message=msg)

except Exception as e:
    set_result(rows=[], message=f"查询失败: {str(e)}")
