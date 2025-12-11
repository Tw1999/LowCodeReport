# 分页查询权限表，默认每页100条，默认第1页
page = int(params.get("page", 1))  # 页码，从1开始
page_size = int(params.get("page_size", 100))  # 每页条数，默认100
offset = (page - 1) * page_size  # 计算偏移量

sql = """
SELECT MenuId, Id, Organizes
FROM tidb_sync_combine.rf_menuuser
"""
args = []

# 可选的ID过滤条件
if params.get("id"):
    sql += " WHERE id = %s"
    args.append(params["id"])

# 排序和分页
sql += " ORDER BY is_delete DESC LIMIT %s OFFSET %s"
args.append(page_size)
args.append(offset)

# 生成调试SQL（将%s替换为实际参数值）
debug_sql = sql
for arg in args:
    if arg is None:
        debug_sql = debug_sql.replace('%s', 'NULL', 1)
    elif isinstance(arg, (int, float)):
        debug_sql = debug_sql.replace('%s', str(arg), 1)
    else:
        debug_sql = debug_sql.replace('%s', f"'{arg}'", 1)

# 如果传入debug=1参数，只返回SQL不执行查询
if params.get("debug") == "1":
    set_result(rows=[{"debug_sql": debug_sql}], message="调试模式-返回SQL语句")
else:
    dataRows = db_query(sql, tuple(args))
    set_result(rows=dataRows, message=f"查询成功，第{page}页，每页{page_size}条\nSQL: {debug_sql}")