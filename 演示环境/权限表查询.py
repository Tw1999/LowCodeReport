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

dataRows = db_query(sql, tuple(args))
set_result(rows=dataRows, message=f"查询成功，第{page}页，每页{page_size}条")