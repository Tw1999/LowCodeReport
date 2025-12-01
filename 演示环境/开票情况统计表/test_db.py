# 测试数据库连接和查看数据库列表
import pymysql

conn = pymysql.connect(
    host='8.137.109.26',
    port=9030,
    user='root',
    password='tw369.com',
    charset='utf8mb4'
)

cursor = conn.cursor()

# 查看所有数据库
cursor.execute("SHOW DATABASES")
databases = cursor.fetchall()
print("可用的数据库:")
for db in databases:
    print(f"  - {db[0]}")

# 查看表 (假设在某个数据库中)
print("\n尝试查找包含 tb_charge_fee 的数据库...")
for db in databases:
    try:
        cursor.execute(f"USE {db[0]}")
        cursor.execute("SHOW TABLES LIKE 'tb_charge_fee'")
        result = cursor.fetchone()
        if result:
            print(f"  找到! 数据库: {db[0]}")
    except:
        pass

print("\n尝试查找包含 rf_organize 的数据库...")
for db in databases:
    try:
        cursor.execute(f"USE {db[0]}")
        cursor.execute("SHOW TABLES LIKE 'rf_organize'")
        result = cursor.fetchone()
        if result:
            print(f"  找到! 数据库: {db[0]}")
    except:
        pass

cursor.close()
conn.close()
