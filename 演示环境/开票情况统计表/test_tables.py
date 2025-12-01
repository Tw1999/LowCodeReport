# 查看erp_dw和erp_statistics中的表
import pymysql

conn = pymysql.connect(
    host='8.137.109.26',
    port=9030,
    user='root',
    password='tw369.com',
    charset='utf8mb4'
)

cursor = conn.cursor()

# 查看 erp_dw 的表
print("=== erp_dw 数据库中的表 ===")
cursor.execute("USE erp_dw")
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
for table in tables:
    if 'charge' in table[0].lower() or 'fee' in table[0].lower() or 'receipt' in table[0].lower():
        print(f"  - {table[0]}")

# 查看 erp_statistics 的表
print("\n=== erp_statistics 数据库中的表 ===")
cursor.execute("USE erp_statistics")
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
for table in tables:
    if 'charge' in table[0].lower() or 'fee' in table[0].lower() or 'receipt' in table[0].lower():
        print(f"  - {table[0]}")

# 检查 erp_base 中的科目表
print("\n=== erp_base 数据库中的表 ===")
cursor.execute("USE erp_base")
cursor.execute("SHOW TABLES LIKE '%cost%'")
tables = cursor.fetchall()
for table in tables:
    print(f"  - {table[0]}")

cursor.close()
conn.close()
