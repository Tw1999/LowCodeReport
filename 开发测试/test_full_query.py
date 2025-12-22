# 测试完整SQL - 使用全部参数
import pymysql
import sys
sys.path.insert(0, 'c:\\Tw.Erp\\LowCodeReport\\演示环境\\开票情况统计表')

# 直接导入原始脚本内容并执行
comm_ids = '0591cfbd-d915-48d6-9831-7f6201cd4d3e','0d8400d6-d728-4f72-9e6c-08bb9bf5828d','21bea8c6-46e8-4d5f-9ead-cc1ff8e3b90e','48b34bc1-4274-4be5-afac-3f252eda05d8','4eb68e2f-d1f4-486d-b7bf-d0403f8a76e7','73b796a2-74a9-4406-af1c-15ee0218e82e','ac3ece62-af2d-45b7-b644-7a60961f8abd','e1c078af-62ac-4923-9267-7f99d5dffb6a','e236f7bb-626a-4288-83bc-405d4a44545c','e5c4dfcb-e1db-443a-ab47-bf18eb48a478'

corp_cost_ids = '07772f1e-4d0e-4ef9-b2b5-e2b1c99f8b7a','09fccd77-0878-4e25-a817-0fb03c305dc1','15d56143-bd27-4ccd-898d-991081694fb3','1904e02c-17d4-4796-a9eb-9eb3ed5dfe2f','28f767a4-b1d6-45fd-92d5-b7668a62d1e9','2a536b39-f4a8-4f13-af61-ad63f56989be','2bb34b1c-b7f4-438d-968d-b56a1de7be64','3d55cb31-2cee-47c6-9883-615b53167d88','40709b88-ce74-48a3-a60b-45758d2efe0e','42605ba4-83f0-4de8-a742-4e64c1081566','4416974b-35ea-4f67-b655-6593cf57b10b','479e2925-a00e-4f62-b35e-cd5ed0405381','4dcaa913-d588-4f30-b03b-11c21810f0d2','5bc4c724-ecc3-4ba3-8ea7-038154f55a13','5ef3f6e7-ec98-4dde-8321-94ba79b5d22c','631eeeaf-0464-4bb2-b1f3-4f0096b11309','6c8912d4-8a96-4290-af65-3807db983925','6cf57fd4-f314-4038-99bb-3af473d0efb5','6dcedf04-1413-4449-8528-e8f017328c68','7e08fcc5-7ed8-4742-9e9e-ef3fa634a875','85580050-8858-4111-9e96-57d825ce0ffc','8c0ab63d-6023-4c89-b4f7-ab886338c033','8c9b4770-51b4-4b25-bfb5-7df38de85ebb','9002c943-2f8f-4f7f-b8f6-bdf818c12586','9c6e5462-e639-4eb7-80e8-b518edeb54ff','a32342c8-94e5-40c1-9e72-903e111aa7e5','a7c26688-f20a-11ec-9bda-00163e03b5f6','a7c2675b-f20a-11ec-9bda-00163e03b5f6','a7c26813-f20a-11ec-9bda-00163e03b5f6','a7c2689b-f20a-11ec-9bda-00163e03b5f6','a7c268c0-f20a-11ec-9bda-00163e03b5f6','a7c26a89-f20a-11ec-9bda-00163e03b5f6','a7c26bca-f20a-11ec-9bda-00163e03b5f6','a7c26bec-f20a-11ec-9bda-00163e03b5f6','a9ced350-60ce-4cf7-8c65-56745bd5b4a7','b8b235bd-3cb0-433d-9256-92b814811245','b9e595b4-004b-4b4b-8582-5bff131fb62d','c0bb1bc6-0336-4db4-be5f-a78c6fe20973','cc9ecb22-0206-4fd1-913f-04a848c42dd1','d754d22f-52c6-4e5b-81d3-7f6d4d7804e6','d93bcc98-6a3d-4603-8285-9d95a64b02ed','e70d1e38-aee1-4b37-9919-8a2d51e0dc4f','e7a382ec-6da7-42e5-80d0-876260ddcc14','ee153b99-0bfb-44e3-9e57-d85a04454a51','f6fd5e82-72ed-4052-a88c-8aafbd744bf6'

params = {
    "comm_ids": list(comm_ids),
    "corp_cost_ids": list(corp_cost_ids),
    "start_date": "2001-1-1",
    "end_date": "2025-12-31",
    "contract_type": "",
    "debug": "1"
}

conn = pymysql.connect(
    host='8.137.109.26',
    port=9030,
    user='root',
    password='tw369.com',
    database='erp_statistics',
    charset='utf8mb4'
)

def db_query(sql, args):
    cursor = conn.cursor()
    try:
        cursor.execute(sql, args)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"SQL执行错误: {e}")
        return []
    finally:
        cursor.close()

def set_result(rows, message):
    print(f"{message}")
    print(f"返回行数: {len(rows)}")
    if len(rows) > 0:
        print("前5行数据:")
        for i, row in enumerate(rows[:5], 1):
            print(f"{i}. {row}")
    else:
        print("没有数据")

# 执行原始脚本
exec(open('C:\Tw.Erp\LowCodeReport\开发测试\欠费查询.py', encoding='utf-8').read())

conn.close()
