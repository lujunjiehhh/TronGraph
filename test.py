import pandas as pd
from sqlalchemy import create_engine
import mmh3
# 线程池
from concurrent.futures import ThreadPoolExecutor

# 步骤1: 使用pandas读取CSV文件
df = pd.read_csv('F:\Work\Blockchain\code\新建文件夹 (3)\TronGraph\TLRhiCwfqHbEdLqEDBsUA83JQPJAhyr7yQ_transactions1.csv', sep=',')
# 跳过头部信息，如果CSV文件中有表头，您可以使用 skiprows 参数跳过它
# 例如，如果表头在第一行，您可以添加 skiprows=1 参数

# 取100bit
def get_bit(x:int,n:int):
    return int(x & ((1 << n) - 1))

df['from_hash'] = df.apply(lambda x: mmh3.hash64(x['from'], x64arch=True)[0],axis=1)
df['to_hash'] = df.apply(lambda x: mmh3.hash64(x['to'], x64arch=True)[0], axis=1)
df['transid_hash'] = df.apply(lambda x: mmh3.hash(x['transaction_id']),axis=1)
# 步骤2: 使用SQLAlchemy创建数据库连接
# 请根据您的StarRocks数据库信息替换以下占位符
username = 'root'
password = ''  # 如果您设置了密码，请在这里填写
host = '172.31.98.9'
port = 9030    # StarRocks默认的MySQL协议端口是9030，但这里使用8030，如果不同请修改
database = 'tron_usdt_trans'  # 您的数据库名

# 创建数据库引擎

engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}')
# df分块
# 步骤3: 将DataFrame写入StarRocks表中
# 请确保表 'weatherdata' 已经存在于您的数据库中，并且其列与DataFrame中的列匹配
import time

start_time = time.time()
df.to_sql('usdt_trading_details', con=engine, if_exists='append', index=False,  chunksize=10000)
end_time = time.time()
# 关闭数据库连接
engine.dispose()
# 对times画折线图


