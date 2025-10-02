import os
import struct
import pandas as pd
import hashlib
from mootdx.reader import Reader
from tqdm import tqdm
import redis
from redis.exceptions import ConnectionError


# 组前缀（与Rust端保持一致：rust_stock_group:）
GROUP_PREFIX = "stock_group:"  # 统一组前缀，确保和Rust端同组


def read_tdx_day_file_direct(file_path, max_records=20):
    """读取日线文件，取最近20条记录，并对high/low保留两位小数"""
    data_list = []
    try:
        with open(file_path, 'rb') as f:
            data_buffer = f.read()
        
        record_size = 32
        num_records = len(data_buffer) // record_size
        start_idx = max(0, num_records - max_records)
        
        for i in range(start_idx, num_records):
            record = data_buffer[i*record_size:(i+1)*record_size]
            fields = struct.unpack('IIIIIfII', record) 
            
            date_str = str(fields[0])
            open_price = fields[1] / 100.0
            # 对high和low四舍五入保留两位小数（核心修改）
            high_price = round(fields[2] / 100.0, 2)  # 保留两位小数
            low_price = round(fields[3] / 100.0, 2)   # 保留两位小数
            close_price = fields[4] / 100.0
            amount = fields[5]
            volume = fields[6]
            
            data_list.append({
                'date': pd.to_datetime(date_str, format='%Y%m%d'),
                'open': open_price,
                'high': high_price,  # 处理后的值
                'low': low_price,    # 处理后的值
                'close': close_price,
                'amount': amount,
                'volume': volume
            })
        
        df = pd.DataFrame(data_list)
        return df.tail(max_records) if not df.empty else df
    except Exception as e:
        print(f"读取文件 {file_path} 失败: {e}")
        return None


def is_stock_symbol(symbol, market):
    """判断是否为股票代码"""
    if market == 'sh':
        return symbol.startswith(('6'))
    elif market == 'sz':
        return symbol.startswith(('0', '3'))
    elif market == 'bj':
        return symbol.startswith(('4', '8', '9'))
    return False


def get_all_stock_data(tdx_path, min_records=20):
    """读取股票数据，确保至少20条记录，并对high/low保留两位小数"""
    reader = Reader.factory(market='std', tdxdir=tdx_path)
    market_dirs = {
        'sh': os.path.join(tdx_path, 'vipdoc', 'sh', 'lday'),
        'sz': os.path.join(tdx_path, 'vipdoc', 'sz', 'lday'),
        'bj': os.path.join(tdx_path, 'vipdoc', 'bj', 'lday')
    }
    
    all_stock_data = {}
    for market, market_dir in market_dirs.items():
        if not os.path.exists(market_dir):
            print(f"目录不存在: {market_dir}")
            continue
            
        day_files = [f for f in os.listdir(market_dir) if f.endswith('.day')]
        print(f"读取{market}市场，共{len(day_files)}个文件...")
        
        for file_name in tqdm(day_files):
            try:
                symbolok = file_name.replace('.day', '')[2:5]
                symbol = file_name.replace('.day', '')[2:]
                
                if not is_stock_symbol(symbolok, market):
                    continue
                
                if market == 'bj':
                    file_path = os.path.join(market_dir, file_name)
                    df = read_tdx_day_file_direct(file_path, max_records=min_records)
                else:
                    df = reader.daily(symbol=symbol)
                    if df is not None and not df.empty:
                        df = df.tail(min_records).reset_index(drop=True)
                        # 对reader获取的high和low四舍五入保留两位小数（核心修改）
                        df['high'] = df['high'].round(2)  # 保留两位小数
                        df['low'] = df['low'].round(2)     # 保留两位小数
                
                if df is not None and not df.empty and len(df) >= min_records:
                    all_stock_data[f"{market}{symbol}"] = df
                elif df is not None and len(df) < min_records:
                    print(f"{market}{symbol} 记录不足{min_records}条，跳过")
                
            except Exception as e:
                print(f"读取{file_name}出错: {e}")
                continue
        
        market_count = len([k for k in all_stock_data if k.startswith(market)])
        print(f"{market}市场: 符合条件的股票共{market_count}只")
    
    return all_stock_data


def generate_unique_id(high_last20, low_last20):
    """生成与Rust端一致的唯一ID（基于保留两位小数后的high和low）"""
    hasher = hashlib.sha256()
    # 大端字节序，与Rust的to_be_bytes保持一致
    for val in high_last20:
        hasher.update(struct.pack('>d', val))  # 基于两位小数的val计算哈希
    for val in low_last20:
        hasher.update(struct.pack('>d', val))  # 基于两位小数的val计算哈希
    return hasher.hexdigest()


def save_to_redis(all_stock_data, redis_host="localhost", redis_port=6379, 
                 redis_password=None, db=4, group_prefix=GROUP_PREFIX):
    """
    按key前缀分组（与Rust端同组）：key=rust_stock_group:唯一ID，value=股票代码
    """
    # 连接Redis
    try:
        r = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=db,
            decode_responses=False,
            socket_timeout=10
        )
        r.ping()
        print(f"成功连接Redis db{db}，组前缀: {group_prefix}（与Rust端一致）")
    except ConnectionError as e:
        print(f"Redis连接失败: {e}")
        return

    # 处理每只股票
    total = len(all_stock_data)
    success = 0
    fail = 0
    
    print(f"\n开始存储{total}只股票数据...")
    for stock_code, df in tqdm(all_stock_data.items()):
        try:
            # 提取最近20个high和low（已确保是两位小数）
            recent_20 = df.tail(20)
            high_list = recent_20['high'].tolist()
            low_list = recent_20['low'].tolist()
            
            # 打印调试：输出处理后的high和low（可选，用于验证）
            # print(f"股票{stock_code}的high_list: {high_list}")
            # print(f"股票{stock_code}的low_list: {low_list}")
            
            # 生成唯一ID（基于两位小数的数组）
            unique_id = generate_unique_id(high_list, low_list)
            
            # 最终key：rust_stock_group:唯一ID（和Rust端同组）
            key = f"{group_prefix}{unique_id}"
            
            # 存储：key=组前缀+唯一ID，value=股票代码
            r.set(key, stock_code.encode('utf-8'))
            
            success += 1
            
        except Exception as e:
            print(f"{stock_code} 处理失败: {str(e)[:100]}")
            fail += 1

    # 结果统计
    r.close()
    print(f"\n存储完成：成功{success}只，失败{fail}只")
    print(f"所有key格式: {group_prefix}唯一ID（与Rust端同组）")


if __name__ == "__main__":
    tdx_path = r"D:\zd_hbzq"  # 你的通达信路径
    redis_host = "localhost"
    redis_port = 6379
    redis_password = None  # 有密码则填写
    
    # 1. 读取股票数据（已对high/low保留两位小数）
    stock_data = get_all_stock_data(tdx_path, min_records=20)
    print(f"成功读取符合条件的股票共 {len(stock_data)} 只")

    # 2. 存入Redis（与Rust端同组：rust_stock_group:）
    if stock_data:
        save_to_redis(stock_data, redis_host=redis_host, redis_port=redis_port, redis_password=redis_password)