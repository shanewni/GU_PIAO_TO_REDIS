import os
import struct
import redis
import re
from collections import defaultdict

# 配置参数
TDX_PATH = r"D:\zd_hbzq"  # 通达信安装路径
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 4
RUST_STOCK_GROUP_PREFIX = "rust_stock_group:"
STOCK_GROUP_PREFIX = "stock_group:"
HANGYE_PREFIX = "hangye:val"

def connect_redis():
    """连接到Redis数据库"""
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_connect_timeout=5
        )
        r.ping()
        print("成功连接到Redis")
        return r
    except Exception as e:
        print(f"Redis连接失败: {e}")
        return None

def get_industry_stocks(tdx_path):
    """从通达信获取56个行业板块及其成分股（修正.blk文件解析逻辑）"""
    industry_stocks = {}
    block_dir = os.path.join(tdx_path, "T0002", "blocknew")
    
    if not os.path.exists(block_dir):
        print(f"板块目录不存在: {block_dir}")
        return industry_stocks
    
    # 筛选行业板块文件（.blk后缀，通常包含"行业"关键词）
    blk_files = [f for f in os.listdir(block_dir) if f.endswith(".blk") and "880" in f]
    
    for file_name in blk_files:
        try:
            file_path = os.path.join(block_dir, file_name)
            # 按文本模式打开，通达信文件通常用gbk编码
            with open(file_path, 'r', encoding='gbk', errors='ignore') as f:
                lines = f.readlines()  # 读取所有行
            
            # 跳过第一行空行（根据您的描述，第一行为空）
            if len(lines) < 2:
                print(f"文件 {file_name} 内容过短，跳过")
                continue
            stock_lines = lines[1:]  # 从第二行开始才是股票代码
            
            stocks = []
            for line in stock_lines:
                line = line.strip()  # 去除换行符、空格等
                if not line:  # 跳过空行
                    continue
                # 验证是否为7位数字
                if len(line) == 7 and line.isdigit():
                    # 去除第一位，保留后6位
                    code = line[1:]  # 例如"0600000" → "600000"，"1000001" → "000001"
                    stocks.append(code)
                else:
                    print(f"文件 {file_name} 中存在非7位数字行: {line}，已跳过")
            
            # 提取行业名称（去除文件名中的数字前缀和扩展名）
            industry_name = re.sub(r'^\s+', '', os.path.splitext(file_name)[0]).strip()
            if stocks:
                industry_stocks[industry_name] = stocks
                print(f"加载行业: {industry_name}, 成分股数量: {len(stocks)}")
            else:
                print(f"行业 {industry_name} 未提取到有效股票代码")
        
        except Exception as e:
            print(f"解析板块文件 {file_name} 失败: {e}")
    
    
    print(f"共加载 {len(industry_stocks)} 个行业板块")
    return industry_stocks

def get_stock_data(r):
    """从Redis获取股票数据"""
    # 获取所有rust_stock_group:前缀的键
    rust_keys = r.keys(f"{RUST_STOCK_GROUP_PREFIX}*")
    if not rust_keys:
        print(f"未找到{RUST_STOCK_GROUP_PREFIX}相关的键")
        return {}
    
    stock_data = {}
    print(f"开始读取 {len(rust_keys)} 个股票数据...")
    
    for rust_key in rust_keys:
        try:
            # 获取股票的val数组
            val_str = r.get(rust_key)
            if not val_str:
                continue
            
            # 解析val数组(处理带方括号的情况)
            cleaned_str = val_str.strip('[]')
            cleaned_str = re.sub(r'\s+', '', cleaned_str)
            if not cleaned_str:
                continue
                
            val_array = list(map(int, cleaned_str.split(',')))
            
            # 获取对应的股票代码
            # 从rust_stock_group:xxx提取唯一ID部分
            unique_id = rust_key[len(RUST_STOCK_GROUP_PREFIX):]
            stock_code_key = f"{STOCK_GROUP_PREFIX}{unique_id}"
            stock_code = r.get(stock_code_key)
            
            if stock_code:
                # 提取6位核心代码
                core_code = re.sub(r'\D', '', stock_code)[:6]
                stock_data[core_code] = val_array
        except Exception as e:
            print(f"处理键 {rust_key} 失败: {e}")
            continue
    
    print(f"成功加载 {len(stock_data)} 个股票的val数据")
    return stock_data

def classify_by_industry(industry_stocks, stock_data):
    """按行业分类股票数据"""
    industry_data = defaultdict(list)
    
    # 构建股票到行业的映射
    stock_to_industry = {}
    for industry, stocks in industry_stocks.items():
        for stock in stocks:
            stock_to_industry[stock] = industry
    
    # 按行业分组
    for stock_code, val_array in stock_data.items():
        if stock_code in stock_to_industry:
            industry = stock_to_industry[stock_code]
            industry_data[industry].append(val_array)
    
    print(f"按行业分类完成，共 {len(industry_data)} 个行业有数据")
    return industry_data

def calculate_industry_average(industry_data):
    """计算每个行业的平均值"""
    industry_averages = {}
    
    for industry, arrays in industry_data.items():
        try:
            # 找到最长数组的长度
            max_length = max(len(arr) for arr in arrays) if arrays else 0
            if max_length == 0:
                continue
            
            # 从后往前计算每个位置的平均值
            average_array = []
            for pos in range(max_length):  # pos=0对应最后一个元素
                total = 0
                count = 0
                
                for arr in arrays:
                    # 从后往前取元素，不足的补0
                    if len(arr) > pos:
                        # 倒数第pos+1个元素
                        val = arr[len(arr) - 1 - pos]
                        total += val
                    count += 1  # 即使数组较短也要计入数量
                
                # 计算平均值并保留1位小数
                avg = round(total / count, 1)
                average_array.append(avg)
            
            # 结果数组需要反转回来，因为我们是从后往前计算的
            average_array.reverse()
            industry_averages[industry] = average_array
            print(f"计算完成: {industry}, 平均数组长度: {len(average_array)}")
        except Exception as e:
            print(f"计算行业 {industry} 平均值失败: {e}")
            continue
    
    return industry_averages

def save_to_redis(r, industry_averages):
    """将计算结果保存到Redis"""
    if not industry_averages:
        print("没有计算结果可保存")
        return
    
    for industry, avg_array in industry_averages.items():
        try:
            # 将数组转换为逗号分隔的字符串
            val_str = ','.join(map(str, avg_array))
            key = f"{HANGYE_PREFIX}{industry}"
            r.set(key, val_str)
            print(f"已保存: {key}, 长度: {len(avg_array)}")
        except Exception as e:
            print(f"保存行业 {industry} 结果失败: {e}")
            continue

def main():
    # 连接Redis
    r = connect_redis()
    if not r:
        return
    
    # 1. 获取行业板块及其成分股
    industry_stocks = get_industry_stocks(TDX_PATH)
    if not industry_stocks:
        print("未能获取行业板块数据，程序退出")
        return
    
    # 2. 从Redis获取股票数据
    stock_data = get_stock_data(r)
    if not stock_data:
        print("未能获取股票数据，程序退出")
        return
    
    # 3. 按行业分类股票数据
    industry_data = classify_by_industry(industry_stocks, stock_data)
    if not industry_data:
        print("未能按行业分类数据，程序退出")
        return
    
    # 4. 计算每个行业的平均值
    industry_averages = calculate_industry_average(industry_data)
    if not industry_averages:
        print("未能计算行业平均值，程序退出")
        return
    
    # 5. 保存结果到Redis
    save_to_redis(r, industry_averages)
    
    print("所有操作完成")

if __name__ == "__main__":
    main()