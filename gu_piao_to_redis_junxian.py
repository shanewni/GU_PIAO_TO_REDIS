import redis
import re  # 用于正则处理数组格式

def connect_redis():
    """连接到Redis的db4数据库"""
    try:
        r = redis.Redis(
            host='localhost',  # 替换为实际Redis主机
            port=6379,         # 替换为实际Redis端口
            db=4,
            decode_responses=True,  # 自动解码为字符串
            socket_connect_timeout=5
        )
        r.ping()  # 测试连接
        return r
    except Exception as e:
        print(f"Redis连接失败: {e}")
        return None

def process_stock_group(r):
    """处理rust_stock_group数据并计算统计结果（修复数组解析问题）"""
    # 1. 获取所有rust_stock_group:前缀的键
    print("正在读取rust_stock_group:相关数据...")
    keys = r.keys('rust_stock_group:*')
    if not keys:
        print("未找到rust_stock_group:相关的键")
        return

    # 2. 解析所有值为整数数组（处理JSON格式数组，过滤无效数据）
    valid_arrays = []
    for key in keys:
        val_str = r.get(key)
        if not val_str:
            print(f"键 {key} 的值为空，已跳过")
            continue
        
        try:
            # 核心修复：处理带方括号的JSON格式数组（如"[1, 2, 3]"）
            # 步骤1：去除首尾的方括号
            cleaned_str = val_str.strip('[]')
            # 步骤2：去除所有空格（处理"1, 2, 3"这类带空格的情况）
            cleaned_str = re.sub(r'\s+', '', cleaned_str)
            # 步骤3：按逗号分割为元素（空数组处理：避免split后出现空字符串）
            if not cleaned_str:
                print(f"键 {key} 的值为空数组，已跳过")
                continue
            elements = cleaned_str.split(',')
            # 步骤4：转换为整数数组并验证范围（1-9）
            arr = list(map(int, elements))
            if all(1 <= x <= 9 for x in arr):
                valid_arrays.append(arr)
            else:
                print(f"键 {key} 包含无效值（需1-9），已跳过")
        except ValueError:
            print(f"键 {key} 格式错误（非整数数组），已跳过")
        except Exception as e:
            print(f"键 {key} 解析出错: {e}，已跳过")

    if not valid_arrays:
        print("没有有效的数组数据可处理")
        return

    # 3. 计算最大数组长度（确定统计维度）
    max_len = max(len(arr) for arr in valid_arrays)
    print(f"最大数组长度: {max_len}，将从后往前统计{max_len}个位置")

    # 4. 初始化统计结果（1-9每个值对应一个统计数组）
    result = {x: [0] * max_len for x in range(1, 10)}

    # 5. 从后往前统计每个位置的数值出现次数
    for pos in range(max_len):  # pos=0对应倒数第1位，pos=1对应倒数第2位...
        count = {x: 0 for x in range(1, 10)}
        
        for arr in valid_arrays:
            # 检查数组是否足够长（当前位置是否有值）
            if len(arr) > pos:
                # 获取当前位置的数值（从后往前数第pos+1个）
                val = arr[len(arr) - 1 - pos]
                count[val] += 1
        
        # 更新结果数组
        for x in range(1, 10):
            result[x][pos] = count[x]

    # 6. 将结果存入Redis（格式：逗号分隔字符串，便于后续解析）
    print("正在保存统计结果到Redis...")
    for x in range(1, 10):
        val_str = ','.join(map(str, result[x]))
        r.set(f"junxian:val{x}", val_str)
        print(f"已存储: junxian:val{x}（长度: {len(result[x])}）")
    
    print("\n处理完成！结果已存入Redis，键格式为junxian:val1~junxian:val9")

def main():
    r = connect_redis()
    if not r:
        return
    process_stock_group(r)

if __name__ == "__main__":
    main()