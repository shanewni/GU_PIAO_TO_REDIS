import pandas as pd
from pytdx.hq import TdxHq_API
import redis
import time
import schedule
import os
from datetime import datetime
import logging
import json
import winsound

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_data.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def identify_turns(high, low):
    """
    识别价格转折点（高点和低点）
    
    参数:
        high: 最高价序列（列表或类似可索引对象）
        low: 最低价序列（列表或类似可索引对象）
    
    返回:
        list: 转折点标记列表，1.0表示高点，-1.0表示低点，0.0表示无转折
    """
    data_len = len(high)
    # 确保high和low长度一致
    if len(low) != data_len:
        raise ValueError("high和low必须具有相同的长度")
    
    # 数据量不足时直接返回全0
    if data_len <= 6:
        return [0.0] * data_len
    
    turns = []  # 存储候选转折点 (索引, 类型)，1为高点，-1为低点
    
    # 处理中间部分数据（前后都有足够数据）
    for i in range(4, data_len - 4):
        current_high = high[i]
        current_low = low[i]
        
        # 计算前后4根K线的最高值（不含当前）
        max_prev_next_high = float('-inf')
        # 前4根（i-4到i-1）
        for j in range(i - 4, i):
            max_prev_next_high = max(max_prev_next_high, high[j])
        # 后4根（i+1到i+4）
        for j in range(i + 1, i + 5):
            max_prev_next_high = max(max_prev_next_high, high[j])
        
        # 计算前后4根K线的最低值（不含当前）
        min_prev_next_low = float('inf')
        # 前4根（i-4到i-1）
        for j in range(i - 4, i):
            min_prev_next_low = min(min_prev_next_low, low[j])
        # 后4根（i+1到i+4）
        for j in range(i + 1, i + 5):
            min_prev_next_low = min(min_prev_next_low, low[j])
        
        # 判断是否为高点
        if current_high >= max_prev_next_high:
            turns.append((i, 1))
        # 判断是否为低点
        if current_low <= min_prev_next_low:
            turns.append((i, -1))
    
    # 处理最后4根K线（后面没有足够数据）
    start = max(0, data_len - 4)
    for i in range(start, data_len):
        current_high = high[i]
        current_low = low[i]
        
        # 只检查前4根数据
        start_prev = max(0, i - 4)
        max_prev_high = float('-inf')
        min_prev_low = float('inf')
        
        for j in range(start_prev, i):
            max_prev_high = max(max_prev_high, high[j])
            min_prev_low = min(min_prev_low, low[j])
        
        # 判断是否为高点（只需大于前4根最高）
        if current_high >= max_prev_high:
            turns.append((i, 1))
        # 判断是否为低点（只需小于前4根最低）
        if current_low <= min_prev_low:
            turns.append((i, -1))
    
    # 合并连续相同类型的转折点，保留最优值（高点保留最高，低点保留最低）
    new_turns = []
    i = 0
    while i < len(turns):
        current_index, current_type = turns[i]
        current_high_val = high[current_index]
        current_low_val = low[current_index]
        
        j = i + 1
        while j < len(turns) and turns[j][1] == current_type:
            j_index = turns[j][0]
            if current_type == 1:  # 高点，保留更高的
                if high[j_index] > current_high_val:
                    current_index = j_index
                    current_high_val = high[j_index]
            else:  # 低点，保留更低的
                if low[j_index] < current_low_val:
                    current_index = j_index
                    current_low_val = low[j_index]
            j += 1
        
        new_turns.append((current_index, current_type))
        i = j
    
    # 转折点数量不足时返回全0
    if len(new_turns) <= 3:
        return [0.0] * data_len
    
    # 验证转折点的交替性（高-低-高 或 低-高-低）
    confirmed_turns = []
    for i in range(len(new_turns) - 1):
        idx1, t1 = new_turns[i]
        idx2, t2 = new_turns[i + 1]
        if (t1 == 1 and t2 == -1) or (t1 == -1 and t2 == 1):
            confirmed_turns.append((idx1, t1))
    
    # 构建输出结果
    pf_out = [0.0] * data_len
    for index, frac in confirmed_turns:
        if index < data_len:
            pf_out[index] = float(frac)
    
    return pf_out

def three_buy_variant(frac, high, low):
    """
    识别三买变体信号
    
    参数:
        frac: 转折点标记列表（1.0为高点，-1.0为低点，0.0无转折）
        high: 最高价序列
        low: 最低价序列
    
    返回:
        list: 信号列表，1.0表示存在三买变体信号，0.0表示无
    """
    data_len = len(high)
    # 确保输入数组长度一致
    if len(low) != data_len or len(frac) != data_len:
        raise ValueError("frac、high、low必须具有相同的长度")
    
    # 初始化输出数组为0
    pf_out = [0.0] * data_len
    
    if data_len <= 0:
        return pf_out
    
    # 1. 提取所有转折点 (索引, 类型)，类型为1（高点）或-1（低点）
    turn_points = []
    for i in range(data_len):
        val = frac[i]
        if val != 0.0:
            turn_points.append((i, int(val)))
    
    # 2. 构建线段（确保转折点方向交替）
    segments = []  # 元素为 (起点索引, 终点索引, 方向)，方向为起点的类型（1或-1）
    i = 0
    while i < len(turn_points):
        idx1, dir1 = turn_points[i]
        found = False
        # 寻找下一个相反方向的转折点
        for j in range(i + 1, len(turn_points)):
            idx2, dir2 = turn_points[j]
            if dir2 == -dir1:  # 方向相反
                segments.append((idx1, idx2, dir1))
                i = j  # 跳到下一个线段的起点
                found = True
                break
        if not found:
            break  # 找不到相反方向的转折点，终止构建
    
    # 3. 筛选向下线段（方向为1：从高点到低点）
    down_segments = []
    for seg in segments:
        start_idx, end_idx, direction = seg
        if direction == 1:  # 高点到低点，属于向下线段
            down_segments.append((start_idx, end_idx))
    
    # 按线段结束位置从近到远排序（最近的在前面）
    down_segments.sort(key=lambda x: -x[1])  # 按end_idx降序排列
    
    # 至少需要3个向下线段才可能形成信号
    if len(down_segments) < 3:
        return pf_out
    
    # 取最近的3个向下线段
    latest_seg = down_segments[0]    # 最近的向下线段
    prev_seg = down_segments[1]      # 前一个向下线段
    prev2_seg = down_segments[2]     # 前两个向下线段
    
    # 4. 条件1：低点依次降低（前2线段低点 > 前1线段低点 > 最近线段低点）
    latest_low = low[latest_seg[1]]    # 最近线段终点（低点）的价格
    prev_low = low[prev_seg[1]]        # 前一线段终点（低点）的价格
    prev2_low = low[prev2_seg[1]]      # 前两线段终点（低点）的价格
    
    # 检查低点是否依次降低
    if prev_low >= prev2_low:
        return pf_out  # 前一线段低点不低于前两线段，不满足
    if latest_low >= prev_low:
        return pf_out  # 最近线段低点不低于前一线段，不满足
    
    # 5. 条件2：最后一根K线价格突破最近两个向下线段的起点高点之一
    last_k_idx = data_len - 1  # 最后一根K线的索引
    last_k_price = high[last_k_idx]  # 最后一根K线的最高价（保持原逻辑）
    
    latest_seg_high = high[latest_seg[0]]  # 最近向下线段起点（高点）的价格
    prev_seg_high = high[prev_seg[0]]      # 前一向下线段起点（高点）的价格
    
    # 检查是否突破至少一个高点
    if last_k_price <= latest_seg_high and last_k_price <= prev_seg_high:
        return pf_out  # 未突破任何高点，不满足
    
    # 所有条件满足，在最后一根K线标记信号
    pf_out[last_k_idx] = 1.0
    
    return pf_out

def identify_three_buy_variant(high, low):
    """
    从高低点序列识别三买变体信号
    
    参数:
        high: 最高价序列（列表或类似可索引对象）
        low: 最低价序列（列表或类似可索引对象）
    
    返回:
        list: 信号列表，1.0表示存在三买变体信号，0.0表示无
    """
    data_len = len(high)
    # 确保输入序列长度一致
    if len(low) != data_len:
        raise ValueError("high和low必须具有相同的长度")
    
    # 初始化输出信号为全0
    pf_out = [0.0] * data_len
    
    # 数据量不足时直接返回
    if data_len <= 6:
        return pf_out
    
    # --------------------------
    # 第一步：识别转折点（整合identify_turns逻辑）
    # --------------------------
    turns = []  # 候选转折点 (索引, 类型)，1为高点，-1为低点
    
    # 处理中间部分数据（前后均有4根K线）
    for i in range(4, data_len - 4):
        current_high = high[i]
        current_low = low[i]
        
        # 计算前后4根K线的最高值（不含当前）
        max_prev_next_high = float('-inf')
        for j in range(i - 4, i):
            max_prev_next_high = max(max_prev_next_high, high[j])
        for j in range(i + 1, i + 5):
            max_prev_next_high = max(max_prev_next_high, high[j])
        
        # 计算前后4根K线的最低值（不含当前）
        min_prev_next_low = float('inf')
        for j in range(i - 4, i):
            min_prev_next_low = min(min_prev_next_low, low[j])
        for j in range(i + 1, i + 5):
            min_prev_next_low = min(min_prev_next_low, low[j])
        
        # 判断高点/低点
        if current_high >= max_prev_next_high:
            turns.append((i, 1))
        if current_low <= min_prev_next_low:
            turns.append((i, -1))
    
    # 处理最后4根K线（后面无足够数据）
    start = max(0, data_len - 4)
    for i in range(start, data_len):
        current_high = high[i]
        current_low = low[i]
        
        # 只检查前4根数据
        start_prev = max(0, i - 4)
        max_prev_high = float('-inf')
        min_prev_low = float('inf')
        for j in range(start_prev, i):
            max_prev_high = max(max_prev_high, high[j])
            min_prev_low = min(min_prev_low, low[j])
        
        # 判断高点/低点
        if current_high >= max_prev_high:
            turns.append((i, 1))
        if current_low <= min_prev_low:
            turns.append((i, -1))
    
    # 合并连续相同类型的转折点（保留最优值）
    new_turns = []
    i = 0
    while i < len(turns):
        current_index, current_type = turns[i]
        current_high_val = high[current_index]
        current_low_val = low[current_index]
        
        j = i + 1
        while j < len(turns) and turns[j][1] == current_type:
            j_index = turns[j][0]
            if current_type == 1 and high[j_index] > current_high_val:
                current_index = j_index
                current_high_val = high[j_index]
            elif current_type == -1 and low[j_index] < current_low_val:
                current_index = j_index
                current_low_val = low[j_index]
            j += 1
        
        new_turns.append((current_index, current_type))
        i = j
    
    # 转折点数量不足时返回
    if len(new_turns) <= 3:
        return pf_out
    
    # 验证转折点交替性，生成最终转折点列表（frac）
    frac = [0.0] * data_len
    for i in range(len(new_turns) - 1):
        idx1, t1 = new_turns[i]
        idx2, t2 = new_turns[i + 1]
        if (t1 == 1 and t2 == -1) or (t1 == -1 and t2 == 1):
            if idx1 < data_len:
                frac[idx1] = float(t1)
    
    # --------------------------
    # 第二步：识别三买变体信号（整合three_buy_variant逻辑）
    # --------------------------
    # 提取所有转折点
    turn_points = []
    for i in range(data_len):
        val = frac[i]
        if val != 0.0:
            turn_points.append((i, int(val)))
    
    # 构建交替线段
    segments = []  # (起点索引, 终点索引, 方向)
    i = 0
    while i < len(turn_points):
        idx1, dir1 = turn_points[i]
        found = False
        for j in range(i + 1, len(turn_points)):
            idx2, dir2 = turn_points[j]
            if dir2 == -dir1:
                segments.append((idx1, idx2, dir1))
                i = j
                found = True
                break
        if not found:
            break
    
    # 筛选向下线段（方向为1：高点到低点）
    down_segments = [
        (start, end) for start, end, dir in segments
        if dir == 1
    ]
    
    # 按结束位置从近到远排序
    down_segments.sort(key=lambda x: -x[1])
    
    # 至少需要3个向下线段
    if len(down_segments) < 3:
        return pf_out
    
    # 取最近的3个向下线段
    latest_seg, prev_seg, prev2_seg = down_segments[0], down_segments[1], down_segments[2]
    
    # 条件1：低点依次降低
    latest_low = low[latest_seg[1]]
    prev_low = low[prev_seg[1]]
    prev2_low = low[prev2_seg[1]]
    if prev_low >= prev2_low or latest_low >= prev_low:
        return pf_out
    
    # 条件2：最后一根K线突破最近两个向下线段的起点高点之一
    last_k_idx = data_len - 1
    last_k_price = high[last_k_idx]
    latest_seg_high = high[latest_seg[0]]
    prev_seg_high = high[prev_seg[0]]
    if last_k_price <= latest_seg_high and last_k_price <= prev_seg_high:
        return pf_out
    
    # 所有条件满足，标记信号
    pf_out[last_k_idx] = 1.0
    
    return pf_out

class StockDataCollector:
    def __init__(self, blk_file_path):
        """
        初始化股票数据收集器
        
        Args:
            blk_file_path: blk文件路径
            redis_host: Redis主机
            redis_port: Redis端口
            redis_db: Redis数据库编号
        """
        self.blk_file_path = blk_file_path
        self.stock_list = self.load_stock_list()
        self.triggered_stocks = set()  # 新增：用于记录已触发的股票代码
        # 服务器列表
        self.servers = [
            ('152.136.167.10', 7709),
            ('36.153.42.16', 7709)
        ]
        
        logging.info(f"初始化完成，共加载 {len(self.stock_list)} 只股票")
    
    def load_stock_list(self):
        """
        从blk文件加载股票列表
        
        Returns:
            list: 股票代码列表，格式为 [('市场代码', '股票代码', '完整代码'), ...]
        """
        stock_list = []
        
        try:
            with open(self.blk_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
            # 跳过第一行空白，从第二行开始处理
            for line in lines[1:]:
                line = line.strip()
                if line:
                    # 第一位是市场代码，后六位是股票代码
                    market_code = line[0]
                    stock_code = line[1:7]
                    
                    # 将市场代码转换为pytdx需要的格式
                    # 0: 深圳, 1: 上海
                    if market_code == '0':
                        market = 0  # 深圳
                        market_prefix = 'sz'
                    else:
                        market = 1  # 上海
                        market_prefix = 'sh'
                    
                    full_code = f"{market_prefix}{stock_code}"
                    stock_list.append((market, stock_code, full_code))
                    
                    logging.debug(f"加载股票: {full_code}")
                    
        except Exception as e:
            logging.error(f"读取blk文件失败: {e}")
            
        return stock_list
    
    def get_5min_data(self, market, stock_code, full_code):
        """
        获取单只股票的5分钟K线数据（200条）
        
        Args:
            market: 市场代码 (0: 深圳, 1: 上海)
            stock_code: 股票代码
            full_code: 完整股票代码 (如 sh600000)
            
        Returns:
            list or None: 200条K线数据，每条包含open, close, high, low, datetime
        """
        api = TdxHq_API()
        
        # 尝试所有服务器直到成功
        for server_ip, server_port in self.servers:
            try:
                if api.connect(server_ip, server_port):
                    logging.debug(f"成功连接到服务器 {server_ip}:{server_port}，获取 {full_code}")
                    
                    # 获取5分钟K线数据 (category=0)，获取200条
                    data = api.get_security_bars(0, market, stock_code, 0, 200)
                    api.disconnect()
                    
                    if data:
                        # 处理所有200条数据
                        result_list = []
                        result_list_high = []
                        result_list_low = []
                        for bar in data:
                            # 提取需要的字段
                            result = {
                                'open': float(bar['open']),
                                'high': float(bar['high']),
                                'low': float(bar['low']),
                                'close': float(bar['close']),
                                'datetime': bar['datetime']
                            }
                            result_list_high.append(float(bar['high']))
                            result_list_low.append(float(bar['low']))
                            result_list.append(result)
                        
                        # 按时间排序（从旧到新）
                        # result_list.sort(key=lambda x: x['datetime'])
                        
                        logging.debug(f"成功获取 {full_code} 数据: {len(result_list)} 条")
                        return result_list,result_list_high,result_list_low
                    else:
                        logging.warning(f"未获取到 {full_code} 的数据")
                        return None,None,None
                else:
                    logging.debug(f"连接服务器 {server_ip}:{server_port} 失败")
                    
            except Exception as e:
                logging.debug(f"服务器 {server_ip}:{server_port} 获取 {full_code} 出错: {e}")
                continue
                
        logging.error(f"所有服务器都无法获取 {full_code} 的数据")
        return None,None,None
    
    def update_all_stocks(self):
        """
        更新所有股票数据到Redis
        """
        success_count = 0
        fail_count = 0
        
        logging.info("开始更新所有股票数据...")
        
        for market, stock_code, full_code in self.stock_list:
            try:
                # 获取股票数据（200条）
                stock_data,stock_data_high,stock_data_low = self.get_5min_data(market, stock_code, full_code)
                
                if stock_data:
                    
                    # stock_data_frac =identify_turns(stock_data_high,stock_data_low)
                    # ok = three_buy_variant(stock_data_frac,stock_data_high,stock_data_low)
                    ok = identify_three_buy_variant(stock_data_high,stock_data_low)
                    if  ok[-1] ==1.0 and full_code not in self.triggered_stocks:
                        logging.warning(f"强势背驰股票： {stock_code}")
                        winsound.Beep(1000, 500)  # 1000Hz频率，持续500毫秒
                        self.triggered_stocks.add(full_code)  # 记录已触发的股票
                 
                    success_count += 1
                else:
                    fail_count += 1
                    logging.warning(f"获取 {full_code} 数据失败")
                    
            except Exception as e:
                fail_count += 1
                logging.error(f"处理 {full_code} 时发生错误: {e}")
                
        
        logging.info(f"数据更新完成: 成功 {success_count}, 失败 {fail_count}")
        return success_count, fail_count


    
    def get_stock_data_from_redis(self, full_code):
        """
        从Redis获取某只股票的所有数据
        
        Args:
            full_code: 完整股票代码
            
        Returns:
            list: 股票数据列表
        """
        try:
            redis_key = f"shishi:{full_code}"
            data_list = self.redis_client.lrange(redis_key, 0, -1)
            
            result = []
            for data_json in data_list:
                data_item = json.loads(data_json)
                result.append(data_item)
            
            return result
        except Exception as e:
            logging.error(f"从Redis获取 {full_code} 数据失败: {e}")
            return []
    
    def run(self, interval_seconds=2):
        """
        运行数据收集器
        
        Args:
            interval_seconds: 更新间隔（秒）
        """
        
        logging.info(f"开始定时数据收集，间隔: {interval_seconds}秒")
        
        # 立即执行一次
        self.update_all_stocks()
        
        # 设置定时任务
        schedule.every(interval_seconds).seconds.do(self.update_all_stocks)
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("程序被用户中断")
        except Exception as e:
            logging.error(f"程序运行出错: {e}")


def main():
    """
    主函数
    """
    # 配置参数
    BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\fl8.blk"
    UPDATE_INTERVAL = 2       # 更新间隔（秒）
    
    # 检查blk文件是否存在
    if not os.path.exists(BLOB_FILE_PATH):
        logging.error(f"blk文件不存在: {BLOB_FILE_PATH}")
        return
    
    # 创建并运行数据收集器
    collector = StockDataCollector(
        blk_file_path=BLOB_FILE_PATH,
    )
    
    # 运行收集器
    collector.run(interval_seconds=UPDATE_INTERVAL)


if __name__ == "__main__":
    main()