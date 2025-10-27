import pandas as pd
from pytdx.hq import TdxHq_API
import redis
import time
import schedule
import os
from datetime import datetime
import gupiaojichu
import logging
import json
import winsound

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_data_1.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

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
    if len(down_segments) < 4:
        return pf_out
    
    # 取最近的3个向下线段
    latest_seg = down_segments[0]    # 最近的向下线段
    prev_seg = down_segments[1]      # 前一个向下线段
    prev2_seg = down_segments[2]     # 前两个向下线段
    
    # 4. 条件1：低点依次降低（前2线段低点 > 前1线段低点 > 最近线段低点）
    latest_low = low[latest_seg[1]]    # 最近线段终点（低点）的价格
    prev_low = low[prev_seg[1]]        # 前一线段终点（低点）的价格
    prev2_low = low[prev2_seg[1]]      # 前两线段终点（低点）的价格

    latest_high = high[latest_seg[0]]    # 最近线段起点（高点）的价格
    prev_high = high[prev_seg[0]]        # 前一线段起点（高点）的价格
    prev2_high = high[prev2_seg[0]]      # 前两线段起点（高点）的价格
    

    if prev_low >= prev2_low or prev_high > prev2_high:
        return pf_out  # 最近线段低点不低于前一线段，不满足
    
    if latest_high < prev_high or latest_high < prev2_high:
        return pf_out  # 最近线段高点不高于前一线段，不满足
    
    if latest_low <= prev_low:
        return pf_out  # 最近线段低点不低于前一线段，不满足
    
    # 5. 条件2：最后一根K线价格突破最近两个向下线段的起点高点之一
    last_k_idx = data_len - 1  # 最后一根K线的索引
    
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
    
    def write_to_blk_files(self, market, stock_code):
        """将股票代码写入两个blk文件"""
        # 组合格式：market(1位) + stock_code(6位)
        blk_code = f"{market}{stock_code}"
        # 目标文件路径
        file_paths = [
            r"D:\zd_hbzq\T0002\blocknew\QBGRX.blk",
            r"D:\new_tdx\T0002\blocknew\QBGRX.blk",
            r"D:\zd_hbzq\T0002\blocknew\zxg.blk",
            r"D:\new_tdx\T0002\blocknew\zxg.blk"
        ]
        
        for file_path in file_paths:
            try:
                # 以追加模式写入，确保文件存在（不存在则创建）
                with open(file_path, 'a', encoding='utf-8') as f:
                    f.write(f"{blk_code}\n")
                logging.info(f"成功将 {blk_code} 写入 {file_path}")
            except Exception as e:
                logging.error(f"写入文件 {file_path} 失败: {e}")

    def update_all_stocks(self):
        """
        更新所有股票数据到Redis
        """
        self.stock_list = self.load_stock_list()
        success_count = 0
        fail_count = 0
        
        logging.info("开始更新所有股票数据...")
        
        for market, stock_code, full_code in self.stock_list:
            try:
                # 获取股票数据（200条）
                stock_data,stock_data_high,stock_data_low = self.get_5min_data(market, stock_code, full_code)
                
                if stock_data:
                    data_len = len(stock_data_high)
                    stock_data_frac =gupiaojichu.identify_turns(data_len,stock_data_high,stock_data_low)
                    ok = three_buy_variant(stock_data_frac,stock_data_high,stock_data_low)
                    # ok = identify_three_buy_variant(stock_data_high,stock_data_low)
                    if  ok[-1] ==1.0 and full_code not in self.triggered_stocks:
                        self.write_to_blk_files(market, stock_code)
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
    BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\BSMJB.blk"
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