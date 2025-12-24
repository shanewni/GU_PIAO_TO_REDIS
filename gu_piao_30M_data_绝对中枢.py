import logging
import time
import schedule
import numpy as np
import gupiaojichu
from pytdx.hq import TdxHq_API

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class StockSignalAnalyzer:
    def __init__(self, blk_file_path, n=10):
        """
        初始化股票信号分析器
        :param blk_file_path: 股票列表文件路径
        :param n: NEAR_DOWN条件中的K线数量阈值
        """
        self.blk_file_path = blk_file_path
        self.stock_list = self.load_stock_list()
        self.n = n  # NEAR_DOWN参数
        self.triggered_stocks = set()  # 新增：用于记录已触发的股票代码
        self.servers = [
            ('152.136.167.10', 7709),
            ('36.153.42.16', 7709)
        ]
        logging.info(f"初始化完成，共加载 {len(self.stock_list)} 只股票，N参数: {self.n}")

    def load_stock_list(self):
        """加载股票列表，格式参考原有逻辑"""
        stock_list = []
        try:
            with open(self.blk_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line in lines[1:]:  # 跳过首行
                line = line.strip()
                if line:
                    market_code = line[0]
                    stock_code = line[1:7]
                    market = 0 if market_code == '0' else 1  # 0:深圳, 1:上海
                    market_prefix = 'sz' if market == 0 else 'sh'
                    full_code = f"{market_prefix}{stock_code}"
                    stock_list.append((market, stock_code, full_code))
        except Exception as e:
            logging.error(f"读取股票列表失败: {e}")
        return stock_list

    def get_kline_data(self, market, stock_code, full_code):
        """获取K线数据（包含高开低收等信息）"""
        api = TdxHq_API()
        for server_ip, server_port in self.servers:
            try:
                if api.connect(server_ip, server_port):
                    # 获取5分钟K线数据(200条)，category=9代表5分钟线
                    data = api.get_security_bars(2, market, stock_code, 0, 200)
                    api.disconnect()
                    
                    if data:
                        # 整理数据为字典列表
                        kline_data = []
                        for bar in data:
                            kline_data.append({
                                'open': float(bar['open']),
                                'high': float(bar['high']),
                                'low': float(bar['low']),
                                'close': float(bar['close']),
                                'datetime': bar['datetime']
                            })
                        return kline_data
                    else:
                        logging.warning(f"{full_code} 未获取到K线数据")
                        return None
                else:
                    logging.debug(f"连接服务器 {server_ip}:{server_port} 失败")
            except Exception as e:
                logging.debug(f"获取 {full_code} 数据出错: {e}")
                continue
        logging.error(f"所有服务器均无法获取 {full_code} 数据")
        return None

    def three_buy2(self, frac, high, low):
        """对应通达信TDXDLL1(6,...)，即ThreeBuy2函数"""
        data_len = len(frac)
        if len(high) != data_len or len(low) != data_len:
            raise ValueError("输入序列长度必须一致")
        
        pf_out = [0.0] * data_len
        if data_len <= 0:
            return pf_out

        # 提取转折点
        turn_points = []
        for i in range(data_len):
            val = frac[i]
            if val != 0.0:
                turn_points.append((i, int(val)))

        # 构建线段
        segments = []
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

        # 筛选向下线段
        down_segments = [
            (s, e) for s, e, d in segments if d == 1
        ]
        down_segments.sort(key=lambda x: -x[1])  # 按结束位置降序

        if len(down_segments) < 4:
            return pf_out

        # 取最近4个向下线段
        seg1, seg2, seg3 = down_segments[0], down_segments[1], down_segments[2]

        # 判断线段重合
        def segments_overlap(a, b):
            a_high = high[a[0]]
            a_low = low[a[1]]
            b_high = high[b[0]]
            b_low = low[b[1]]
            return min(a_high,b_high) > max(a_low,b_low)

        seg2_seg3 = segments_overlap(seg2, seg3)
        all_overlap = seg2_seg3

        # 线段低点
        seg1_low = low[seg1[1]]
        seg2_high = high[seg2[0]]
        seg3_high = high[seg3[0]]

        # 判断输出条件

        should_output = seg1_low > seg2_high or seg1_low > seg3_high

        # 最后线段方向判断
        if should_output and all_overlap and segments and segments[-1][2] == 1:
            if seg1[1] < data_len:
                pf_out[seg1[1]] = 1.0
            if seg1[0] < data_len:
                pf_out[seg1[0]] = -1.0
        return pf_out

    def calculate_buy_signal(self, kline_data):
        """计算买入信号，对应通达信公式逻辑"""
        if not kline_data or len(kline_data) < 20:
            return False, None

        # 提取所需数据序列
        high = [k['high'] for k in kline_data]
        low = [k['low'] for k in kline_data]
        close = [k['close'] for k in kline_data]
        data_len = len(high)

        # 计算TURNS和TURNS1
        turns = gupiaojichu.identify_turns(data_len,high, low)
        turns1 = self.three_buy2(turns, high, low)

        # 寻找TURNS1中值为1的位置
        has_one = sum(1 for val in turns1 if val == 1.0) > 0
        # one_pos = -1
        # for i in range(data_len-1, -1, -1):
        #     if turns1[i] == 1.0:
        #         one_pos = data_len - 1 - i  # 距离当前K线的位置
        #         break

        # # 计算ZG_VALUE
        # zg_value = 0.0
        # if has_one and one_pos >= 0:
        #     ref_idx = data_len - 1 - one_pos
        #     if ref_idx >= 0:
        #         zg_value = high[ref_idx]

        # # 判断NEAR_DOWN条件
        # near_down = has_one and (one_pos <= self.n)

        # # 计算买入信号基础条件
        # buy_signal = close[-1] > zg_value and zg_value > 0 and near_down

        # # 处理TURNS1中值为-1的位置
        # has_onef = sum(1 for val in turns1 if val == -1.0) > 0
        # one_posf = -1
        # for i in range(data_len-1, -1, -1):
        #     if turns1[i] == -1.0:
        #         one_posf = data_len - 1 - i
        #         break

        # # 计算ZG_VALUEF和HHV
        # zg_valuef = 0.0
        # if has_onef and one_posf >= 0:
        #     ref_idx_f = data_len - 1 - one_posf
        #     if ref_idx_f >= 0:
        #         zg_valuef = high[ref_idx_f]

        # # 计算区间最高价
        # hh = 0.0
        # if one_pos > 0 and (data_len - 1 - one_pos) >= 0:
        #     hh = max(high[data_len - 1 - one_pos : data_len])

        # # 最终买入信号
        # final_signal = buy_signal and (hh > zg_valuef) if hh != 0 else False
        final_signal = has_one
        zg_value=0
        return final_signal, zg_value
    
    def write_to_blk_files(self, market, stock_code):
        """将股票代码写入两个blk文件"""
        # 组合格式：market(1位) + stock_code(6位)
        blk_code = f"{market}{stock_code}"
        # 目标文件路径
        file_paths = [
            r"D:\zd_hbzq\T0002\blocknew\BSMJB.blk",
            # r"D:\new_tdx\T0002\blocknew\QBGRX.blk",
            # r"D:\zd_hbzq\T0002\blocknew\zxg.blk",
            # r"D:\new_tdx\T0002\blocknew\zxg.blk"
        ]
        
        for file_path in file_paths:
            try:
                # 以追加模式写入，确保文件存在（不存在则创建）
                with open(file_path, 'a', encoding='utf-8') as f:
                    f.write(f"{blk_code}\n")
                logging.info(f"成功将 {blk_code} 写入 {file_path}")
            except Exception as e:
                logging.error(f"写入文件 {file_path} 失败: {e}")

    def check_all_stocks(self):
        """检查所有股票的买入信号"""
        logging.info("开始检查所有股票信号...")
        signal_count = 0
        
        for market, stock_code, full_code in self.stock_list:
            try:
                kline_data = self.get_kline_data(market, stock_code, full_code)
                if not kline_data:
                    continue

                signal, zg_value = self.calculate_buy_signal(kline_data)
                if signal and full_code not in self.triggered_stocks:
                    signal_count += 1
                    self.write_to_blk_files(market, stock_code)
                    self.triggered_stocks.add(full_code)  # 记录已触发的股票
                    logging.warning(f"股票 {stock_code} 出现买入信号! ZG_VALUE: {zg_value:.2f}")
                    
            except Exception as e:
                logging.error(f"处理 {full_code} 时出错: {e}")

        logging.info(f"信号检查完成，共发现 {signal_count} 个买入信号")
        return signal_count

    def run(self, interval_seconds=30):
        """运行分析器，定时检查信号"""
        logging.info(f"开始定时信号检查，间隔: {interval_seconds}秒")
        self.check_all_stocks()  # 立即执行一次
        
        schedule.every(interval_seconds).seconds.do(self.check_all_stocks)
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("程序被用户中断")
        except Exception as e:
            logging.error(f"程序运行出错: {e}")

if __name__ == "__main__":
    # 示例用法
    blk_file_path = r"D:\zd_hbzq\T0002\blocknew\ZFG.blk"
    analyzer = StockSignalAnalyzer(blk_file_path, n=10)
    analyzer.run(interval_seconds=30)