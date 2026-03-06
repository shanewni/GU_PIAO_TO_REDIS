import time
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pytdx.hq import TdxHq_API
from typing import Callable, Dict, List, Tuple
import gupiaojichu
import struct
from mootdx.reader import Reader

# 忽略无关警告
warnings.filterwarnings('ignore')

# 通达信板块文件路径
BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\TEST.blk"
# 本地通达信数据默认路径
DEFAULT_TDX_PATH = r"D:\zd_hbzq"

class TdxStockBacktest:
    """基于pytdx的股票回测框架（支持多周期K线+止损策略+以损定量+三买变体买点）"""
    
    def __init__(self):
        self.api = TdxHq_API()
        self.stock_data = {}  # 改为字典存储多周期数据: {'day': 日线数据, '30min': 30分钟数据}
        self.backtest_result = None  # 存储回测结果
        self.trade_records = None  # 交易记录
        self.stop_loss_price = 0.0  # 止损价格（买入K线的最低点）
        self.in_position = False    # 是否持仓标记
        self.stop_loss_ratio = 0.02 # 新增：总资金止损百分比（默认2%）
        self.risk_per_trade = 0.0   # 新增：单笔交易可承受的最大亏损金额
        # 新增：存储30分钟周期的frac（转折点）数据，用于顶分型判断
        self.all_trades_detail = [] # 新增：存储单股票所有交易明细（用于总笔数汇总）
    
    def connect_tdx(self, ip: str = "152.136.167.10", port: int = 7709) -> bool:
        """
        连接通达信服务器
        :param ip: 通达信服务器IP
        :param port: 端口
        :return: 连接成功返回True，失败返回False
        """
        try:
            self.api.connect(ip, port)
            print(f"成功连接通达信服务器 {ip}:{port}")
            return True
        except Exception as e:
            print(f"连接失败: {e}")
            return False
    def get_exact_tdx_30min(self, symbol, tdx_path=DEFAULT_TDX_PATH):
        """
        从本地通达信数据读取并合成精准30分钟K线
        :param symbol: 股票代码
        :param tdx_path: 通达信安装路径
        :return: 30分钟K线DataFrame
        """
        reader = Reader.factory(market='std', tdxdir=tdx_path)
        df_5m = reader.fzline(symbol=symbol)
        
        if df_5m is None or df_5m.empty:
            print(f"未获取到{symbol}本地5分钟数据")
            return pd.DataFrame()

        # 1. 基础清洗与重命名
        df_5m = df_5m.rename(columns={
            'open': '开盘价', 'high': '最高价', 'low': '最低价',
            'close': '收盘价', 'volume': '成交量', 'amount': '成交额'
        })
        
        # 2. 预处理精度（解决 41.79999 问题）
        price_cols = ['开盘价', '最高价', '最低价', '收盘价']
        df_5m[price_cols] = df_5m[price_cols].round(2)

        # 3. 核心合成逻辑：对齐通达信的 11:30 和 15:00
        df_30m = df_5m.resample(
            '30Min', 
            closed='right',   # 每根K线包含右侧边界（如 10:00 包含 10:00 那一分钟）
            label='right',    # 用右侧时间命名（显示为 10:00）
            origin='start_day' # 从每日 00:00 开始计算偏移
        ).agg({
            '开盘价': 'first',
            '最高价': 'max',
            '最低价': 'min',
            '收盘价': 'last',
            '成交量': 'sum',
            '成交额': 'sum'
        })

        # 4. 剔除中午休市和非交易时段的空行
        df_30m = df_30m.dropna(subset=['开盘价'])
        
        # 过滤掉非正常的分钟点
        valid_times = ['10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30', '15:00']
        df_30m = df_30m[df_30m.index.strftime('%H:%M').isin(valid_times)]

        # 5. 最后修正一遍精度
        df_30m[price_cols] = df_30m[price_cols].round(2)
        df_30m.index.name = 'datetime'
        print(f"成功从本地读取 {symbol} 30分钟数据，共 {len(df_30m)} 条")
        return df_30m
    
    def get_local_day_data(self, symbol, tdx_path=DEFAULT_TDX_PATH):
        """
        从本地通达信数据读取日线
        :param symbol: 股票代码
        :param tdx_path: 通达信安装路径
        :return: 日线DataFrame
        """
        reader = Reader.factory(market='std', tdxdir=tdx_path)
        df_day = reader.daily(symbol=symbol)
        
        if df_day is None or df_day.empty:
            print(f"未获取到{symbol}本地日线数据")
            return pd.DataFrame()
        
        # 数据清洗与重命名
        df_day = df_day.rename(columns={
            'open': '开盘价', 'high': '最高价', 'low': '最低价',
            'close': '收盘价', 'volume': '成交量', 'amount': '成交额'
        })
        
        # 精度修正
        price_cols = ['开盘价', '最高价', '最低价', '收盘价']
        df_day[price_cols] = df_day[price_cols].round(2)
        
        # 设置索引为datetime
        df_day.index = pd.to_datetime(df_day.index)
        
        print(f"成功从本地读取 {symbol} 日线数据，共 {len(df_day)} 条")
        return df_day
        
    def get_stock_k_data(self, code: str, start: int = 0, count: int = 1000, ktype: int = 9) -> pd.DataFrame:
        """
        获取指定周期的股票K线数据
        :param code: 股票代码（深市0开头，沪市6开头）
        :param start: 起始位置（0表示最新数据）
        :param count: 获取数据条数
        :param ktype: K线类型 2=30分钟, 9=日线, 8=周线, 7=月线
        :return: 格式化后的K线DataFrame + 最高价列表 + 最低价列表
        """
        # 判断市场（沪市1，深市0）
        market = 1 if code.startswith('6') else 0
        
        try:
            # 获取原始数据
            data = self.api.get_security_bars(ktype, market, code, start, count)
            # 转换为DataFrame并格式化
            df = pd.DataFrame(data)
            if df.empty:
                print(f"未获取到{self._ktype2name(ktype)}数据")
                return df, [], []
            
            # 提取最高价和最低价列表
            result_list_high = df['high'].astype(float).tolist()
            result_list_low = df['low'].astype(float).tolist()
            
            # 格式化字段
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.rename(columns={
                'open': '开盘价', 'high': '最高价', 'low': '最低价', 
                'close': '收盘价', 'vol': '成交量', 'amount': '成交额'
            })
            # 设置日期时间为索引
            df = df.set_index('datetime')
            # 保留核心字段
            result_df = df[['开盘价', '最高价', '最低价', '收盘价', '成交量', '成交额']]
            
            # 按周期存储数据
            period = self._ktype2name(ktype)
            self.stock_data[period] = result_df
            print(f"成功获取 {code} {period} 数据，共 {len(result_df)} 条")
            return result_df, result_list_high, result_list_low
        except Exception as e:
            print(f"获取{self._ktype2name(ktype)}数据失败: {e}")
            return pd.DataFrame(), [], []
    
    def _ktype2name(self, ktype: int) -> str:
        """将ktype数值转换为周期名称"""
        ktype_map = {
            0: '5min',
            1: '15min',
            2: '30min',
            3: '60min',
            9: 'day',
            8: 'week',
            7: 'month'
        }
        return ktype_map.get(ktype, f'未知周期({ktype})')
    
    def get_multi_period_data(self, code: str, count: int = 800, use_local: bool = False, tdx_path: str = DEFAULT_TDX_PATH) -> Dict[str, pd.DataFrame]:
        """
        一键获取日线+30分钟线数据（支持本地/联网切换）
        :param code: 股票代码
        :param count: 各周期获取的数据条数（仅联网模式生效）
        :param use_local: 是否使用本地数据（True=本地，False=联网）
        :param tdx_path: 通达信本地路径（仅本地模式生效）
        :return: 多周期数据字典
        """
        result = {}
        
        if use_local:
            # 本地模式：读取本地数据
            day_data = self.get_local_day_data(code, tdx_path)
            min30_data = self.get_exact_tdx_30min(code, tdx_path)
            
            # 提取高低价列表
            min30_high = min30_data['最高价'].astype(float).tolist() if not min30_data.empty else []
            min30_low = min30_data['最低价'].astype(float).tolist() if not min30_data.empty else []
            
            result = {
                'day': day_data,
                '30min_high_list': min30_high,
                '30min_low_list': min30_low,
                '30min': min30_data
            }
        else:
            # 联网模式：原有逻辑
            day_data, day_high_list, day_low_list = self.get_stock_k_data(code, count=count, ktype=9)
            min30_data, min30_high_list, min30_low_list = self.get_stock_k_data(code, count=count, ktype=2)
            
            result = {
                'day': day_data,
                '30min_high_list': min30_high_list,
                '30min_low_list': min30_low_list,
                '30min': min30_data
            }
        
        # 存储到实例变量
        self.stock_data['day'] = result['day']
        self.stock_data['30min'] = result['30min']
        
        return result
    
    def calculate_position_size(self, current_cash: float, entry_price: float, stop_loss_price: float) -> int:
        """
        以损定量核心计算：根据止损百分比计算可买数量
        :param current_cash: 当前可用总资金
        :param entry_price: 买入价格
        :param stop_loss_price: 止损价格
        :return: 可买数量（100的整数倍，整手）
        """
        if entry_price <= stop_loss_price:
            return 0  # 止损价不能大于等于买入价
        
        # 单笔可承受的最大亏损金额 = 当前总资金 * 止损百分比
        self.risk_per_trade = current_cash * self.stop_loss_ratio
        
        # 每股亏损 = 买入价 - 止损价
        loss_per_share = entry_price - stop_loss_price
        
        # 可买数量 = 可承受亏损金额 / 每股亏损（取整手）
        max_shares = self.risk_per_trade / loss_per_share
        position_size = int(max_shares // 100 * 100)  # 转换为100的整数倍
        
        # 安全校验：确保买入金额不超过可用资金（含手续费）
        max_affordable = current_cash / (entry_price * 1.001)  # 预留手续费空间
        position_size = min(position_size, int(max_affordable // 100 * 100))
        
        return max(position_size, 0)  # 确保数量非负
    
    @staticmethod
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
        up_segments = []
        for seg in segments:
            start_idx, end_idx, direction = seg
            if direction == 1:  # 高点到低点，属于向下线段
                down_segments.append((start_idx, end_idx))
            else:
                up_segments.append((start_idx, end_idx))

        # 按线段结束位置从近到远排序（最近的在前面）
        down_segments.sort(key=lambda x: -x[1])  # 按end_idx降序排列
        up_segments.sort(key=lambda x: -x[1])  # 按end_idx降序排列
        
        # 至少需要1个向下线段才可能形成信号
        if len(down_segments) < 1:
            return pf_out
        
        # 取最近的向下线段
        latest_seg = down_segments[0]    # 最近的向下线段
        latest_high = high[latest_seg[0]]    # 最近线段起点（高点）的价格

        # 条件：最后一根K线价格突破最近向下线段的起点高点
        last_k_idx = data_len - 1  # 最后一根K线的索引
        if high[last_k_idx] <= latest_high:
            return pf_out  # 未突破高点，不满足
        
        # 线段间隔条件判断
        seg_length = latest_seg[1] - latest_seg[0]
        after_seg_length = last_k_idx - latest_seg[1]
        if seg_length >= 9:
            if after_seg_length * 1.2 > seg_length:
                return pf_out  # 线段间隔过近，不满足
        else:
            if after_seg_length > seg_length:
                return pf_out  # 线段间隔过近，不满足
        
        # 检查向上线段中是否有价格突破最近高点
        if len(down_segments) > 0:
            up_seg_start, up_seg_end = down_segments[0]
            i = up_seg_end
            if i < last_k_idx:
                while i < last_k_idx:
                    if high[i] >= latest_high:
                        return pf_out  # 向上线段中有价格高于最近高点，不满足
                    i += 1

        # 所有条件满足，标记信号
        pf_out[last_k_idx] = 1.0
        return pf_out
    
    @staticmethod
    def calculate_three_buy_signals( high_full, low_full, close_full) -> List[float]:
        """
        遍历完整数据序列，逐段计算三买变体买点信号
        :param high_full: 完整的最高价序列（全量数据）
        :param low_full: 完整的最低价序列（全量数据）
        :return: 全量数据的买点信号列表，1.0表示对应位置是买点，0.0表示无
        """
        # 校验全量数据长度一致
        if  len(high_full) != len(low_full) or len(high_full) != len(close_full):
            raise ValueError("high_full、low_full、close_full必须长度一致")
        
        total_length = len(high_full)
        # 初始化全量信号数组（默认全为0）
        full_signals = [0.0] * total_length
        
        # 遍历每个数据点，逐步扩展窗口计算信号
        for window_end in range(1, total_length + 1):
            # 截取当前窗口的子数据（从0到window_end-1）
            high_window = high_full[:window_end]
            low_window = low_full[:window_end]
            frac_window = gupiaojichu.identify_turns(window_end, high_window, low_window)
            
            # 调用三买变体函数，计算当前窗口的信号
            try:
                window_signal = TdxStockBacktest.three_buy_variant(frac_window, high_window, low_window)
            except Exception as e:
                # 若窗口数据不足，跳过并保持0
                continue
                # 如果当前窗口有信号，需要检查收盘价是否高于最近顶分型最高价
            if window_signal[-1] == 1.0:
                current_close = close_full[window_end - 1]
                # 在frac_window中找最后一个顶分型（值为1.0）的索引
                last_top_idx = -1
                for i in range(window_end - 1, -1, -1):
                    if frac_window[i] == 1.0:
                        last_top_idx = i
                        if last_top_idx != window_end - 1:  
                            break
                if last_top_idx != -1:
                    last_top_high = high_window[last_top_idx]
                    if current_close <= last_top_high:
                        window_signal[-1] = 0.0  # 不满足收盘价高于前顶分型，撤销信号

            # 提取当前窗口最后一个位置的信号
            current_signal = window_signal[-1]
            full_signals[window_end - 1] = current_signal
        
        return full_signals
    
    @staticmethod
    def calculate_dynamic_sell_signals(high_full: List[float], low_full: List[float], 
                                     close_full: List[float], ma60_full: pd.Series,
                                     buy_price: float = None, buy_idx: int = None) -> tuple[List[bool], List[str]]:
        """
        逐K线模拟动态出现，计算动态卖出条件。
        增加逻辑：针对MA60卖出条件，需满足涨幅>10%或持仓>30根K线。
        """
        total_length = len(high_full)
        sell_signals = [False] * total_length
        sell_reasons = [""] * total_length
        ma60_list = ma60_full.tolist()
        
        for window_end in range(1, total_length + 1):
            if window_end < 60:
                continue
                
            current_idx = window_end - 1
            current_close = close_full[current_idx]
            
            # === 1. 均线条件判定 (含门槛校验) ===
            cond_ma60 = False
            ma60_reason = ""
            
            # 基础条件：连续两根收盘价小于MA60
            if current_idx >= 1:
                if current_close < ma60_list[current_idx] and close_full[current_idx-1] < ma60_list[current_idx-1]:
                    # 检查是否处于持仓状态且满足门槛
                    if buy_price is not None and buy_idx is not None:
                        # 计算当前涨幅和持仓K线数
                        profit_ratio = (current_close - buy_price) / buy_price
                        # 持仓K线数 = 当前索引 - 买入索引
                        hold_count = current_idx - buy_idx
                        
                        # 执行门槛检查
                        if profit_ratio >= 0.10 or hold_count >= 30:
                            cond_ma60 = True
                            ma60_reason = f"连续2根低于MA60(达标:收益{profit_ratio:.1%}/持仓{hold_count}线)"
                    else:
                        # 如果没有传入买入信息（初始化阶段），默认不触发均线卖出
                        cond_ma60 = False

            # === 2. 分型条件判定 (不设门槛，作为硬止损) ===
            high_window = high_full[:window_end]
            frac_window = gupiaojichu.identify_turns(window_end, high_window, low_full[:window_end])
            
            top_indices = [i for i, val in enumerate(frac_window) if val == 1.0]
            bottom_indices = [i for i, val in enumerate(frac_window) if val == -1.0]
            
            cond_pattern1 = False # 顶分型后未突破前高且距离达标
            cond_pattern2 = False # 跌破底分型
            
            if bottom_indices:
                last_bottom_idx = bottom_indices[-1]
                if low_full[current_idx] < low_full[last_bottom_idx]:
                    cond_pattern2 = True
                    
                valid_tops = [i for i in top_indices if i < last_bottom_idx]
                if valid_tops:
                    prev_top_idx = valid_tops[-1]
                    if (current_idx - last_bottom_idx) >= (last_bottom_idx - prev_top_idx):
                        if max(high_window[last_bottom_idx:]) <= high_window[prev_top_idx]:
                            cond_pattern1 = True
            
            # === 3. 综合信号 ===
            reasons = []
            if cond_ma60: reasons.append(ma60_reason)
            if cond_pattern1: reasons.append("底分型后未突破前高且距离达标")
            if cond_pattern2: reasons.append("跌破最后底分型最低价")
            
            if reasons:
                sell_signals[current_idx] = True
                sell_reasons[current_idx] = "；".join(reasons)
                
        return sell_signals, sell_reasons
    
    @staticmethod
    def calc_max_drawdown(asset_values: np.ndarray) -> float:
        """
        计算最大回撤
        :param asset_values: 总资产序列
        :return: 最大回撤百分比（负数表示回撤）
        """
        if len(asset_values) == 0:
            return 0.0
        
        # 计算累计最大值
        running_max = np.maximum.accumulate(asset_values)
        # 计算回撤
        drawdown = (asset_values - running_max) / running_max
        # 最大回撤
        max_dd = np.min(drawdown) * 100
        return max_dd
    
    def calc_backtest_metrics(self, init_cash: float) -> Dict:
        """计算回测核心指标（包含盈亏比）"""
        if self.backtest_result is None or len(getattr(self, 'trade_pnl', [])) == 0:
            return {}
        
        # 累计收益
        total_profit = self.backtest_result['总资产'].iloc[-1] - init_cash
        total_return = total_profit / init_cash * 100
        
        # 年化收益
        time_delta = (self.backtest_result.index[-1] - self.backtest_result.index[0])
        if '30min' in self.stock_data and not self.stock_data['30min'].empty:
            total_periods = time_delta.total_seconds() / 1800  # 总30分钟数
            annual_return = (pow((self.backtest_result['总资产'].iloc[-1] / init_cash), 1440/total_periods) - 1) * 100 if total_periods !=0 else 0
        else:
            days = time_delta.days
            annual_return = (pow((self.backtest_result['总资产'].iloc[-1] / init_cash), 365/days) - 1) * 100 if days !=0 else 0
        
        # 最大回撤
        max_drawdown = self.calc_max_drawdown(self.backtest_result['总资产'].values)
        
        # 胜率
        total_trades = len(self.trade_pnl)
        win_trades = sum(1 for pnl in self.trade_pnl if pnl > 0)
        win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
        
        # 盈亏比：平均盈利 / 平均亏损（取绝对值）
        profits = [pnl for pnl in self.trade_pnl if pnl > 0]
        losses = [abs(pnl) for pnl in self.trade_pnl if pnl < 0]
        avg_profit = np.mean(profits) if profits else 0.0
        avg_loss = np.mean(losses) if losses else 0.0
        profit_loss_ratio = avg_profit / avg_loss if avg_loss != 0 else 0.0
        
        # 夏普比率（简化版，无风险利率取0）
        returns = self.backtest_result['总资产'].pct_change().dropna()
        sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std() if returns.std() != 0 else 0.0
        
        metrics = {
            '初始资金': init_cash,
            '最终总资产': self.backtest_result['总资产'].iloc[-1],
            '累计收益': total_profit,
            '累计收益率(%)': total_return,
            '年化收益率(%)': annual_return,
            '最大回撤(%)': max_drawdown,
            '总交易次数': total_trades,
            '盈利交易次数': win_trades,
            '胜率(%)': win_rate,
            '平均盈利': avg_profit,
            '平均亏损': avg_loss,
            '盈亏比': profit_loss_ratio,
            '夏普比率': sharpe_ratio
        }
        
        return metrics
    @staticmethod
    def check_dynamic_sell_condition(current_idx: int, high_full: List[float], low_full: List[float], 
                                    close_full: List[float], ma60_full: List[float],
                                    buy_price: float, buy_idx: int, min30_frac: List[float]) -> tuple[bool, str]:
        """
        针对单根K线判定是否触发动态卖出（含盈亏门槛校验）
        """
        current_close = close_full[current_idx]
        
        # === 1. 均线条件判定 (满足涨幅 > 10% 或 持仓 > 30根) ===
        cond_ma60 = False
        ma60_reason = ""
        if current_idx >= 1:
            # 基础条件：连续两根收盘价小于MA60
            if current_close < ma60_full[current_idx] and close_full[current_idx-1] < ma60_full[current_idx-1]:
                # 计算当前涨幅和持仓时长
                profit_ratio = (current_close - buy_price) / buy_price
                hold_count = current_idx - buy_idx
                
                # 门槛检查
                if profit_ratio >= 0.10 or hold_count >= 30:
                    cond_ma60 = True
                    ma60_reason = f"跌破60均线(收益{profit_ratio:.1%}/持仓{hold_count}线)"

        # === 2. 分型/形态条件 (作为硬性保护，不设门槛) ===
        cond_pattern = False
        pattern_reason = ""
        # 截取到当前为止的转折点数据
        frac_window = min30_frac[:current_idx+1]
        top_indices = [i for i, val in enumerate(frac_window) if val == 1.0]
        bottom_indices = [i for i, val in enumerate(frac_window) if val == -1.0]
        
        if bottom_indices:
            last_bottom_idx = bottom_indices[-1]
            # 跌破最近底分型最低价
            if low_full[current_idx] < low_full[last_bottom_idx] and current_idx > last_bottom_idx+1:
                cond_pattern = True
                pattern_reason = "跌破最后底分型最低价"
                
            # 顶分型后未创新高逻辑
            valid_tops = [i for i in top_indices if i < last_bottom_idx]
            if valid_tops:
                prev_top_idx = valid_tops[-1]
                if (current_idx - last_bottom_idx) >= (last_bottom_idx - prev_top_idx):
                    if max(high_full[last_bottom_idx:current_idx+1]) <= high_full[prev_top_idx]:
                        cond_pattern = True
                        pattern_reason = "底分型后未突破前高且距离达标"
        
        if cond_ma60: return True, ma60_reason
        if cond_pattern: return True, pattern_reason
        return False, ""   
    
    def three_buy_strategy(self, day_df: pd.DataFrame, min30_data: pd.DataFrame, min30_high: List[float], min30_low: List[float]) -> pd.DataFrame:
        data = min30_data.copy()
        # --- 新增：统一索引名称，确保 reset_index 后的列名一致 ---
        data.index.name = 'datetime'
        day_df.index.name = 'datetime'
        # --- 1. 日线过滤条件 (保持原逻辑) ---
        day_df = day_df.copy()
        day_df['ma60'] = day_df['收盘价'].rolling(window=60).mean()
        day_df['ma60_shift3'] = day_df['ma60'].shift(3)
        day_df['day_cond'] = (day_df['收盘价'] > day_df['ma60']) & (day_df['ma60'] > day_df['ma60_shift3'])
        day_df['day_signal_valid'] = day_df['day_cond'].shift(1).fillna(False)

        data['date_only'] = data.index.date
        day_df['date_only'] = day_df.index.date
        # --- 这里的合并逻辑现在就不会报错了 ---
        data = data.reset_index().merge(
            day_df[['date_only', 'day_signal_valid']], 
            on='date_only', 
            how='left'
        ).set_index('datetime')

        # --- 2. 预计算基础指标 ---
        # 计算买入信号 (全量计算基础信号)
        buy_signals = self.calculate_three_buy_signals(min30_high, min30_low, data['收盘价'].tolist())
        data['buy_signal'] = buy_signals
        data['ma60'] = data['收盘价'].rolling(window=60).mean().bfill()
        
        # 转换列表以提高循环效率
        close_list = data['收盘价'].tolist()
        ma60_list = data['ma60'].tolist()
        high_list = min30_high
        low_list = min30_low
        
        # --- 3. 核心状态机循环 ---
        data['signal'] = 0
        data['sell_reason'] = ""
        in_pos = False
        buy_price = 0.0
        buy_idx = 0
        initial_stop_loss = 0.0
        
        for i in range(len(data)):
            current_idx_time = data.index[i]
            
            if not in_pos:
                # 尝试买入
                if data['buy_signal'].iloc[i] == 1.0 and data['day_signal_valid'].iloc[i]:
                    data.loc[current_idx_time, 'signal'] = 1
                    in_pos = True
                    buy_price = close_list[i]
                    buy_idx = i
                    # 设置初始止损价：买入K线最低点
                    initial_stop_loss = low_list[i]
            else:
                # 持仓中：判定卖出
                
                # A. 初始止损检查 (优先级最高)
                if low_list[i] <= initial_stop_loss:
                    data.loc[current_idx_time, 'signal'] = -1
                    data.loc[current_idx_time, 'sell_reason'] = "触发初始止损价"
                    in_pos = False
                    continue
                min30_frac = gupiaojichu.identify_turns(i, high_list[:i], low_list[:i])
                # B. 动态卖出检查 (传入实时持仓数据)
                is_sell, reason = self.check_dynamic_sell_condition(
                    current_idx=i,
                    high_full=high_list,
                    low_full=low_list,
                    close_full=close_list,
                    ma60_full=ma60_list,
                    buy_price=buy_price,  # 现在这里有值了！
                    buy_idx=buy_idx,      # 现在这里有值了！
                    min30_frac =min30_frac
                )
                
                if is_sell:
                    data.loc[current_idx_time, 'signal'] = -1
                    data.loc[current_idx_time, 'sell_reason'] = reason
                    in_pos = False

        return data
    
    def run_backtest(self, code: str, period: str = '30min', init_cash: float = 100000.0, 
                     commission: float = 0.0003, stop_loss_ratio: float = 0.01,
                     use_local: bool = False, tdx_path: str = DEFAULT_TDX_PATH) -> Tuple[pd.DataFrame, Dict, List[Dict]]:
        """
        执行三买变体策略回测（30分钟周期）
        升级：返回单股票交易明细列表，用于总笔数汇总
        :param code: 股票代码
        :param period: 回测周期（仅支持30min）
        :param init_cash: 初始资金
        :param commission: 交易佣金（默认0.03%）
        :param stop_loss_ratio: 总资金止损百分比（默认2%）
        :return: 回测结果DataFrame, 回测指标字典, 单股票交易明细列表
        """
        print(f"\n========== 开始回测股票 {code} ==========")
        print(f"数据源模式：{'本地通达信数据' if use_local else '联网获取'}")
   
        # 重置单股票交易明细
        self.all_trades_detail = []
        
        if period != '30min':
            print("当前版本仅支持30分钟周期回测")
            return pd.DataFrame(), {}, []
        
        # 联网模式需要先连接服务器
        if not use_local:
            connect_success = self.connect_tdx()
            if not connect_success:
                print("联网模式连接失败，终止回测")
                return pd.DataFrame(), {}, []
        
        # 获取多周期数据（自动切换本地/联网）
        multi_data = self.get_multi_period_data(code, count=800, use_local=use_local, tdx_path=tdx_path)
        day_data = multi_data['day']
        min30_data = multi_data['30min']
        min30_high = multi_data['30min_high_list']
        min30_low = multi_data['30min_low_list']
        
        if min30_data.empty:
            print(f"股票 {code} 30分钟数据为空，无法回测")
            return pd.DataFrame(), {}, []
        
        # 生成策略信号
        data = self.three_buy_strategy(day_data, min30_data, min30_high, min30_low)
        
        # 初始化止损百分比
        self.stop_loss_ratio = stop_loss_ratio
        print(f"\n以损定量配置：单笔交易最大亏损 = 总资金 × {stop_loss_ratio*100}%")
        
        # 初始化回测参数
        cash = init_cash  # 可用资金
        position = 0  # 持仓数量
        total_asset = init_cash  # 总资产（现金+持仓市值）
        trade_records = []  # 交易记录
        daily_results = []  # 每30分钟结果
        self.trade_pnl = []      # 记录每笔交易的盈亏
        
        # 止损相关初始化
        self.stop_loss_price = 0.0
        self.in_position = False
        buy_kline_low = 0.0
        buy_datetime = None
        
        # 逐行执行回测
        buy_price = 0
        buy_fee = 0
        
        for datetime, row in data.iterrows():
            close_price = row['收盘价']
            low_price = row['最低价']
            high_price = row['最高价']
            current_total_asset = cash + position * close_price
            current_idx = data.index.get_loc(datetime)  # 当前K线索引
            
            # ===== 止损逻辑：触发止损则强制卖出 =====
            if self.in_position and low_price <= self.stop_loss_price:
                # 触发止损，以收盘价卖出全部持仓
                sell_num = position
                fee = sell_num * close_price * commission
                # 卖出收入 = 卖出数量 × 卖出价格 × (1 - 佣金率)
                income = sell_num * close_price * (1 - commission)
                
                # 计算这笔止损交易的盈亏
                total_buy_cost = sell_num * buy_price + buy_fee
                total_sell_income = income - fee
                pnl = total_sell_income - total_buy_cost
                
                # 更新资金和记录交易
                cash += income
                trade_detail = {
                    '股票代码': code,
                    '交易时间': datetime,
                    '交易类型': '止损卖出',
                    '价格': close_price,
                    '数量': sell_num,
                    '费用': fee,
                    '单笔盈亏': pnl,
                    '是否盈利': pnl > 0,
                    '触发止损价': self.stop_loss_price,
                    '当前K线最低价': low_price,
                    '单笔风险金额': self.risk_per_trade,
                    '实际盈亏比例': pnl/current_total_asset*100
                }
                trade_records.append(trade_detail)
                self.trade_pnl.append(pnl)
                self.all_trades_detail.append(trade_detail)  # 存入交易明细
                
                # 重置持仓和止损参数
                position = 0
                buy_price = 0
                buy_fee = 0
                self.stop_loss_price = 0.0
                self.in_position = False
                buy_kline_low = 0.0
                self.risk_per_trade = 0.0
                
                print(f"【止损触发】{datetime} - 价格{low_price} <= 止损价{self.stop_loss_price}，以收盘价{close_price}卖出{sell_num}股")
                print(f"          - 单笔风险金额:{self.risk_per_trade:.2f} | 实际亏损:{pnl:.2f} | 实际盈亏比例:{abs(pnl)/current_total_asset*100:.2f}%")
            
            # ===== 优化后的买入逻辑：加入收盘价高于前顶分型条件 =====
            if row['signal'] == 1 and cash > close_price and not self.in_position:
                # 3. 原有的止损价计算逻辑
                if current_idx > 0:
                    prev_close = data['最高价'].iloc[current_idx - 1]
                    loss_price = min(row['最低价'], prev_close)

                    if (close_price-loss_price)/loss_price*100 > 2.4:
                        continue
                    if (close_price-loss_price)/loss_price*100 < 0.5:
                        continue
                    self.stop_loss_price = loss_price
                else:
                    # 如果是第一根K线（无前值），则使用当前最低价
                    self.stop_loss_price = row['最低价']
                
                # 4. 以损定量：计算可买数量
                buy_num = self.calculate_position_size(
                    current_cash=current_total_asset,
                    entry_price=close_price,
                    stop_loss_price=self.stop_loss_price
                )
                
                if buy_num > 0:
                    # 计算交易成本
                    cost = buy_num * close_price * (1 + commission)
                    fee = buy_num * close_price * commission
                    if cash >= cost:
                        position += buy_num
                        cash -= cost
                        buy_price = close_price
                        buy_fee = fee
                        self.in_position = True
                        buy_datetime = datetime
                        
                        trade_detail = {
                            '股票代码': code,
                            '交易时间': datetime,
                            '交易类型': '买入',
                            '价格': close_price,
                            '数量': buy_num,
                            '费用': fee,
                            '单笔盈亏': 0.0,  # 买入时盈亏为0，卖出时更新
                            '是否盈利': None,
                            '设置止损价': self.stop_loss_price,
                            '单笔风险金额': self.risk_per_trade,
                            '风险比例': self.stop_loss_ratio*100,
                            '单k涨幅': (close_price - self.stop_loss_price)/self.stop_loss_price*100
                        }
                        trade_records.append(trade_detail)
                        self.all_trades_detail.append(trade_detail)  # 存入交易明细
                        
                        print(f"【买入开仓（以损定量）】{datetime} - 价格{close_price}，数量{buy_num}")
                        print(f"          - 止损价:{self.stop_loss_price} | 单笔风险金额:{self.risk_per_trade:.2f} | 风险比例:{self.stop_loss_ratio*100}%")
                else:
                    print(f"【买入失败】{datetime} - 以损定量计算可买数量为0（止损价{self.stop_loss_price} >= 买入价{close_price}）")
            
            # ===== 正常卖出信号执行 =====
            elif row['signal'] == -1 and position > 0:
                # 获取当前K线索引（第几根K线）
                current_kline_idx = data.index.get_loc(datetime) + 1  # 从1开始计数
                # 获取卖出原因
                sell_reason = row['sell_reason'] if 'sell_reason' in row else "未知原因"
                
                # 卖出全部持仓
                sell_num = position
                # 计算交易费用
                fee = sell_num * close_price * commission
                # 卖出收入 = 卖出数量 × 卖出价格 × (1 - 佣金率)
                income = sell_num * close_price * (1 - commission)
                # 计算这笔交易的盈亏
                total_buy_cost = sell_num * buy_price + buy_fee
                total_sell_income = income - fee
                pnl = total_sell_income - total_buy_cost
                
                cash += income
                trade_detail = {
                    '股票代码': code,
                    '交易时间': datetime,
                    '交易类型': '策略卖出',
                    '价格': close_price,
                    '数量': sell_num,
                    '费用': fee,
                    '单笔盈亏': pnl,
                    '是否盈利': pnl > 0,
                    '卖出原因': sell_reason,
                    '当前K线索引': current_kline_idx,
                    '单笔风险金额': self.risk_per_trade,
                    '实际盈亏比例': pnl/current_total_asset*100
                }
                trade_records.append(trade_detail)
                self.trade_pnl.append(pnl)
                self.all_trades_detail.append(trade_detail)  # 存入交易明细
                
                # 重置持仓和止损参数
                position = 0
                buy_price = 0
                buy_fee = 0
                self.stop_loss_price = 0.0
                self.in_position = False
                buy_kline_low = 0.0
                self.risk_per_trade = 0.0
                
                # 新增：打印卖出原因和K线索引
                print(f"【策略卖出】{datetime} - 第{current_kline_idx}根K线 | 价格{close_price}，数量{sell_num}，盈亏{pnl:.2f}")
                print(f"          - 卖出原因：{sell_reason}")
                print(f"          - 单笔风险金额:{self.risk_per_trade:.2f} | 实际盈亏比例:{pnl/current_total_asset*100:.2f}%")
            
            # 计算当前总资产
            total_asset = cash + position * close_price
            daily_results.append({
                '时间': datetime,
                '收盘价': close_price,
                '持仓数量': position,
                '可用现金': cash,
                '总资产': total_asset,
                '累计收益': total_asset - init_cash,
                '累计收益率': (total_asset - init_cash) / init_cash * 100,
                '止损价格': self.stop_loss_price if self.in_position else 0.0,
                '单笔风险金额': self.risk_per_trade if self.in_position else 0.0
            })
        
        # 整理回测结果
        self.backtest_result = pd.DataFrame(daily_results)
        self.backtest_result = self.backtest_result.set_index('时间')
        
        # 添加交易记录
        self.trade_records = pd.DataFrame(trade_records)
        
        # 计算核心指标
        metrics = self.calc_backtest_metrics(init_cash)
        
        # 打印单股票回测指标
        print(f"\n===== 股票 {code} 回测核心指标 =====")
        for k, v in metrics.items():
            print(f"{k}: {v:.2f}")
        
        print(f"\n========== 股票 {code} 回测完成 ==========\n")
        return self.backtest_result, metrics, self.all_trades_detail


def parse_tdx_blk_file(file_path: str) -> List[str]:
    stock_list = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
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
                # stock_list.append((market, stock_code, full_code))            
                stock_list.append((stock_code))            
    except Exception as e:
        print(f"读取blk文件失败: {e}")
        
    return stock_list

def calculate_total_trades_metrics(all_trades_detail: List[Dict]) -> Dict:
    """
    新增：按总交易笔数计算核心指标
    :param all_trades_detail: 所有股票的交易明细列表
    :return: 总笔数维度的汇总指标
    """
    if not all_trades_detail:
        return {}
    
    # 过滤出有盈亏的交易（买入交易无盈亏，仅统计卖出/止损交易）
    pnl_trades = [trade for trade in all_trades_detail if trade['单笔盈亏'] != 0.0]
    if not pnl_trades:
        return {
            '总交易笔数（含买入）': len(all_trades_detail),
            '有效交易笔数（有盈亏）': 0,
            '盈利笔数': 0,
            '亏损笔数': 0,
            '总胜率(%)': 0.0,
            '总盈利金额': 0.0,
            '总亏损金额': 0.0,
            '总净收益': 0.0,
            '平均每笔盈利': 0.0,
            '平均每笔亏损': 0.0,
            '总盈亏比': 0.0,
            '最大单笔盈利': 0.0,
            '最大单笔亏损': 0.0
        }
    
    # 核心计算
    total_trades_all = len(all_trades_detail)          # 所有交易笔数（含买入）
    total_pnl_trades = len(pnl_trades)                 # 有效交易笔数（卖出/止损）
    win_trades = [t for t in pnl_trades if t['是否盈利']] # 盈利笔数
    loss_trades = [t for t in pnl_trades if not t['是否盈利']] # 亏损笔数
    total_win_count = len(win_trades)
    total_loss_count = len(loss_trades)
    
    # 金额类指标
    total_profit = sum(t['单笔盈亏'] for t in win_trades)
    total_loss = abs(sum(t['单笔盈亏'] for t in loss_trades))
    total_net_profit = total_profit - total_loss
    avg_win_per_trade = total_profit / total_win_count if total_win_count > 0 else 0.0
    avg_loss_per_trade = total_loss / total_loss_count if total_loss_count > 0 else 0.0
    total_profit_loss_ratio = avg_win_per_trade / avg_loss_per_trade if avg_loss_per_trade > 0 else 0.0
    
    # 极值指标
    max_single_win = max([t['单笔盈亏'] for t in win_trades], default=0.0)
    max_single_loss = min([t['单笔盈亏'] for t in loss_trades], default=0.0)
    
    # 胜率
    total_win_rate = (total_win_count / total_pnl_trades) * 100 if total_pnl_trades > 0 else 0.0
    
    # 汇总结果
    total_metrics = {
        '总交易笔数（含买入）': total_trades_all,
        '有效交易笔数（卖出/止损）': total_pnl_trades,
        '盈利笔数': total_win_count,
        '亏损笔数': total_loss_count,
        '总胜率(%)': total_win_rate,
        '总盈利金额': total_profit,
        '总亏损金额': total_loss,
        '总净收益': total_net_profit,
        '平均每笔盈利': avg_win_per_trade,
        '平均每笔亏损': avg_loss_per_trade,
        '总盈亏比': total_profit_loss_ratio,
        '最大单笔盈利': max_single_win,
        '最大单笔亏损': max_single_loss
    }
    
    return total_metrics

def batch_backtest(stock_codes: List[str], init_cash: float = 100000.0, 
                   commission: float = 0.0003, stop_loss_ratio: float = 0.01) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    批量回测股票列表（升级：新增总笔数汇总）
    :param stock_codes: 股票代码列表
    :param init_cash: 单股票初始资金
    :param commission: 佣金比例
    :param stop_loss_ratio: 止损比例
    :return: 单股票汇总结果DF, 所有交易明细DF, 总笔数汇总指标
    """
    # 初始化回测框架并连接通达信
    backtest = TdxStockBacktest()
    connect_success = backtest.connect_tdx()
    if not connect_success:
        print("通达信服务器连接失败，无法执行批量回测")
        return pd.DataFrame(), pd.DataFrame(), {}
    
    # 存储所有股票的回测指标 + 所有交易明细
    all_metrics = []
    all_trades_detail = []  # 存储所有股票的交易明细
    
    # 逐只股票回测
    for idx, code in enumerate(stock_codes):
        print(f"\n------------------- 进度 {idx+1}/{len(stock_codes)} -------------------")
        try:
            _, metrics, trades_detail = backtest.run_backtest(
                code=code,
                init_cash=init_cash,
                commission=commission,
                stop_loss_ratio=stop_loss_ratio,
                use_local=True  ,# 统一使用联网模式获取数据
                tdx_path=DEFAULT_TDX_PATH
            )
            if metrics:  # 仅保留有有效指标的股票
                metrics['股票代码'] = code
                all_metrics.append(metrics)
            if trades_detail:  # 收集交易明细
                all_trades_detail.extend(trades_detail)
            # 避免请求过快，休眠0.1秒
            time.sleep(0.1)
        except Exception as e:
            print(f"股票 {code} 回测异常: {e}")
            continue
    
    # ========== 原有：单股票维度汇总 ==========
    if not all_metrics:
        print("无有效回测结果")
        return pd.DataFrame(), pd.DataFrame(), {}
    
    # 单股票汇总DF
    stock_summary_df = pd.DataFrame(all_metrics)
    # 调整列顺序，把股票代码放第一列
    cols = ['股票代码'] + [col for col in stock_summary_df.columns if col != '股票代码']
    stock_summary_df = stock_summary_df[cols]
    
    # 单股票维度汇总指标
    print("\n" + "="*80)
    print("                          板块回测（单股票维度）汇总")
    print("="*80)
    total_stocks = len(stock_summary_df)
    profitable_stocks = len(stock_summary_df[stock_summary_df['累计收益'] > 0])
    avg_return = stock_summary_df['累计收益率(%)'].mean()
    avg_max_dd = stock_summary_df['最大回撤(%)'].mean()
    avg_win_rate = stock_summary_df['胜率(%)'].mean()
    avg_profit_loss_ratio = stock_summary_df['盈亏比'].mean()
    total_profit_all = stock_summary_df['累计收益'].sum()
    total_return_all = (total_profit_all / (init_cash * total_stocks)) * 100
    
    # 打印单股票维度汇总
    print(f"1. 参与回测股票总数：{total_stocks} 只")
    print(f"2. 盈利股票数量：{profitable_stocks} 只 | 盈利股票占比：{profitable_stocks/total_stocks*100:.2f}%")
    print(f"3. 单股票平均累计收益率：{avg_return:.2f}%")
    print(f"4. 单股票平均最大回撤：{avg_max_dd:.2f}%")
    print(f"5. 单股票平均胜率：{avg_win_rate:.2f}%")
    print(f"6. 单股票平均盈亏比：{avg_profit_loss_ratio:.2f}")
    print(f"7. 板块总累计收益：{total_profit_all:.2f} 元 (等额资金分配下)")
    print(f"8. 板块总累计收益率：{total_return_all:.2f}% (等额资金分配下)")
    print("="*80)
    
    # ========== 新增：总交易笔数维度汇总 ==========
    total_trades_metrics = calculate_total_trades_metrics(all_trades_detail)
    print("\n" + "="*80)
    print("                          板块回测（总交易笔数维度）汇总")
    print("="*80)
    for k, v in total_trades_metrics.items():
        print(f"{k}: {v:.2f}")
    print("="*80)
    
    # 交易明细DF（便于保存和分析）
    trades_detail_df = pd.DataFrame(all_trades_detail)
    
    return stock_summary_df, trades_detail_df, total_trades_metrics

def calculate_pure_compounding(all_trades_detail: List[Dict], init_cash: float = 100000.0) -> Dict:
    """
    纯理论复利计算：忽略资金重叠，将所有有效交易按时间排序，进行单队列资产滚动。
    :param all_trades_detail: batch_backtest 返回的交易明细列表
    :param init_cash: 模拟的账户初始总资金（默认100万）
    """
    if not all_trades_detail:
        return {}

    # 1. 过滤出平仓交易（只有卖出和止损才有实际的单笔盈亏结算）
    closed_trades = [t for t in all_trades_detail if t.get('单笔盈亏', 0) != 0.0]
    
    if not closed_trades:
        return {}

    # 2. 严格按照交易时间（平仓时间）升序排列
    closed_trades.sort(key=lambda x: pd.to_datetime(x['交易时间']))

    # 3. 开始模拟资金滚动
    current_capital = init_cash
    capital_curve = [current_capital]  # 记录资金曲线用于算回撤
    
    for trade in closed_trades:
        # 你原本代码里的 '实际盈亏比例' 是相对于单股10万资产的波动百分比
        # 这里我们直接将这个净值波动率作用于全局总资金
        # 注意：原代码 '实际盈亏比例' 是放大了100倍的百分比（如 1.5 表示 1.5%），所以这里要除以 100
        trade_return_ratio = trade['实际盈亏比例'] / 100.0
        
        # 核心复利公式：新资金 = 老资金 * (1 + 单笔涨跌幅)
        current_capital = current_capital * (1 + trade_return_ratio)
        capital_curve.append(current_capital)

    # 4. 计算复利曲线的最大回撤
    capital_array = np.array(capital_curve)
    running_max = np.maximum.accumulate(capital_array)
    drawdowns = (capital_array - running_max) / running_max
    max_dd = np.min(drawdowns) * 100

    # 5. 汇总数据
    total_return_pct = (current_capital - init_cash) / init_cash * 100

    return {
        "初始总资金": init_cash,
        "最终总资金": current_capital,
        "参与复利交易笔数": len(closed_trades),
        "理论复利总收益率(%)": total_return_pct,
        "复利资金曲线最大回撤(%)": max_dd
    }

def analyze_loss_periods(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame()
    
    df = trades_df.copy()
    # 转换时间格式
    df['交易时间'] = pd.to_datetime(df['交易时间'])
    df['月份'] = df['交易时间'].dt.to_period('M')
    df['周几'] = df['交易时间'].dt.day_name()
    df['日期'] = df['交易时间'].dt.date
    
    # 修正字段名：原代码中记录盈亏的列名是 '单笔盈亏'
    loss_df = df[df['单笔盈亏'] < 0]
    
    if loss_df.empty:
        print("\n太棒了，没有亏损单！")
        return pd.DataFrame()
    
    # --- 维度1：按月份统计亏损额 ---
    monthly_loss = loss_df.groupby('月份')['单笔盈亏'].sum().sort_values()
    
    # --- 维度2：按日期统计（找出最惨烈的几天） ---
    # 这里用全量df去算，因为要知道那一天的胜率
    daily_stats = df.groupby('日期').agg(
        当日净损益=('单笔盈亏', 'sum'),
        交易笔数=('单笔盈亏', 'count'),
        亏损占比=('单笔盈亏', lambda x: (x < 0).mean() * 100)
    ).sort_values(by='当日净损益')

    print("\n" + "!"*20 + " 亏损时间分布报告 " + "!"*20)
    print("\n[1] 亏损最严重的月份:")
    print(monthly_loss.head())
    
    print("\n[2] 亏损最严重的 5 个交易日:")
    print(daily_stats.head(5))
    
    # --- 维度3：连续亏损预警 ---
    # 如果某天亏损笔数 > 5 且 胜率 < 10%，定义为“系统性收割日”
    trap_days = daily_stats[(daily_stats['交易笔数'] > 5) & (daily_stats['亏损占比'] > 80)]
    if not trap_days.empty:
        print("\n[3] 识别到“系统性收割日”（大面积止损）:")
        print(trap_days.index.tolist())
    
    # === 核心提取：将所有亏损日期提取出来，用于返回并存入Excel ===
    # 统计每一天发生的具体亏损情况
    loss_dates_summary = loss_df.groupby('日期').agg(
        亏损单数量=('股票代码', 'count'),
        当日总亏损额=('单笔盈亏', 'sum')
    ).reset_index().sort_values(by='当日总亏损额', ascending=True)

    return loss_dates_summary

# ------------------- 主执行入口 -------------------
if __name__ == "__main__":
    # 1. 解析通达信板块文件
    stock_list = parse_tdx_blk_file(BLOB_FILE_PATH)
    # stock_list = stock_list[:10]  # 测试时可限制股票数量
    
    if not stock_list:
        print("未提取到股票代码，退出程序")
    else:
        # 2. 执行批量回测
        stock_summary_result, trades_detail_result, total_trades_metrics = batch_backtest(
            stock_codes=stock_list,
            init_cash=100000.0,    # 单股票初始资金10万
            commission=0.0003,     # 佣金0.03%
            stop_loss_ratio=0.01   # 单笔止损1%
        )
        # ================== 执行理论复利计算 ==================
        trades_list = trades_detail_result.to_dict('records') if not trades_detail_result.empty else []
        compounding_metrics = calculate_pure_compounding(trades_list, init_cash=100000.0)
        
        print("\n" + "="*80)
        print("                    板块回测（无视重叠的极限复利）汇总")
        print("="*80)
        for k, v in compounding_metrics.items():
            if "(%)" in k:
                print(f"{k}: {v:.2f}%")
            elif "资金" in k:
                print(f"{k}: {v:,.2f} 元")
            else:
                print(f"{k}: {v}")
        print("="*80)
        
        # ================== 执行亏损日期分析 ==================
        loss_dates_df = analyze_loss_periods(trades_detail_result)
        t = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(time.time()))
        # 3. 保存结果到Excel（多sheet）
        if not stock_summary_result.empty:
            with pd.ExcelWriter(f"板块回测汇总结果_含总笔数{t}.xlsx", engine="openpyxl") as writer:
                # Sheet1：单股票汇总
                stock_summary_result.to_excel(writer, sheet_name="单股票维度汇总", index=False)
                # Sheet2：所有交易明细
                trades_detail_result.to_excel(writer, sheet_name="所有交易明细", index=False)
                # Sheet3：总笔数维度汇总
                total_trades_df = pd.DataFrame([total_trades_metrics])
                total_trades_df.to_excel(writer, sheet_name="总交易笔数维度汇总", index=False)
                # Sheet4：提取的亏损明细表
                if loss_dates_df is not None and not loss_dates_df.empty:
                    loss_dates_df.to_excel(writer, sheet_name="亏损日期提取", index=False)
            
            print(f"\n汇总结果已保存到: 板块回测汇总结果_含总笔数.xlsx，请查看 '亏损日期提取' Sheet。")