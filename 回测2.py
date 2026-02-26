import time
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pytdx.hq import TdxHq_API
from typing import Callable, Dict, List
import gupiaojichu

# 忽略无关警告
warnings.filterwarnings('ignore')

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
    
    def get_multi_period_data(self, code: str, count: int = 800) -> Dict[str, pd.DataFrame]:
        """
        一键获取日线+30分钟线数据
        :param code: 股票代码
        :param count: 各周期获取的数据条数
        :return: 多周期数据字典 {'day': 日线, '30min_high_list': 30分钟最高价列表, '30min_low_list': 30分钟最低价列表, '30min': 30分钟线}
        """
        # 获取日线数据
        day_data, day_high_list, day_low_list = self.get_stock_k_data(code, count=count, ktype=9)
        # 获取30分钟线数据
        min30_data, min30_high_list, min30_low_list = self.get_stock_k_data(code, count=count, ktype=2)
        
        return {
            'day': day_data,
            '30min_high_list': min30_high_list,
            '30min_low_list': min30_low_list,
            '30min': min30_data
        }
    
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
    # def detect_turning_points(high: List[float], low: List[float], window: int = 5) -> List[float]:
    #     """
    #     检测高低转折点，生成frac数组（1.0=高点，-1.0=低点，0.0=无转折）
    #     :param high: 最高价列表
    #     :param low: 最低价列表
    #     :param window: 窗口期（判断高低点的前后K线数）
    #     :return: frac转折点标记列表
    #     """
    #     data_len = len(high)
    #     frac = [0.0] * data_len
        
    #     for i in range(window, data_len - window):
    #         # 判断是否为高点：当前最高价大于前后window根K线的最高价
    #         is_high = True
    #         for j in range(1, window + 1):
    #             if high[i] <= high[i - j] or high[i] <= high[i + j]:
    #                 is_high = False
    #                 break
    #         if is_high:
    #             frac[i] = 1.0
    #             continue
            
    #         # 判断是否为低点：当前最低价小于前后window根K线的最低价
    #         is_low = True
    #         for j in range(1, window + 1):
    #             if low[i] >= low[i - j] or low[i] >= low[i + j]:
    #                 is_low = False
    #                 break
    #         if is_low:
    #             frac[i] = -1.0
        
    #     return frac
    
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
            if after_seg_length * 1.9 > seg_length:
                return pf_out  # 线段间隔过近，不满足
        else:
            if after_seg_length > seg_length:
                return pf_out  # 线段间隔过近，不满足
        
        # 检查向上线段中是否有价格突破最近高点
        if len(up_segments) > 0:
            up_seg_start, up_seg_end = up_segments[0]
            i = up_seg_start
            while i < up_seg_end:
                if high[i] >= latest_high:
                    return pf_out  # 向上线段中有价格高于最近高点，不满足
                i += 1

        # 所有条件满足，标记信号
        pf_out[last_k_idx] = 1.0
        return pf_out
    
    @staticmethod
    def calculate_three_buy_signals( high_full, low_full):
        """
        遍历完整数据序列，逐段计算三买变体买点信号
        :param frac_full: 完整的转折点标记列表（全量数据）
        :param high_full: 完整的最高价序列（全量数据）
        :param low_full: 完整的最低价序列（全量数据）
        :return: 全量数据的买点信号列表，1.0表示对应位置是买点，0.0表示无
        """
        # 校验全量数据长度一致
        if  len(high_full) != len(low_full):
            raise ValueError("frac_full、high_full、low_full必须长度一致")
        
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
            
            # 提取当前窗口最后一个位置的信号
            current_signal = window_signal[-1]
            full_signals[window_end - 1] = current_signal
        
        return full_signals
    @staticmethod
    def calculate_dynamic_sell_signals(high_full: List[float], low_full: List[float], close_full: List[float], ma60_full: pd.Series) -> List[bool]:
        """
        逐K线模拟动态出现，计算动态卖出条件，彻底杜绝未来函数。
        """
        total_length = len(high_full)
        sell_signals = [False] * total_length
        
        # 将Series转换为列表，大幅提升循环取值效率
        ma60_list = ma60_full.tolist()
        
        # 逐根K线向右推进（模拟实盘K线一根根走出来）
        for window_end in range(1, total_length + 1):
            if window_end < 60:
                continue
                
            current_idx = window_end - 1
            
            # === 条件1：均线条件（无未来函数风险，只看当前和前一根） ===
            cond_ma60 = False
            if current_idx >= 1:
                # 连续两根收盘价小于MA60
                if close_full[current_idx] < ma60_list[current_idx] and close_full[current_idx-1] < ma60_list[current_idx-1]:
                    cond_ma60 = True
                    
            # === 条件2 & 3：分型条件（需动态截取数据避免未来函数） ===
            # 【核心逻辑】：只切片取到当前K线的数据
            high_window = high_full[:window_end]
            low_window = low_full[:window_end]
            
            # 在当前可见的历史窗口内，识别顶底分型
            frac_window = gupiaojichu.identify_turns(window_end, high_window, low_window)
            
            top_indices = [i for i, val in enumerate(frac_window) if val == 1.0]
            bottom_indices = [i for i, val in enumerate(frac_window) if val == -1.0]
            
            cond_pattern1 = False
            cond_pattern2 = False
            
            # 只有当历史上已经确认出现过底分型时才判断
            if bottom_indices:
                last_bottom_idx = bottom_indices[-1]
                
                # 卖出条件3：当前最低价跌破已确认的最后底分型最低价
                if low_full[current_idx] < low_full[last_bottom_idx]:
                    cond_pattern2 = True
                    
                # 卖出条件2：距离和最高价限制
                valid_tops = [i for i in top_indices if i < last_bottom_idx]
                if valid_tops:
                    prev_top_idx = valid_tops[-1]
                    distance_btw = last_bottom_idx - prev_top_idx
                    current_distance = current_idx - last_bottom_idx
                    
                    if current_distance >= distance_btw:
                        # 检查从最后底分型到当前K线的最高价
                        high_range = high_window[last_bottom_idx:] 
                        if max(high_range) <= high_window[prev_top_idx]:
                            cond_pattern1 = True
                            
            # 综合判断：满足任一条件则触发卖出
            # 这里默认三个开关全部开启，你可以根据需要调整
            if cond_ma60 or cond_pattern1 or cond_pattern2:
                sell_signals[current_idx] = True
                
        return sell_signals
    
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
        if self.backtest_result is None or len(self.trade_pnl) == 0:
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
        
        # 打印核心指标
        print("\n===== 回测核心指标 =====")
        for k, v in metrics.items():
            print(f"{k}: {v:.2f}")
        
        return metrics
    
    def three_buy_strategy(self, min30_data: pd.DataFrame, min30_high: List[float], min30_low: List[float]) -> pd.DataFrame:
        """
        三买变体策略函数：生成30分钟级别的买卖信号（已消除未来函数）
        """
        data = min30_data.copy()
        
        # 1. 动态计算三买买点信号 (原函数已经是滚动窗口，安全的)
        buy_signals = self.calculate_three_buy_signals(min30_high, min30_low)
        data['buy_signal'] = buy_signals
        
        # 2. 计算60均线
        data['ma60'] = data['收盘价'].rolling(window=60).mean().bfill()
        
        # 3. 动态计算卖出信号 (调用新增的去未来函数方法)
        close_list = data['收盘价'].tolist()
        sell_signals = self.calculate_dynamic_sell_signals(min30_high, min30_low, close_list, data['ma60'])
        data['new_sell_cond'] = sell_signals
        
        # 4. 状态机生成最终交易信号
        data['signal'] = 0
        in_pos = False
        stop_loss = 0.0
        
        for idx in data.index:
            pos_idx = data.index.get_loc(idx)
            
            # 买入信号：有三买信号且未持仓
            if data['buy_signal'].iloc[pos_idx] == 1.0 and not in_pos:
                data.loc[idx, 'signal'] = 1
                in_pos = True
                # 设置初始止损价（买入K线最低价），防止建仓后极端插针行情
                stop_loss = data['最低价'].iloc[pos_idx]
            
            # 卖出信号：持仓状态下触发卖出条件
            elif in_pos:
                current_low = data['最低价'].iloc[pos_idx]
                
                # 初始止损：跌破买入K线最低价依然强制防守
                if current_low <= stop_loss:
                    data.loc[idx, 'signal'] = -1
                    in_pos = False
                # 动态策略卖出：触发了多维卖出条件之一
                elif data['new_sell_cond'].iloc[pos_idx]:
                    data.loc[idx, 'signal'] = -1
                    in_pos = False
        
        # 清理辅助列
        cols_to_drop = ['ma60', 'new_sell_cond', 'buy_signal']
        data = data.drop(columns=[c for c in cols_to_drop if c in data.columns])
        
        return data
    
    def run_backtest(self, code: str, period: str = '30min', init_cash: float = 100000.0, 
                     commission: float = 0.0003, stop_loss_ratio: float = 0.01) -> pd.DataFrame:
        """
        执行三买变体策略回测（30分钟周期）
        :param code: 股票代码
        :param period: 回测周期（仅支持30min）
        :param init_cash: 初始资金
        :param commission: 交易佣金（默认0.03%）
        :param stop_loss_ratio: 总资金止损百分比（默认2%）
        :return: 回测结果DataFrame
        """
        if period != '30min':
            print("当前版本仅支持30分钟周期回测")
            return pd.DataFrame()
        
        # 获取多周期数据
        multi_data = self.get_multi_period_data(code)
        min30_data = multi_data['30min']
        min30_high = multi_data['30min_high_list']
        min30_low = multi_data['30min_low_list']
        
        if min30_data.empty:
            print("30分钟数据为空，无法回测")
            return pd.DataFrame()
        
        # 生成策略信号
        data = self.three_buy_strategy(min30_data, min30_high, min30_low)
        
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
            
            # ===== 止损逻辑：触发止损则强制卖出 =====
            if self.in_position and low_price <= self.stop_loss_price:
                # 触发止损，以收盘价卖出全部持仓
                sell_num = position
                fee = sell_num * close_price * commission
                income = sell_num * close_price * (1 - commission)
                
                # 计算这笔止损交易的盈亏
                total_buy_cost = sell_num * buy_price + buy_fee
                total_sell_income = income - fee
                pnl = total_sell_income - total_buy_cost
                
                cash += income
                trade_records.append({
                    '时间': datetime,
                    '操作': '止损卖出',
                    '价格': close_price,
                    '数量': sell_num,
                    '费用': fee,
                    '单笔盈亏': pnl,
                    '触发止损价': self.stop_loss_price,
                    '当前K线最低价': low_price,
                    '单笔风险金额': self.risk_per_trade,
                    '实际亏损比例': abs(pnl)/current_total_asset*100
                })
                self.trade_pnl.append(pnl)
                
                # 重置持仓和止损参数
                position = 0
                buy_price = 0
                buy_fee = 0
                self.stop_loss_price = 0.0
                self.in_position = False
                buy_kline_low = 0.0
                self.risk_per_trade = 0.0
                
                print(f"【止损触发】{datetime} - 价格{low_price} <= 止损价{self.stop_loss_price}，以收盘价{close_price}卖出{sell_num}股")
                print(f"          - 单笔风险金额:{self.risk_per_trade:.2f} | 实际亏损:{pnl:.2f} | 实际亏损比例:{abs(pnl)/current_total_asset*100:.2f}%")
            
            # ===== 正常买入信号执行（以损定量） =====
            if row['signal'] == 1 and cash > close_price and not self.in_position:
                # 确定止损价（买入K线最低价）
                buy_kline_low = row['最低价']
                self.stop_loss_price = buy_kline_low
                
                # 以损定量：计算可买数量
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
                        
                        trade_records.append({
                            '时间': datetime,
                            '操作': '买入',
                            '价格': close_price,
                            '数量': buy_num,
                            '费用': fee,
                            '买入K线最低价': buy_kline_low,
                            '设置止损价': self.stop_loss_price,
                            '单笔风险金额': self.risk_per_trade,
                            '风险比例': self.stop_loss_ratio*100,
                        })
                        print(f"【买入开仓（以损定量）】{datetime} - 价格{close_price}，数量{buy_num}")
                        print(f"          - 止损价:{self.stop_loss_price} | 单笔风险金额:{self.risk_per_trade:.2f} | 风险比例:{self.stop_loss_ratio*100}%")
                else:
                    print(f"【买入失败】{datetime} - 以损定量计算可买数量为0（止损价{self.stop_loss_price} >= 买入价{close_price}）")
            
            # ===== 正常卖出信号执行 =====
            elif row['signal'] == -1 and position > 0:
                # 卖出全部持仓
                sell_num = position
                fee = sell_num * close_price * commission
                income = sell_num * close_price * (1 - commission)
                # 计算这笔交易的盈亏
                total_buy_cost = sell_num * buy_price + buy_fee
                total_sell_income = income - fee
                pnl = total_sell_income - total_buy_cost
                
                cash += income
                trade_records.append({
                    '时间': datetime,
                    '操作': '策略卖出',
                    '价格': close_price,
                    '数量': sell_num,
                    '费用': fee,
                    '单笔盈亏': pnl,
                    '单笔风险金额': self.risk_per_trade,
                    '实际盈亏比例': pnl/current_total_asset*100
                })
                self.trade_pnl.append(pnl)
                
                # 重置持仓和止损参数
                position = 0
                buy_price = 0
                buy_fee = 0
                self.stop_loss_price = 0.0
                self.in_position = False
                buy_kline_low = 0.0
                self.risk_per_trade = 0.0
                
                print(f"【策略卖出】{datetime} - 价格{close_price}，数量{sell_num}，盈亏{pnl:.2f}")
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
        self.calc_backtest_metrics(init_cash)
        
        print(f"\n{period}周期三买变体策略回测完成！")
        return self.backtest_result


# ------------------- 测试入口 -------------------
if __name__ == "__main__":
    # 初始化回测框架
    backtest = TdxStockBacktest()
    
    # 连接通达信服务器
    backtest.connect_tdx()
    
    # 执行回测
    result = backtest.run_backtest(
        code="600362",
        period="30min",
        init_cash=100000.0,
        commission=0.0003,
        stop_loss_ratio=0.01
    )
