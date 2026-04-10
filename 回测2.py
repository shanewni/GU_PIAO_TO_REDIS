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
from collections import defaultdict, deque

# 忽略无关警告
warnings.filterwarnings('ignore')

# 通达信板块文件路径
# BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\SSYNYS.blk"
BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\TEST2.blk"
# 本地通达信数据默认路径
DEFAULT_TDX_PATH = r"D:\zd_hbzq"

class TdxStockBacktest:
    """基于pytdx的股票回测框架（支持多周期K线+止损策略+以损定量+三买变体买点）"""
    
    def __init__(self):
        self.api = TdxHq_API()
        self.stock_data = {}  
        self.backtest_result = None  
        self.trade_records = None  
        self.stop_loss_price = 0.0  
        self.in_position = False    
        self.stop_loss_ratio = 0.02 
        self.risk_per_trade = 0.0   
        self.all_trades_detail = [] 
    
    def connect_tdx(self, ip: str = "152.136.167.10", port: int = 7709) -> bool:
        try:
            self.api.connect(ip, port)
            print(f"成功连接通达信服务器 {ip}:{port}")
            return True
        except Exception as e:
            print(f"连接失败: {e}")
            return False
            
    def get_exact_tdx_30min(self, symbol, tdx_path=DEFAULT_TDX_PATH, start_date=None, end_date=None):
        reader = Reader.factory(market='std', tdxdir=tdx_path)
        df_5m = reader.fzline(symbol=symbol)
        
        if df_5m is None or df_5m.empty:
            print(f"未获取到{symbol}本地5分钟数据")
            return pd.DataFrame()

        df_5m = df_5m.rename(columns={
            'open': '开盘价', 'high': '最高价', 'low': '最低价',
            'close': '收盘价', 'volume': '成交量', 'amount': '成交额'
        })
        
        price_cols = ['开盘价', '最高价', '最低价', '收盘价']
        df_5m[price_cols] = df_5m[price_cols].round(2)

        df_30m = df_5m.resample(
            '30Min', 
            closed='right',   
            label='right',    
            origin='start_day' 
        ).agg({
            '开盘价': 'first',
            '最高价': 'max',
            '最低价': 'min',
            '收盘价': 'last',
            '成交量': 'sum',
            '成交额': 'sum'
        })

        df_30m = df_30m.dropna(subset=['开盘价'])
        valid_times = ['10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30', '15:00']
        df_30m = df_30m[df_30m.index.strftime('%H:%M').isin(valid_times)]

        df_30m[price_cols] = df_30m[price_cols].round(2)
        df_30m.index.name = 'datetime'
        if start_date:
            df_30m = df_30m[df_30m.index >= pd.to_datetime(start_date)]
        if end_date:
            df_30m = df_30m[df_30m.index <= pd.to_datetime(end_date)]
            
        print(f"成功合成 {symbol} 30min数据，时间段：{start_date or '不限'} - {end_date or '不限'}，共 {len(df_30m)} 条")
        return df_30m
    
    def get_local_day_data(self, symbol, tdx_path=DEFAULT_TDX_PATH, start_date=None, end_date=None):
        reader = Reader.factory(market='std', tdxdir=tdx_path)
        df_day = reader.daily(symbol=symbol)
        
        if df_day is None or df_day.empty:
            print(f"未获取到{symbol}本地日线数据")
            return pd.DataFrame()
        
        df_day = df_day.rename(columns={
            'open': '开盘价', 'high': '最高价', 'low': '最低价',
            'close': '收盘价', 'volume': '成交量', 'amount': '成交额'
        })
        
        df_day.index = pd.to_datetime(df_day.index)
            
        if start_date:
            df_day = df_day[df_day.index >= pd.to_datetime(start_date)]
        if end_date:
            df_day = df_day[df_day.index <= pd.to_datetime(end_date)]
        
        price_cols = ['开盘价', '最高价', '最低价', '收盘价']
        df_day[price_cols] = df_day[price_cols].round(2)
        
        print(f"成功读取 {symbol} 日线({start_date or '起点'}至{end_date or '至今'})，共 {len(df_day)} 条")
        return df_day
        
    def get_stock_k_data(self, code: str, start: int = 0, count: int = 1000, ktype: int = 9) -> pd.DataFrame:
        market = 1 if code.startswith('6') else 0
        try:
            data = self.api.get_security_bars(ktype, market, code, start, count)
            df = pd.DataFrame(data)
            if df.empty:
                print(f"未获取到{self._ktype2name(ktype)}数据")
                return df, [], []
            
            result_list_high = df['high'].astype(float).tolist()
            result_list_low = df['low'].astype(float).tolist()
            
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.rename(columns={
                'open': '开盘价', 'high': '最高价', 'low': '最低价', 
                'close': '收盘价', 'vol': '成交量', 'amount': '成交额'
            })
            df = df.set_index('datetime')
            result_df = df[['开盘价', '最高价', '最低价', '收盘价', '成交量', '成交额']]
            
            period = self._ktype2name(ktype)
            self.stock_data[period] = result_df
            print(f"成功获取 {code} {period} 数据，共 {len(result_df)} 条")
            return result_df, result_list_high, result_list_low
        except Exception as e:
            print(f"获取{self._ktype2name(ktype)}数据失败: {e}")
            return pd.DataFrame(), [], []
    
    def _ktype2name(self, ktype: int) -> str:
        ktype_map = {0: '5min', 1: '15min', 2: '30min', 3: '60min', 9: 'day', 8: 'week', 7: 'month'}
        return ktype_map.get(ktype, f'未知周期({ktype})')
    
    def get_multi_period_data(self, code: str, count: int = 800, use_local: bool = False, tdx_path: str = DEFAULT_TDX_PATH, start_date=None, end_date=None) -> Dict[str, pd.DataFrame]:
        result = {}
        if use_local:
            day_data = self.get_local_day_data(code, tdx_path, start_date, end_date)
            min30_data = self.get_exact_tdx_30min(code, tdx_path, start_date, end_date)
            min30_high = min30_data['最高价'].astype(float).tolist() if not min30_data.empty else []
            min30_low = min30_data['最低价'].astype(float).tolist() if not min30_data.empty else []
            result = {'day': day_data, '30min_high_list': min30_high, '30min_low_list': min30_low, '30min': min30_data}
        else:
            day_data, day_high_list, day_low_list = self.get_stock_k_data(code, count=count, ktype=9)
            min30_data, min30_high_list, min30_low_list = self.get_stock_k_data(code, count=count, ktype=2)
            result = {'day': day_data, '30min_high_list': min30_high_list, '30min_low_list': min30_low_list, '30min': min30_data}
        
        self.stock_data['day'] = result['day']
        self.stock_data['30min'] = result['30min']
        return result
    
    @staticmethod
    def calculate_rps_matrix(all_stocks_data: Dict[str, pd.DataFrame], n: int = 250) -> pd.DataFrame:
        extrs_dict = {}
        for code, df in all_stocks_data.items():
            if df is None or df.empty or '收盘价' not in df.columns:
                continue
            extrs_dict[code] = df['收盘价'].pct_change(n)

        if not extrs_dict:
            return pd.DataFrame()
        extrs_df = pd.DataFrame(extrs_dict)
        rps_df = extrs_df.rank(axis=1, pct=True, ascending=True) * 100
        return rps_df
    
    def calculate_position_size(self, current_cash: float, entry_price: float, stop_loss_price: float) -> int:
        if entry_price <= stop_loss_price:
            return 0  
        self.risk_per_trade = current_cash * self.stop_loss_ratio
        loss_per_share = entry_price - stop_loss_price
        max_shares = self.risk_per_trade / loss_per_share
        position_size = int(max_shares // 100 * 100)  
        max_affordable = current_cash / (entry_price * 1.001)  
        position_size = min(position_size, int(max_affordable // 100 * 100))
        return max(position_size, 0)  
    
    @staticmethod
    def three_buy_variant(frac, high, low):
        data_len = len(high)
        if len(low) != data_len or len(frac) != data_len:
            raise ValueError("frac、high、low必须具有相同的长度")
        
        pf_out = [0.0] * data_len
        if data_len <= 0: return pf_out
        
        turn_points = []
        for i in range(data_len):
            val = frac[i]
            if val != 0.0: turn_points.append((i, int(val)))
        
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
            if not found: break  
        
        down_segments = []
        up_segments = []
        for seg in segments:
            start_idx, end_idx, direction = seg
            if direction == 1:  
                down_segments.append((start_idx, end_idx))
            else:
                up_segments.append((start_idx, end_idx))

        down_segments.sort(key=lambda x: -x[1])  
        up_segments.sort(key=lambda x: -x[1])  
        
        if len(down_segments) < 1: return pf_out
        
        latest_seg = down_segments[0]    
        latest_high = high[latest_seg[0]]    

        last_k_idx = data_len - 1  
        if high[last_k_idx] <= latest_high: return pf_out  
        
        seg_length = latest_seg[1] - latest_seg[0]
        after_seg_length = last_k_idx - latest_seg[1]
        if seg_length >= 9:
            if after_seg_length * 1.2 > seg_length: return pf_out  
        else:
            if after_seg_length > seg_length: return pf_out  
        
        down_seg_start, down_seg_end = down_segments[0]
        up_seg_start, up_seg_end = up_segments[0]
        if len(down_segments) > 0:
            if up_seg_end <= down_seg_end: return pf_out  
            if down_seg_start+6 > up_seg_end: return pf_out  
            i = down_seg_end
            if i < last_k_idx:
                while i < last_k_idx:
                    if high[i] >= latest_high: return pf_out  
                    i += 1
        
        if last_k_idx -1 <= down_seg_end: return pf_out  
        pf_out[last_k_idx] = 1.0
        return pf_out
    
    @staticmethod
    def calculate_three_buy_signals(high_full, low_full, close_full) -> List[float]:
        if len(high_full) != len(low_full) or len(high_full) != len(close_full):
            raise ValueError("high_full、low_full、close_full必须长度一致")
        total_length = len(high_full)
        full_signals = [0.0] * total_length
        LOOKBACK_WINDOW = 150
        for window_end in range(1, total_length + 1):
            start_idx = max(0, window_end - LOOKBACK_WINDOW)
            high_window = high_full[start_idx : window_end]
            low_window = low_full[start_idx : window_end]
            close_window = close_full[start_idx : window_end]
            
            frac_window = gupiaojichu.identify_turns(len(high_window), high_window, low_window)
            try: window_signal = TdxStockBacktest.three_buy_variant(frac_window, high_window, low_window)
            except Exception: continue
                
            if window_signal[-1] == 1.0:
                current_close = close_window[-1]
                last_top_idx = -1
                for i in range(len(frac_window) - 1, -1, -1):
                    if frac_window[i] == 1.0:
                        if i < len(frac_window) - 1:
                            last_top_idx = i
                            break
                if last_top_idx != -1:
                    last_top_high = high_window[last_top_idx]
                    if current_close <= last_top_high:
                        window_signal[-1] = 0.0
            full_signals[window_end - 1] = window_signal[-1]
        return full_signals

    @staticmethod
    def calc_max_drawdown(asset_values: np.ndarray) -> float:
        if len(asset_values) == 0: return 0.0
        running_max = np.maximum.accumulate(asset_values)
        drawdown = (asset_values - running_max) / running_max
        return np.min(drawdown) * 100
    
    def calc_backtest_metrics(self, init_cash: float) -> Dict:
        if self.backtest_result is None or len(getattr(self, 'trade_pnl', [])) == 0: return {}
        total_profit = self.backtest_result['总资产'].iloc[-1] - init_cash
        total_return = total_profit / init_cash * 100
        time_delta = (self.backtest_result.index[-1] - self.backtest_result.index[0])
        if '30min' in self.stock_data and not self.stock_data['30min'].empty:
            total_periods = time_delta.total_seconds() / 1800  
            annual_return = (pow((self.backtest_result['总资产'].iloc[-1] / init_cash), 1440/total_periods) - 1) * 100 if total_periods !=0 else 0
        else:
            days = time_delta.days
            annual_return = (pow((self.backtest_result['总资产'].iloc[-1] / init_cash), 365/days) - 1) * 100 if days !=0 else 0
        
        max_drawdown = self.calc_max_drawdown(self.backtest_result['总资产'].values)
        total_trades = len(self.trade_pnl)
        win_trades = sum(1 for pnl in self.trade_pnl if pnl > 0)
        win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
        profits = [pnl for pnl in self.trade_pnl if pnl > 0]
        losses = [abs(pnl) for pnl in self.trade_pnl if pnl < 0]
        avg_profit = np.mean(profits) if profits else 0.0
        avg_loss = np.mean(losses) if losses else 0.0
        profit_loss_ratio = avg_profit / avg_loss if avg_loss != 0 else 0.0
        returns = self.backtest_result['总资产'].pct_change().dropna()
        sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std() if returns.std() != 0 else 0.0
        
        return {
            '初始资金': init_cash, '最终总资产': self.backtest_result['总资产'].iloc[-1],
            '累计收益': total_profit, '累计收益率(%)': total_return, '年化收益率(%)': annual_return,
            '最大回撤(%)': max_drawdown, '总交易次数': total_trades, '盈利交易次数': win_trades,
            '胜率(%)': win_rate, '平均盈利': avg_profit, '平均亏损': avg_loss,
            '盈亏比': profit_loss_ratio, '夏普比率': sharpe_ratio
        }

    @staticmethod
    def check_dynamic_sell_condition(current_idx: int, high_full: List[float], low_full: List[float], 
                                    close_full: List[float], ma60_full: List[float],
                                    buy_price: float, buy_idx: int, min30_frac: List[float]) -> tuple[bool, str]:
        current_close = close_full[current_idx]
        cond_ma60 = False
        ma60_reason = ""
        if current_idx >= 1:
            if current_close < ma60_full[current_idx] and close_full[current_idx-1] < ma60_full[current_idx-1]:
                profit_ratio = (current_close - buy_price) / buy_price
                hold_count = current_idx - buy_idx
                if profit_ratio >= 0.10 or hold_count >= 30:
                    cond_ma60 = True
                    ma60_reason = f"跌破60均线(收益{profit_ratio:.1%}/持仓{hold_count}线)"

        cond_pattern = False
        pattern_reason = ""
        frac_window = min30_frac[:current_idx+1]
        top_indices = [i for i, val in enumerate(frac_window) if val == 1.0]
        bottom_indices = [i for i, val in enumerate(frac_window) if val == -1.0]
        
        if bottom_indices:
            last_bottom_idx = bottom_indices[-1]
            if low_full[current_idx] < low_full[last_bottom_idx] and current_idx > last_bottom_idx+1:
                cond_pattern = True
                pattern_reason = "跌破最后底分型最低价"
            valid_tops = [i for i in top_indices if i < last_bottom_idx]
            if valid_tops:
                prev_top_idx = valid_tops[-1]
                if (current_idx - last_bottom_idx) >= (last_bottom_idx - prev_top_idx) and (last_bottom_idx - prev_top_idx) >= 3:
                    if max(high_full[last_bottom_idx:current_idx+1]) <= high_full[prev_top_idx]:
                        cond_pattern = True
                        pattern_reason = "底分型后未突破前高且距离达标"
        
        if cond_ma60: return True, ma60_reason
        if cond_pattern: return True, pattern_reason
        return False, ""   
    
    def three_buy_strategy(self, day_df: pd.DataFrame, min30_data: pd.DataFrame, min30_high: List[float], min30_low: List[float]) -> pd.DataFrame:
            data = min30_data.copy()
            data.index.name = 'datetime'
            day_df = day_df.copy()
            day_df.index.name = 'datetime'
            
            # --- 核心改动：实时计算前日线下跌笔及其50%位置 ---
            high_day = day_df['最高价'].tolist()
            low_day = day_df['最低价'].tolist()
            mid_prices = [np.nan] * len(day_df)
            down_lens = [np.nan] * len(day_df)
            
            WINDOW_SIZE = 200 # 设置滑动窗口数量为200
            # 使用滑动窗口保障数据严格实时计算（不引用未来K线判断分型）
            for k in range(5, len(day_df)):
                # 截取最近200根日线K线进行笔结构识别
                start_win = max(0, k - WINDOW_SIZE + 1)
                h_win = high_day[start_win : k + 1]
                l_win = low_day[start_win : k + 1]

                try:
                    fracs = gupiaojichu.identify_turns(len(h_win), h_win, l_win)
                    # 寻找最后一个底分型
                    bottoms = [idx for idx, val in enumerate(fracs) if val == -1.0]
                    if bottoms:
                        last_b = bottoms[-1]
                        # 寻找底分型前的最后一个顶分型
                        tops = [idx for idx, val in enumerate(fracs) if val == 1.0 and idx < last_b]
                        if tops:
                            last_t = tops[-1]
                            # 计算50%位置和下跌笔K线数量
                            mid_prices[k] = (h_win[last_t] + l_win[last_b]) / 2.0
                            down_lens[k] = last_b - last_t
                except Exception:
                    pass
            
            day_df['prev_down_mid_price'] = mid_prices
            day_df['prev_down_len'] = down_lens
            day_df['day_idx'] = range(len(day_df)) # 用于后续比对日线持仓时长
            
            # 剥离原有日线均线与MFI限制，只匹配计算好的日线特征数据
            data['date_only'] = data.index.date
            day_df['date_only'] = day_df.index.date
            data = data.reset_index().merge(
                day_df[['date_only', 'prev_down_mid_price', 'prev_down_len', 'day_idx']], 
                on='date_only', 
                how='left'
            ).set_index('datetime')

            # --- 预计算基础指标 ---
            buy_signals = self.calculate_three_buy_signals(min30_high, min30_low, data['收盘价'].tolist())
            data['buy_signal'] = buy_signals
            data['ma60'] = data['收盘价'].rolling(window=60).mean().bfill()
            
            close_list = data['收盘价'].tolist()
            open_list = data['开盘价'].tolist()
            ma60_list = data['ma60'].tolist()
            high_list = min30_high
            low_list = min30_low
            
            # --- 核心状态机循环 ---
            data['signal'] = 0.0
            data['sell_reason'] = ""
            in_pos = False
            buy_price = 0.0
            buy_idx = 0
            initial_stop_loss = 0.0
            current_stop_loss = 0.0 
            
            has_reduced_half = False
            target_mid_price = np.nan
            target_hold_days = np.nan
            buy_day_idx = 0
            
            for i in range(len(data)):
                current_idx_time = data.index[i]
                current_time = current_idx_time.time()
                
                if not in_pos:
                    # 获取基本过滤器状态，与run_backtest同步
                    red = close_list[i] > open_list[i]
                    valid_time = (current_time >= pd.Timestamp('03:40').time()) and (current_time < pd.Timestamp('17:20').time())
                    
                    if data['buy_signal'].iloc[i] == 1.0 and red and valid_time:
                        close_p = close_list[i]
                        
                        # 设置拟止损价
                        if i > 0:
                            prev_high = high_list[i-1]
                            temp_stop_loss = min(low_list[i], prev_high)
                        else:
                            temp_stop_loss = low_list[i]
                            
                        if temp_stop_loss <= 0: continue
                        
                        # 执行涨跌幅限制过滤（原run_backtest剥离）
                        risk_pct = (close_p - temp_stop_loss) / temp_stop_loss * 100
                        if 0.5 <= risk_pct <= 3.0:
                            mid_price = data['prev_down_mid_price'].iloc[i]
                            # 新增条件：起爆k的潜在收益幅度 > 止损风险幅度
                            if pd.notna(mid_price) and close_p > temp_stop_loss:
                                reward = mid_price - close_p
                                risk = close_p - temp_stop_loss
                                if reward > risk:
                                    # 确认买入
                                    data.loc[current_idx_time, 'signal'] = 1
                                    in_pos = True
                                    buy_price = close_p
                                    buy_idx = i
                                    initial_stop_loss = temp_stop_loss
                                    current_stop_loss = initial_stop_loss
                                    has_reduced_half = False
                                    target_mid_price = mid_price
                                    target_hold_days = data['prev_down_len'].iloc[i]
                                    buy_day_idx = data['day_idx'].iloc[i]
                else:       
                    data.loc[current_idx_time, 'active_stop_loss'] = current_stop_loss
                    
                    # 关心一：是否止损 (全平)
                    if low_list[i] <= current_stop_loss:
                        data.loc[current_idx_time, 'signal'] = -1
                        data.loc[current_idx_time, 'sell_reason'] = f"触发止损(当前止损价:{current_stop_loss})"
                        in_pos = False
                        has_reduced_half = False
                        continue

                    # 关心三：日线级别持仓天数是否大于前日线下跌笔天数 (全平)
                    current_day_idx = data['day_idx'].iloc[i]
                    if pd.notna(target_hold_days) and (current_day_idx - buy_day_idx) > target_hold_days:
                        data.loc[current_idx_time, 'signal'] = -1
                        data.loc[current_idx_time, 'sell_reason'] = f"持仓天数({current_day_idx - buy_day_idx})超过前日线下跌笔({target_hold_days})"
                        in_pos = False
                        has_reduced_half = False
                        continue
                        
                    # 关心二：是否达到前笔50%位置 (减仓一半)
                    if not has_reduced_half and pd.notna(target_mid_price) and high_list[i] >= target_mid_price:
                        data.loc[current_idx_time, 'signal'] = -0.5 # 新增：-0.5代表减半
                        data.loc[current_idx_time, 'sell_reason'] = "到达前日线下跌笔50%阻力位，减仓一半"
                        has_reduced_half = True
                        # 保本止损：将剩余仓位的止损线上移至成本价
                        current_stop_loss = max(current_stop_loss, buy_price)
                        continue # 继续持有另一半

                    # 其他：参考现有卖出规则
                    min30_frac = gupiaojichu.identify_turns(i, high_list[:i], low_list[:i])
                    is_sell, reason = self.check_dynamic_sell_condition(
                        current_idx=i, high_full=high_list, low_full=low_list, close_full=close_list,
                        ma60_full=ma60_list, buy_price=buy_price, buy_idx=buy_idx, min30_frac=min30_frac
                    )
                    
                    if is_sell:
                        data.loc[current_idx_time, 'signal'] = -1
                        data.loc[current_idx_time, 'sell_reason'] = reason
                        in_pos = False
                        has_reduced_half = False

            return data
    
    def run_backtest(self, code: str, period: str = '30min', init_cash: float = 100000.0, 
                     commission: float = 0.0003, stop_loss_ratio: float = 0.01,
                     use_local: bool = False, tdx_path: str = DEFAULT_TDX_PATH, start_date=None, end_date=None) -> Tuple[pd.DataFrame, Dict, List[Dict]]:
        print(f"\n========== 开始回测股票 {code} ==========")
        print(f"数据源模式：{'本地通达信数据' if use_local else '联网获取'}")
   
        self.all_trades_detail = []
        if period != '30min':
            return pd.DataFrame(), {}, []
        
        if not use_local and not self.connect_tdx():
            return pd.DataFrame(), {}, []
        
        multi_data = self.get_multi_period_data(code, count=800, use_local=use_local, tdx_path=tdx_path, start_date=start_date, end_date=end_date)
        day_data = multi_data['day']
        min30_data = multi_data['30min']
        min30_high = multi_data['30min_high_list']
        min30_low = multi_data['30min_low_list']
        
        if min30_data.empty: return pd.DataFrame(), {}, []
        
        data = self.three_buy_strategy(day_data, min30_data, min30_high, min30_low)
        self.stop_loss_ratio = stop_loss_ratio
        
        cash = init_cash  
        position = 0  
        trade_records = []  
        daily_results = []  
        self.trade_pnl = []      
        self.stop_loss_price = 0.0
        self.in_position = False
        buy_price = 0
        buy_fee = 0
        
        for datetime, row in data.iterrows():
            close_price = row['收盘价']
            current_total_asset = cash + position * close_price
            current_idx = data.index.get_loc(datetime) 

            if self.in_position:
                self.stop_loss_price = row.get('active_stop_loss', self.stop_loss_price)

            # ===== 买入执行 =====
            if row['signal'] == 1 and cash > close_price and not self.in_position:
                # 过滤逻辑已全部上提到 three_buy_strategy 以保证状态同步
                if current_idx > 0:
                    prev_close = data['最高价'].iloc[current_idx - 1]
                    self.stop_loss_price = min(row['最低价'], prev_close)
                else:
                    self.stop_loss_price = row['最低价']
                
                buy_num = self.calculate_position_size(
                    current_cash=current_total_asset, entry_price=close_price, stop_loss_price=self.stop_loss_price
                )
                
                if buy_num > 0:
                    self.buy_in_total_asset = cash + position * close_price
                    cost = buy_num * close_price * (1 + commission)
                    fee = buy_num * close_price * commission
                    if cash >= cost:
                        position += buy_num
                        cash -= cost
                        buy_price = close_price
                        buy_fee = fee
                        self.in_position = True
                        self.buy_kline_index = current_idx  
                        
                        trade_detail = {
                            '股票代码': code, '交易时间': datetime, '交易类型': '买入', '价格': close_price,
                            '数量': buy_num, '费用': fee, '单笔盈亏': 0.0, '是否盈利': None,
                            '设置止损价': self.stop_loss_price, '单笔风险金额': self.risk_per_trade,
                            '风险比例': self.stop_loss_ratio*100, '单k涨幅': (close_price - self.stop_loss_price)/self.stop_loss_price*100
                        }
                        trade_records.append(trade_detail)
                        self.all_trades_detail.append(trade_detail)  
                        print(f"【买入开仓（以损定量）】{datetime} - 价格{close_price}，数量{buy_num}")
            
            # ===== 卖出及减半执行 =====
            elif (row['signal'] == -1 or row['signal'] == -0.5) and position > 0:
                current_kline_idx = data.index.get_loc(datetime) + 1  
                sell_reason = row.get('sell_reason', "未知原因")
                is_half = (row['signal'] == -0.5)
                
                if is_half:
                    # 减半仓位，并且必须满足整手
                    sell_num = int((position / 2) // 100 * 100)
                    if sell_num <= 0:
                        sell_num = position  # 如果只剩一手无法再减半，默认全部平掉
                        is_half = False
                else:
                    sell_num = position
                
                fee = sell_num * close_price * commission
                income = sell_num * close_price * (1 - commission)
                
                # 按照拆分比例计算这笔卖出应摊派的建仓成本与费用
                proportion = sell_num / position if position > 0 else 1.0
                cost_of_sold = sell_num * buy_price
                proportional_buy_fee = buy_fee * proportion
                
                pnl = (income - fee) - (cost_of_sold + proportional_buy_fee)
                hold_k_count = current_idx - self.buy_kline_index + 1  
                
                cash += income
                position -= sell_num
                buy_fee -= proportional_buy_fee  # 扣减掉已消化的手续费
                
                trade_detail = {
                    '股票代码': code, '交易时间': datetime, '交易类型': '减半卖出' if is_half else '策略卖出',
                    '价格': close_price, '数量': sell_num, '费用': fee, '单笔盈亏': pnl, '是否盈利': pnl > 0,
                    '卖出原因': sell_reason, '当前K线索引': current_kline_idx, '持仓K线数量': hold_k_count,  
                    '单笔风险金额': self.risk_per_trade, '实际盈亏比例': pnl/self.buy_in_total_asset*100
                }
                trade_records.append(trade_detail)
                self.trade_pnl.append(pnl)
                self.all_trades_detail.append(trade_detail)  
                
                print(f"【{'减半卖出' if is_half else '策略卖出'}】{datetime} - 第{current_kline_idx}根K线 | 价格{close_price}，数量{sell_num}，盈亏{pnl:.2f}")
                print(f"          - 卖出原因：{sell_reason}")
                
                # 如果是全平仓 或 减半遇到最后1手直接平了
                if not is_half or position == 0:
                    buy_price = 0
                    buy_fee = 0
                    self.stop_loss_price = 0.0
                    self.in_position = False
                    self.risk_per_trade = 0.0
                else:
                    self.in_position = True # 减仓一半后保持持仓状态
            
            total_asset = cash + position * close_price
            daily_results.append({
                '时间': datetime, '收盘价': close_price, '持仓数量': position, '可用现金': cash,
                '总资产': total_asset, '累计收益': total_asset - init_cash,
                '累计收益率': (total_asset - init_cash) / init_cash * 100,
                '止损价格': self.stop_loss_price if self.in_position else 0.0,
                '单笔风险金额': self.risk_per_trade if self.in_position else 0.0
            })
        
        self.backtest_result = pd.DataFrame(daily_results).set_index('时间')
        self.trade_records = pd.DataFrame(trade_records)
        metrics = self.calc_backtest_metrics(init_cash)
        return self.backtest_result, metrics, self.all_trades_detail

def parse_tdx_blk_file(file_path: str) -> List[str]:
    stock_list = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines[1:]:
            line = line.strip()
            if line:
                stock_code = line[1:7]
                stock_list.append((stock_code))            
    except Exception as e:
        print(f"读取blk文件失败: {e}")
    return stock_list

# ===== 下方辅助统计分析函数 (与原逻辑一致，篇幅所限省略中间不变部分) =====

def calculate_total_trades_metrics(all_trades_detail: List[Dict]) -> Dict:
    if not all_trades_detail: return {}
    pnl_trades = [t for t in all_trades_detail if t.get('单笔盈亏', 0) != 0.0 and '持仓K线数量' in t]
    if not pnl_trades:
        return {'总交易笔数（含买入）': len(all_trades_detail), '有效交易笔数（有盈亏）': 0, '盈利笔数': 0, '亏损笔数': 0, '总胜率(%)': 0.0, '总盈利金额': 0.0, '总亏损金额': 0.0, '总净收益': 0.0, '平均每笔盈利': 0.0, '平均每笔亏损': 0.0, '总盈亏比': 0.0, '最大单笔盈利': 0.0, '最大单笔亏损': 0.0}

    hold_periods = [t['持仓K线数量'] for t in pnl_trades]
    win_hold_periods = [t['持仓K线数量'] for t in pnl_trades if t['是否盈利']]
    loss_hold_periods = [t['持仓K线数量'] for t in pnl_trades if not t['是否盈利']]

    total_trades_all = len(all_trades_detail)          
    total_pnl_trades = len(pnl_trades)                 
    win_trades = [t for t in pnl_trades if t['是否盈利']] 
    loss_trades = [t for t in pnl_trades if not t['是否盈利']] 
    total_win_count = len(win_trades)
    total_loss_count = len(loss_trades)
    
    total_profit = sum(t['单笔盈亏'] for t in win_trades)
    total_loss = abs(sum(t['单笔盈亏'] for t in loss_trades))
    total_net_profit = total_profit - total_loss
    avg_win_per_trade = total_profit / total_win_count if total_win_count > 0 else 0.0
    avg_loss_per_trade = total_loss / total_loss_count if total_loss_count > 0 else 0.0
    total_profit_loss_ratio = avg_win_per_trade / avg_loss_per_trade if avg_loss_per_trade > 0 else 0.0
    
    max_single_win = max([t['单笔盈亏'] for t in win_trades], default=0.0)
    max_single_loss = min([t['单笔盈亏'] for t in loss_trades], default=0.0)
    total_win_rate = (total_win_count / total_pnl_trades) * 100 if total_pnl_trades > 0 else 0.0
    
    return {
        '总交易笔数（含买入）': total_trades_all, '有效交易笔数（卖出/止损）': total_pnl_trades,
        '盈利笔数': total_win_count, '亏损笔数': total_loss_count, '总胜率(%)': total_win_rate,
        '总盈利金额': total_profit, '总亏损金额': total_loss, '总净收益': total_net_profit,
        '平均每笔盈利': avg_win_per_trade, '平均每笔亏损': avg_loss_per_trade,
        '总盈亏比': total_profit_loss_ratio, '最大单笔盈利': max_single_win, '最大单笔亏损': max_single_loss,
        '平均持仓K线数量': round(np.mean(hold_periods), 2) if hold_periods else 0.0,
        '盈利平均K线数量': round(np.mean(win_hold_periods), 2) if win_hold_periods else 0.0,
        '亏损平均K线数量': round(np.mean(loss_hold_periods), 2) if loss_hold_periods else 0.0,
        '最高持仓K线数量': int(np.max(hold_periods)) if hold_periods else 0
    }

def batch_backtest(stock_codes: List[str], init_cash: float = 100000.0, 
                   commission: float = 0.0003, stop_loss_ratio: float = 0.01) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    backtest = TdxStockBacktest()
    if not backtest.connect_tdx(): return pd.DataFrame(), pd.DataFrame(), {}
    
    all_metrics = []
    all_trades_detail = []  

    for idx, code in enumerate(stock_codes):
        print(f"\n------------------- 进度 {idx+1}/{len(stock_codes)} -------------------")
        try:
            _, metrics, trades_detail = backtest.run_backtest(
                code=code, init_cash=init_cash, commission=commission, stop_loss_ratio=stop_loss_ratio,
                use_local=True, tdx_path=DEFAULT_TDX_PATH, start_date='2023-12-01', end_date='2026-05-28'
            )
            if metrics:  
                metrics['股票代码'] = code
                all_metrics.append(metrics)
            if trades_detail:  
                all_trades_detail.extend(trades_detail)
            time.sleep(0.001)
        except Exception as e:
            print(f"股票 {code} 回测异常: {e}")
            continue
    
    if not all_metrics: return pd.DataFrame(), pd.DataFrame(), {}
    
    stock_summary_df = pd.DataFrame(all_metrics)
    cols = ['股票代码'] + [col for col in stock_summary_df.columns if col != '股票代码']
    stock_summary_df = stock_summary_df[cols]
    
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
    
    print(f"1. 参与回测股票总数：{total_stocks} 只")
    print(f"2. 盈利股票数量：{profitable_stocks} 只 | 盈利股票占比：{profitable_stocks/total_stocks*100:.2f}%")
    print(f"3. 单股票平均累计收益率：{avg_return:.2f}%")
    print(f"4. 单股票平均最大回撤：{avg_max_dd:.2f}%")
    print(f"5. 单股票平均胜率：{avg_win_rate:.2f}%")
    print(f"6. 单股票平均盈亏比：{avg_profit_loss_ratio:.2f}")
    print(f"7. 板块总累计收益：{total_profit_all:.2f} 元 (等额资金分配下)")
    print(f"8. 板块总累计收益率：{total_return_all:.2f}% (等额资金分配下)")
    print("="*80)
    
    total_trades_metrics = calculate_total_trades_metrics(all_trades_detail)
    print("\n" + "="*80)
    print("                          板块回测（总交易笔数维度）汇总")
    print("="*80)
    for k, v in total_trades_metrics.items():
        print(f"{k}: {v:.2f}")
    print("="*80)
    
    trades_detail_df = pd.DataFrame(all_trades_detail)
    return stock_summary_df, trades_detail_df, total_trades_metrics

def calculate_pure_compounding(all_trades_detail: List[Dict], init_cash: float = 100000.0) -> Dict:
    if not all_trades_detail: return {}
    closed_trades = [t for t in all_trades_detail if t.get('单笔盈亏', 0) != 0.0]
    if not closed_trades: return {}
    closed_trades.sort(key=lambda x: pd.to_datetime(x['交易时间']))

    current_capital = init_cash
    capital_curve = [current_capital]  
    for trade in closed_trades:
        trade_return_ratio = trade['实际盈亏比例'] / 100.0
        current_capital = current_capital * (1 + trade_return_ratio)
        capital_curve.append(current_capital)

    capital_array = np.array(capital_curve)
    running_max = np.maximum.accumulate(capital_array)
    drawdowns = (capital_array - running_max) / running_max
    max_dd = np.min(drawdowns) * 100
    total_return_pct = (current_capital - init_cash) / init_cash * 100

    return {
        "初始总资金": init_cash, "最终总资金": current_capital, "参与复利交易笔数": len(closed_trades),
        "理论复利总收益率(%)": total_return_pct, "复利资金曲线最大回撤(%)": max_dd
    }

def analyze_loss_periods(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty: return pd.DataFrame()
    df = trades_df.copy()
    df['日期'] = pd.to_datetime(df['交易时间']).dt.date
    daily_stats = df[df['单笔盈亏'] != 0].groupby('日期')['单笔盈亏'].agg(['count', 'sum']).reset_index()
    daily_stats.columns = ['日期', '成交笔数', '当日净损益']

    profit_days = daily_stats[daily_stats['当日净损益'] > 0].copy().sort_values(by='当日净损益', ascending=False).reset_index(drop=True)
    profit_days.columns = ['盈利日期', '盈利单数', '盈利金额']

    loss_days = daily_stats[daily_stats['当日净损益'] < 0].copy().sort_values(by='当日净损益', ascending=True).reset_index(drop=True)
    loss_days.columns = ['亏损日期', '亏损单数', '亏损金额']

    combined_df = pd.concat([profit_days, loss_days], axis=1)
    return combined_df

def analyze_by_buy_date(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty: return pd.DataFrame()
    trades_df = trades_df.sort_values('交易时间').reset_index(drop=True)
    buy_queue = defaultdict(deque)  
    buy_date_summary = {}  

    for _, row in trades_df.iterrows():
        code = row['股票代码']
        ttype = row['交易类型']

        if ttype == '买入':
            buy_queue[code].append((row['交易时间'], row['数量']))
        elif '卖出' in ttype:
            if code in buy_queue and buy_queue[code]:
                buy_time, _ = buy_queue[code].popleft()
                pnl = row['单笔盈亏']
                buy_date = pd.to_datetime(buy_time).date()
                if buy_date not in buy_date_summary:
                    buy_date_summary[buy_date] = {'盈利金额': 0.0, '盈利单数': 0, '亏损金额': 0.0, '亏损单数': 0}
                if pnl > 0:
                    buy_date_summary[buy_date]['盈利金额'] += pnl
                    buy_date_summary[buy_date]['盈利单数'] += 1
                else:
                    buy_date_summary[buy_date]['亏损金额'] += pnl  
                    buy_date_summary[buy_date]['亏损单数'] += 1
            else: pass

    rows = []
    for date, vals in buy_date_summary.items():
        rows.append({
            '日期': date, '盈利单数': vals['盈利单数'], '盈利金额': vals['盈利金额'],
            '亏损单数': vals['亏损单数'], '亏损金额': vals['亏损金额']  
        })
    df = pd.DataFrame(rows)
    if df.empty: return df

    profit_days = df[df['盈利金额'] > 0][['日期', '盈利单数', '盈利金额']].copy().rename(columns={'日期': '盈利日期'}).sort_values('盈利金额', ascending=False).reset_index(drop=True)
    loss_days = df[df['亏损金额'] < 0][['日期', '亏损单数', '亏损金额']].copy().rename(columns={'日期': '亏损日期'}).sort_values('亏损金额', ascending=True).reset_index(drop=True)  
    return pd.concat([profit_days, loss_days], axis=1)

if __name__ == "__main__":
    stock_list = parse_tdx_blk_file(BLOB_FILE_PATH)
    
    if not stock_list:
        print("未提取到股票代码，退出程序")
    else:
        stock_summary_result, trades_detail_result, total_trades_metrics = batch_backtest(
            stock_codes=stock_list, init_cash=100000.0, commission=0.0003, stop_loss_ratio=0.01   
        )
        trades_list = trades_detail_result.to_dict('records') if not trades_detail_result.empty else []
        compounding_metrics = calculate_pure_compounding(trades_list, init_cash=100000.0)
        
        print("\n" + "="*80)
        print("                    板块回测（无视重叠的极限复利）汇总")
        print("="*80)
        for k, v in compounding_metrics.items():
            if "(%)" in k: print(f"{k}: {v:.2f}%")
            elif "资金" in k: print(f"{k}: {v:,.2f} 元")
            else: print(f"{k}: {v}")
        print("="*80)
        
        loss_dates_df = analyze_loss_periods(trades_detail_result)
        t = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(time.time()))
        buy_date_stats = analyze_by_buy_date(trades_detail_result)
        
        if not stock_summary_result.empty:
            with pd.ExcelWriter(f"板块回测汇总结果_含总笔数{t}.xlsx", engine="openpyxl") as writer:
                stock_summary_result.to_excel(writer, sheet_name="单股票维度汇总", index=False)
                trades_detail_result.to_excel(writer, sheet_name="所有交易明细", index=False)
                pd.DataFrame([total_trades_metrics]).to_excel(writer, sheet_name="总交易笔数维度汇总", index=False)
                if loss_dates_df is not None and not loss_dates_df.empty:
                    loss_dates_df.to_excel(writer, sheet_name="亏损日期提取", index=False)
                if not buy_date_stats.empty:
                    buy_date_stats.to_excel(writer, sheet_name="按买入日期统计", index=False)
            print(f"\n汇总结果已保存到: 板块回测汇总结果_含总笔数{t}.xlsx")