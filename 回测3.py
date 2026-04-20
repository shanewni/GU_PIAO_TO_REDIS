import backtrader as bt
import pandas as pd
import numpy as np
import datetime
from mootdx.reader import Reader
import gupiaojichu  # 你的自定义模块
import backtrader as bt
import pandas as pd
import os
from datetime import datetime
from typing import Callable, Dict, List, Tuple

def get_exact_tdx_30min(symbol, tdx_path="", start_date=None, end_date=None):
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
    
def get_local_day_data(symbol, tdx_path="", start_date=None, end_date=None):
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

class TDXPandasData(bt.feeds.PandasData):
    lines = ('amount',)
    params = (
        ('datetime', None),
        ('open', '开盘价'),
        ('high', '最高价'),
        ('low', '最低价'),
        ('close', '收盘价'),
        ('volume', '成交量'),
        ('amount', '成交额'),
        ('openinterest', -1),
    )

class ThreeBuyVariantStrategy(bt.Strategy):
    params = (
        ('ma_period', 60),
        ('stop_loss_ratio', 0.01),
        ('lookback_window', 250),
    )

    def __init__(self):
        self.data_30m = self.datas[0]
        self.data_day = self.datas[1]

        self.ma60_30m = bt.indicators.SMA(self.data_30m.close, period=self.p.ma_period)
        self.ma60_day = bt.indicators.SMA(self.data_day.close, period=self.p.ma_period)
        
        # --- 核心修复 1: 增加订单状态追踪，防止重复触发买卖 ---
        self.order = None               
        
        self.active_stop_loss = None
        self.buy_price = None
        self.buy_bar_index = 0
        self.risk_per_trade = 0.0

    def next(self):
        # 1. 如果有未完成的订单，直接跳过，防止重复下单
        if self.order:
            return
            
        if len(self.data_day) < 60:
            return

        current_idx = len(self)
        current_close = self.data_30m.close[0]
        current_low = self.data_30m.low[0]

        # --- A. 持仓状态下的卖出逻辑 ---
        if self.position:
            hold_count = current_idx - self.buy_bar_index
            profit_ratio = (current_close - self.buy_price) / self.buy_price

            # 1. 硬止损检查
            if current_low <= self.active_stop_loss:
                self.order = self.sell(size=self.position.size)
                self.log(f"触发硬止损，止损价: {self.active_stop_loss:.2f}")
                return

            # 2. 均线动态止盈/止损
            if (self.data_30m.close[0] < self.ma60_30m[0] and 
                self.data_30m.close[-1] < self.ma60_30m[-1]):
                if profit_ratio >= 0.10 or hold_count >= 30:
                    self.order = self.sell(size=self.position.size)
                    self.log(f"均线平仓: 收益 {profit_ratio:.1%}, 持仓 {hold_count} 根")
                    return

            # 3. 形态平仓判断
            if hold_count > 3:
                high_full = self.data_30m.high.get(size=self.p.lookback_window)
                low_full = self.data_30m.low.get(size=self.p.lookback_window)
                
                if len(high_full) >= self.p.lookback_window:
                    frac_window = gupiaojichu.identify_turns(len(high_full), list(high_full), list(low_full))
                    bottom_indices = [i for i, val in enumerate(frac_window) if val == -1.0]
                    top_indices = [i for i, val in enumerate(frac_window) if val == 1.0]
                    
                    if bottom_indices:
                        last_bottom_idx = bottom_indices[-1]
                        
                        # 逻辑 2：跌破最后底分型最低价
                        if low_full[-1] < low_full[last_bottom_idx]:
                            self.order = self.sell(size=self.position.size)
                            self.log(f"形态平仓: 跌破最后底分型最低价({low_full[last_bottom_idx]:.2f})")
                            return

                        # --- 核心修复 2：优化底分型后反弹无力的逻辑，防止过早卖出 ---
                        valid_tops = [i for i in top_indices if i < last_bottom_idx]
                        if valid_tops:
                            prev_top_idx = valid_tops[-1]
                            current_win_idx = len(high_full) - 1
                            
                            drop_time = last_bottom_idx - prev_top_idx
                            rebound_time = current_win_idx - last_bottom_idx
                            
                            # 增加容错时间：反弹时间必须 >= 下跌时间，且至少给予5根K线的反弹确认窗口
                            time_cond = rebound_time >= max(drop_time, 1) 
                            dist_cond = drop_time >= 3
                            
                            if time_cond and dist_cond:
                                if max(high_full[last_bottom_idx:]) <= high_full[prev_top_idx]:
                                    self.order = self.sell(size=self.position.size)
                                    self.log(f"形态平仓: 底分型后反弹无力(未过前高 {high_full[prev_top_idx]:.2f})")
                                    return

        # --- B. 空仓状态下的买入逻辑 ---
        else:
            day_cond = (self.data_day.close[-1] > self.ma60_day[-1] and 
                        self.ma60_day[-1] > self.ma60_day[-4])
            
            if not day_cond:
                return

            high_array = self.data_30m.high.get(size=self.p.lookback_window)
            low_array = self.data_30m.low.get(size=self.p.lookback_window)
            close_array = self.data_30m.close.get(size=self.p.lookback_window)
            open_array = self.data_30m.open.get(size=self.p.lookback_window)

            if len(high_array) < self.p.lookback_window:
                return

            frac_window = gupiaojichu.identify_turns(len(high_array), list(high_array), list(low_array))
            
            try:
                window_signal = self.three_buy_variant(frac_window, list(high_array), list(low_array))
            except Exception as e:
                self.log(f"信号计算异常: {e}")
                return

            if window_signal[-1] == 1.0:
                last_top_idx = -1
                for i in range(len(frac_window) - 2, -1, -1):
                    if frac_window[i] == 1.0:
                        last_top_idx = i
                        break
                
                if last_top_idx != -1 and current_close <= high_array[last_top_idx]:
                    return
                
                loss_price = min(current_low, high_array[-2])
                close_price = current_close
                
                price_diff_pct = (close_price - loss_price) / loss_price * 100
                if price_diff_pct > 3 or price_diff_pct < 0.5:
                    return
                
                if current_close <= open_array[-1]:
                    return
                
                self.active_stop_loss = loss_price
                
                if current_close <= self.active_stop_loss:
                    return

                cash = self.broker.get_cash()
                self.risk_per_trade = cash * self.p.stop_loss_ratio
                loss_per_share = current_close - self.active_stop_loss
                
                max_shares = self.risk_per_trade / loss_per_share
                buy_size = int(max_shares // 100 * 100)

                comm_info = self.broker.getcommissioninfo(self.data_30m)
                comm_rate = comm_info.p.commission
                max_affordable = cash / (current_close * (1 + comm_rate))
                buy_size = min(buy_size, int(max_affordable // 100 * 100))

                if buy_size > 0:
                    # 将订单状态赋值给 self.order
                    self.order = self.buy(size=buy_size)
                    self.log(f"触发买入，数量: {buy_size}, 止损设定为: {self.active_stop_loss:.2f}")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.buy_price = order.executed.price
                self.buy_bar_index = len(self)
                self.log(f"买单成交, 价格: {order.executed.price:.2f}, 费用: {order.executed.comm:.2f}")
            elif order.issell():
                self.log(f"卖单成交, 价格: {order.executed.price:.2f}, 利润: {order.executed.pnl:.2f}")
                self.buy_price = None
            self.order = None  # 订单完成，重置状态

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('订单取消/资金不足/被拒绝')
            self.order = None  # 异常状态，同样重置

    def log(self, txt, dt=None):
        dt = dt or self.data_30m.datetime.datetime(0)
        print(f'[{dt}] {txt}')

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
        down_seg_start, down_seg_end = down_segments[0]
        up_seg_start, up_seg_end = up_segments[0]
        if len(down_segments) > 0:
            if up_seg_end <= down_seg_end:
                return pf_out  # 最近的向下线段结束位置过近，不满足
            if down_seg_start+6 > up_seg_end:
                return pf_out  # 最近的向上线段过短，不满足
            i = down_seg_end
            if i < last_k_idx:
                while i < last_k_idx:
                    if high[i] >= latest_high:
                        return pf_out  # 向上线段中有价格高于最近高点，不满足
                    i += 1
        
        if last_k_idx -1 <= down_seg_end:
            return pf_out  # 最后一根K线过近，不满足
        
        # 所有条件满足，标记信号
        pf_out[last_k_idx] = 1.0
        return pf_out

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

def run_batch_backtest(symbols: List[str], tdx_path, start_date, end_date, output_file="backtest_results.xlsx"):
    """
    批量执行回测并将结果保存到 Excel 文件
    """
    all_results = []

    for symbol in symbols:
        print(f"\n{'='*20} 正在回测: {symbol} {'='*20}")
        
        try:
            # 1. 获取数据
            df_30m = get_exact_tdx_30min(symbol, tdx_path, start_date, end_date)
            df_day = get_local_day_data(symbol, tdx_path, start_date, end_date)
            
            if df_30m.empty or df_day.empty:
                print(f"跳过 {symbol}: 数据获取失败")
                continue

            # 2. 初始化 Cerebro
            cerebro = bt.Cerebro()
            cerebro.broker.setcash(100000.0)
            cerebro.broker.setcommission(commission=0.0003)

            data_30m = TDXPandasData(dataname=df_30m, timeframe=bt.TimeFrame.Minutes, compression=30)
            data_day = TDXPandasData(dataname=df_day, timeframe=bt.TimeFrame.Days)

            cerebro.adddata(data_30m, name='30m')
            cerebro.adddata(data_day, name='day')

            # 3. 添加策略与分析器
            cerebro.addstrategy(ThreeBuyVariantStrategy)
            cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
            cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
            cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')

            # 4. 运行回测
            strat_runs = cerebro.run()
            strat = strat_runs[0]

            # 5. 提取统计指标
            analysis_trades = strat.analyzers.trades.get_analysis()
            analysis_dd = strat.analyzers.drawdown.get_analysis()
            analysis_ret = strat.analyzers.returns.get_analysis()

            res = {
                "代码": symbol,
                "期末资产": round(cerebro.broker.get_value(), 2),
                "总收益率(%)": round(analysis_ret.get('rtot', 0) * 100, 2),
                "最大回撤(%)": round(analysis_dd.max.drawdown, 2),
                "交易次数": 0,
                "胜率(%)": 0.0,
                "平均盈利": 0.0,
            }

            if 'total' in analysis_trades and analysis_trades.total.closed > 0:
                total_closed = analysis_trades.total.closed
                won = analysis_trades.won.total
                res["交易次数"] = total_closed
                res["胜率(%)"] = round(won / total_closed * 100, 2)
                res["平均盈利"] = round(analysis_trades.pnl.net.average, 2)

            all_results.append(res)
            print(f"完成 {symbol}: 收益 {res['总收益率(%)']}%")

        except Exception as e:
            print(f"股票 {symbol} 回测期间发生错误: {str(e)}")
            continue

    # 6. 保存到文件
    if all_results:
        final_df = pd.DataFrame(all_results)
        final_df.to_excel(output_file, engine="openpyxl")
        print(f"\n{'#'*20} 所有回测完成 {'#'*20}")
        print(f"结果已保存至: {os.path.abspath(output_file)}")
        print(final_df)
    else:
        print("没有产生任何有效回测结果。")

BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\SSYNYS.blk"
# --- 执行入口 ---
if __name__ == '__main__':
    # 你可以从 Excel 或 数据库读取股票列表
    stock_list = parse_tdx_blk_file(BLOB_FILE_PATH)
    
    PATH_TDX = r"D:\zd_hbzq"  # 请确保路径正确
    START = '2023-01-01'
    END = '2026-04-15'
    
    run_batch_backtest(
        symbols=stock_list, 
        tdx_path=PATH_TDX, 
        start_date=START, 
        end_date=END,
        output_file=f"回测结果_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    )