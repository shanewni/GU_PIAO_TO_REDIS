import backtrader as bt
import pandas as pd
import numpy as np
import datetime
from mootdx.reader import Reader
import gupiaojichu  # 你的自定义模块

def get_exact_tdx_30min(symbol, tdx_path="", start_date=None, end_date=None):
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
    # --- 新增：时间段过滤 ---
    if start_date:
        df_30m = df_30m[df_30m.index >= pd.to_datetime(start_date)]
    if end_date:
        df_30m = df_30m[df_30m.index <= pd.to_datetime(end_date)]
        
    print(f"成功合成 {symbol} 30min数据，时间段：{start_date or '不限'} - {end_date or '不限'}，共 {len(df_30m)} 条")
    return df_30m
    
def get_local_day_data(symbol, tdx_path="", start_date=None, end_date=None):
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
    
    df_day.index = pd.to_datetime(df_day.index)
        
    # --- 新增：时间段过滤逻辑 ---
    if start_date:
        df_day = df_day[df_day.index >= pd.to_datetime(start_date)]
    if end_date:
        df_day = df_day[df_day.index <= pd.to_datetime(end_date)]
    
    price_cols = ['开盘价', '最高价', '最低价', '收盘价']
    df_day[price_cols] = df_day[price_cols].round(2)
    
    print(f"成功读取 {symbol} 日线({start_date or '起点'}至{end_date or '至今'})，共 {len(df_day)} 条")
    return df_day

# --- 1. 自定义 Pandas 数据源适配器 ---
class TDXPandasData(bt.feeds.PandasData):
    """适配通达信数据的 Backtrader DataFeed"""
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

# --- 2. 核心交易策略 ---
class ThreeBuyVariantStrategy(bt.Strategy):
    params = (
        ('ma_period', 60),
        ('stop_loss_ratio', 0.01),  # 总资金止损比例 1%
        ('lookback_window', 250),   # 缠论线段向前回溯的K线数
    )

    def __init__(self):
        # 1. 引用数据源 (约定 datas[0] 为 30min, datas[1] 为 Day)
        self.data_30m = self.datas[0]
        self.data_day = self.datas[1]

        # 2. 初始化指标
        self.ma60_30m = bt.indicators.SMA(self.data_30m.close, period=self.p.ma_period)
        self.ma60_day = bt.indicators.SMA(self.data_day.close, period=self.p.ma_period)
        
        # 3. 状态管理变量
        self.active_stop_loss = None    # 动态止损价
        self.buy_price = None           # 买入均价
        self.buy_bar_index = 0          # 买入时的K线索引
        self.risk_per_trade = 0.0       # 单笔风险金额

    def next(self):
        # 等待均线指标生成
        # 1. 严格检查指标就绪情况
        # 如果日线数据还没跑到能算出 MA60 的位置，直接跳过
        if len(self.data_day) < 60:
            return

        # 2. 打印调试（仅针对你关心的 09.26 那天）
        curr_dt = self.data_30m.datetime.date(0)
        if curr_dt == datetime.date(2024, 5, 6):
            # 获取前一交易日的日线收盘和日线MA60
            # 注意：在30min周期内看日线，[-1]代表上一根完整的日线
            print(f"--- 调试 0926 信号 ---")
            print(f"当前30min时间: {self.data_30m.datetime.datetime(0)}")
            print(f"对应日线收盘: {self.data_day.close[-1]:.2f}, 日线MA60: {self.ma60_day[-1]:.2f}")
            print(f"当前现金: {self.broker.get_cash():.2f}")

        # 3. 买入过滤条件建议优化为：
        # 使用 [-1] 获取“已确认”的昨日指标，避免在日线未收盘时产生漂移
        day_ma_up = self.ma60_day[-1] > self.ma60_day[-4] # 对应你原代码的 shift(3)
        day_close_above = self.data_day.close[-1] > self.ma60_day[-1]

        if not (day_ma_up and day_close_above):
            return

        current_idx = len(self)
        current_close = self.data_30m.close[0]
        current_low = self.data_30m.low[0]
        # --- A. 持仓状态下的卖出逻辑 ---
        if self.position:
            # 持仓K线数
            hold_count = current_idx - self.buy_bar_index
            profit_ratio = (current_close - self.buy_price) / self.buy_price

            # 1. 硬止损检查 (优先级最高)
            if current_low <= self.active_stop_loss:
                self.sell(size=self.position.size)
                self.log(f"触发硬止损，止损价: {self.active_stop_loss:.2f}")
                return

            # 2. 均线动态止盈/止损 (连续两根低于MA60，且涨幅>10%或持仓>30根)
            if (self.data_30m.close[0] < self.ma60_30m[0] and 
                self.data_30m.close[-1] < self.ma60_30m[-1]):
                
                if profit_ratio >= 0.10 or hold_count >= 30:
                    self.sell(size=self.position.size)
                    self.log(f"均线平仓: 收益 {profit_ratio:.1%}, 持仓 {hold_count} 根")
                    return

            # 3. 形态平仓判断
            if hold_count > 3:
                high_full = self.data_30m.high.get(size=self.p.lookback_window)
                low_full = self.data_30m.low.get(size=self.p.lookback_window)
                
                if len(high_full) >= self.p.lookback_window:
                    # 获取分型
                    frac_window = gupiaojichu.identify_turns(len(high_full), list(high_full), list(low_full))
                    bottom_indices = [i for i, val in enumerate(frac_window) if val == -1.0]
                    top_indices = [i for i, val in enumerate(frac_window) if val == 1.0]
                    
                    if bottom_indices:
                        last_bottom_idx = bottom_indices[-1]
                        
                        # --- 逻辑 2：跌破最后底分型最低价 (保留) ---
                        if low_full[-1] < low_full[last_bottom_idx]:
                            self.sell(size=self.position.size)
                            self.log(f"形态平仓: 跌破最后底分型最低价({low_full[last_bottom_idx]:.2f})")
                            return

                        # --- 逻辑 1：底分型后未突破前高且距离达标 (新增) ---
                        valid_tops = [i for i in top_indices if i < last_bottom_idx]
                        if valid_tops:
                            prev_top_idx = valid_tops[-1]
                            current_win_idx = len(high_full) - 1
                            
                            # 条件判断：底分型后的运行时间 >= 底到顶的时间 且 底顶跨度 >= 3 根 K 线
                            time_cond = (current_win_idx - last_bottom_idx) >= (last_bottom_idx - prev_top_idx)
                            dist_cond = (last_bottom_idx - prev_top_idx) >= 3
                            
                            if time_cond and dist_cond:
                                # 判断底分型后的反弹高度是否未能突破前顶
                                if max(high_full[last_bottom_idx:]) <= high_full[prev_top_idx]:
                                    self.sell(size=self.position.size)
                                    self.log(f"形态平仓: 底分型后反弹无力(未过前高 {high_full[prev_top_idx]:.2f})")
                                    return

        # --- B. 空仓状态下的买入逻辑 ---
        else:
            # 1. 日线级别过滤 (避免未来函数，使用 -1 即前一日的数据)
            # 条件：昨日收盘价 > 昨日MA60 且 昨日MA60 > 3日前的MA60
            day_cond = (self.data_day.close[-1] > self.ma60_day[-1] and 
                        self.ma60_day[-1] > self.ma60_day[-4])
            
            if not day_cond:
                return

            # 2. 准备30分钟形态数据
            high_array = self.data_30m.high.get(size=self.p.lookback_window)
            low_array = self.data_30m.low.get(size=self.p.lookback_window)
            close_array = self.data_30m.close.get(size=self.p.lookback_window)
            open_array = self.data_30m.open.get(size=self.p.lookback_window)

            if len(high_array) < self.p.lookback_window:
                return

            # 3. 调用原有的转折点和三买变体逻辑
            frac_window = gupiaojichu.identify_turns(len(high_array), list(high_array), list(low_array))
            
            try:
                # 这里假设 TdxStockBacktest 的静态方法已被移出或可以直接调用
                # window_signal 返回一个列表，我们只看最后一个元素是否为 1.0
                window_signal = self.three_buy_variant(frac_window, list(high_array), list(low_array))
            except Exception:
                return

            if window_signal[-1] == 1.0:
                # 收盘价前顶分型确认逻辑
                last_top_idx = -1
                for i in range(len(frac_window) - 2, -1, -1):
                    if frac_window[i] == 1.0:
                        last_top_idx = i
                        break
                
                if last_top_idx != -1 and current_close <= high_array[last_top_idx]:
                    return # 收盘价未突破前高，撤销信号
                
                # 假设按照你之前的逻辑：当前K线低点与前一根高点的较小值
                loss_price = min(current_low, high_array[-2])
                close_price = current_close
                # A. 盈亏比/幅度过滤
                # 如果 (收盘价-止损价)/止损价 > 3% 或 < 0.5%，则取消信号
                price_diff_pct = (close_price - loss_price) / loss_price * 100
                if price_diff_pct > 3:
                    return  # 对应 window_signal[-1] = 0.0
                elif price_diff_pct < 0.5:
                    return  # 对应 window_signal[-1] = 0.0
                
                # 实体阴线过滤
                if current_close <= open_array[-1]:
                    return
                
                # --- 核心：以损定量仓位计算 ---
                # 初始止损价：当前K线最低价与前一根最高价的较小值
                self.active_stop_loss = loss_price
                
                # 避免除以0或反向
                if current_close <= self.active_stop_loss:
                    return

                # 计算可买股数
                cash = self.broker.get_cash()
                self.risk_per_trade = cash * self.p.stop_loss_ratio
                loss_per_share = current_close - self.active_stop_loss
                
                max_shares = self.risk_per_trade / loss_per_share
                buy_size = int(max_shares // 100 * 100) # 取整手

                # 资金校验预留手续费
                # 获取当前 30 分钟数据的佣金信息对象
                comm_info = self.broker.getcommissioninfo(self.data_30m)
                # 提取具体的佣金比例（浮点数）
                comm_rate = comm_info.p.commission
                # 计算包含手续费的最大可买数量
                max_affordable = cash / (current_close * (1 + comm_rate))
                buy_size = min(buy_size, int(max_affordable // 100 * 100))

                if buy_size > 0:
                    self.buy(size=buy_size)
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

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('订单取消/资金不足/被拒绝')

    def log(self, txt, dt=None):
        dt = dt or self.data_30m.datetime.datetime(0)
        print(f'[{dt}] {txt}')

    # --- 植入你的静态算法 ---
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

# --- 3. 回测执行引擎 ---
def run_backtrader(symbol: str, tdx_path: str, start_date: str, end_date: str):
    cerebro = bt.Cerebro()

    # 1. 设置初始资金与佣金 (双边万三)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.0003)

    # 2. 使用原有逻辑获取 Pandas DataFrame 数据
    # 此处假设原有的 get_exact_tdx_30min 和 get_local_day_data 方法可用
    print(f"正在加载 {symbol} 数据...")
    reader = Reader.factory(market='std', tdxdir=tdx_path)
    
    # 获取并清洗数据（需确保返回的 df 索引为 datetime 且列名包含 开盘价 等）
    df_30m = get_exact_tdx_30min(symbol, tdx_path, start_date, end_date) # 需引用你的原函数
    df_day = get_local_day_data(symbol, tdx_path, start_date, end_date)  # 需引用你的原函数
    
    if df_30m.empty or df_day.empty:
        print("数据加载失败。")
        return

    # 3. 将 DataFrame 转换为 Backtrader DataFeed 并加载
    data_30m = TDXPandasData(dataname=df_30m, timeframe=bt.TimeFrame.Minutes, compression=30)
    data_day = TDXPandasData(dataname=df_day, timeframe=bt.TimeFrame.Days)

    cerebro.adddata(data_30m, name='30m')
    cerebro.adddata(data_day, name='day')

    # 4. 添加策略
    cerebro.addstrategy(ThreeBuyVariantStrategy, stop_loss_ratio=0.01)

    # 5. 添加分析器（对应你原有的胜率、回撤、收益分析）
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')

    # 6. 运行回测
    print(f"========== 启动回测 {symbol} ==========")
    print(f"期初总资金: {cerebro.broker.get_cash():.2f}")
    results = cerebro.run()
    strat = results[0]

    # 7. 打印评估指标
    print(f"期末总资金: {cerebro.broker.get_value():.2f}")
    
    analysis_trades = strat.analyzers.trades.get_analysis()
    if 'total' in analysis_trades and analysis_trades.total.closed > 0:
        total_closed = analysis_trades.total.closed
        won = analysis_trades.won.total
        lost = analysis_trades.lost.total
        win_rate = won / total_closed * 100
        print(f"总交易笔数: {total_closed}")
        print(f"胜率: {win_rate:.2f}%")
        print(f"最大回撤: {strat.analyzers.drawdown.get_analysis().max.drawdown:.2f}%")
    else:
        print("无闭合交易记录。")

    # cerebro.plot(style='candlestick') # 若需可视化可解开注释

if __name__ == '__main__':
    run_backtrader(symbol='002824', tdx_path=r"D:\zd_hbzq", start_date='2022-12-01', end_date='2026-05-28')