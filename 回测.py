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
    """基于pytdx的股票回测框架（支持多周期K线+止损策略+以损定量）"""
    
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
        :return: 格式化后的K线DataFrame
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
                return df
            
            # ========== 新增：提取最高价和最低价列表 ==========
            result_list = []
            result_list_high = []
            result_list_low = []
            for _, bar in df.iterrows():
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
            
            # 输出列表（可根据需要调整输出方式）
            print(f"\n【{code} {self._ktype2name(ktype)} 价格列表】")
            print(f"最高价列表(result_list_high): {result_list_high[:10]}... (共{len(result_list_high)}条)")
            print(f"最低价列表(result_list_low): {result_list_low[:10]}... (共{len(result_list_low)}条)")
            # ========== 新增结束 ==========
            
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
            return result_df,result_list_high,result_list_low
        except Exception as e:
            print(f"获取{self._ktype2name(ktype)}数据失败: {e}")
            return pd.DataFrame(),[],[]
    
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
    
    def get_multi_period_data(self, code: str, count: int = 700) -> Dict[str, pd.DataFrame]:
        """
        一键获取日线+30分钟线数据
        :param code: 股票代码
        :param count: 各周期获取的数据条数
        :return: 多周期数据字典 {'day': 日线, '30min': 30分钟线}
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
    
    def run_backtest(self, strategy_func: Callable, period: str = 'day', init_cash: float = 100000.0, 
                     commission: float = 0.0003, stop_loss_ratio: float = 0.02) -> pd.DataFrame:
        """
        执行指定周期的策略回测（新增以损定量：按总资金止损百分比计算仓位）
        :param strategy_func: 策略函数（输入data，输出买卖信号）
        :param period: 回测周期 ('day'=日线, '30min'=30分钟)
        :param init_cash: 初始资金
        :param commission: 交易佣金（默认0.03%）
        :param stop_loss_ratio: 总资金止损百分比（默认2%）
        :return: 回测结果DataFrame
        """
        # 检查指定周期数据是否存在
        if period not in self.stock_data or self.stock_data[period].empty:
            print(f"请先获取{self._ktype2name({'day':9, '30min':2}[period])}数据")
            return pd.DataFrame()
        
        # 初始化止损百分比
        self.stop_loss_ratio = stop_loss_ratio
        print(f"以损定量配置：单笔交易最大亏损 = 总资金 × {stop_loss_ratio*100}%")
        
        # 复制数据避免修改原数据
        data = self.stock_data[period].copy()
        
        # 初始化回测参数
        cash = init_cash  # 可用资金
        position = 0  # 持仓数量
        total_asset = init_cash  # 总资产（现金+持仓市值）
        trade_records = []  # 交易记录
        daily_results = []  # 每日/每30分钟结果
        trade_pnl = []      # 记录每笔交易的盈亏（用于计算盈亏比）
        
        # 止损相关初始化
        self.stop_loss_price = 0.0  # 止损价格（买入K线的最低价）
        self.in_position = False    # 是否持仓
        buy_kline_low = 0.0         # 买入K线的最低价
        buy_datetime = None         # 买入时间
        
        # 1. 运行策略函数，获取买卖信号（多周期策略需要传入日线数据）
        if period == '30min' and 'day' in self.stock_data:
            data = strategy_func(data, self.stock_data['day'])
        else:
            data = strategy_func(data)
        
        # 检查是否有信号列
        if 'signal' not in data.columns:
            print("策略函数必须返回包含'signal'列的DataFrame（signal: 1=买入, -1=卖出, 0=持有）")
            return pd.DataFrame()
        
        # 2. 逐行执行回测（新增以损定量+止损逻辑）
        buy_price = 0  # 记录买入价格
        buy_fee = 0    # 记录买入手续费
        
        for datetime, row in data.iterrows():
            close_price = row['收盘价']
            low_price = row['最低价']
            high_price = row['最高价']
            current_total_asset = cash + position * close_price  # 当前总资产
            
            # ===== 止损逻辑：触发止损则强制卖出 =====
            if self.in_position and low_price <= self.stop_loss_price:
                # 触发止损，以收盘价卖出全部持仓
                sell_num = position
                fee = sell_num * close_price * commission
                income = sell_num * close_price * (1 - commission)
                
                # 计算这笔止损交易的盈亏
                total_buy_cost = sell_num * buy_price + buy_fee  # 买入总成本（含手续费）
                total_sell_income = income - fee                # 卖出总收入（扣手续费）
                pnl = total_sell_income - total_buy_cost        # 单笔盈亏
                
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
                    '单笔风险金额': self.risk_per_trade,  # 新增：记录单笔风险金额
                    '实际亏损比例': abs(pnl)/current_total_asset*100  # 新增：实际亏损占总资金比例
                })
                trade_pnl.append(pnl)
                
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
                    current_cash=current_total_asset,  # 基于当前总资产计算
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
                        buy_price = close_price  # 记录买入价
                        buy_fee = fee            # 记录买入手续费
                        self.in_position = True  # 标记为持仓状态
                        buy_datetime = datetime
                        
                        trade_records.append({
                            '时间': datetime,
                            '操作': '买入',
                            '价格': close_price,
                            '数量': buy_num,
                            '费用': fee,
                            '买入K线最低价': buy_kline_low,
                            '设置止损价': self.stop_loss_price,
                            '单笔风险金额': self.risk_per_trade,  # 新增：记录单笔风险金额
                            '风险比例': self.stop_loss_ratio*100,  # 新增：记录风险比例
                        })
                        print(f"【买入开仓（以损定量）】{datetime} - 价格{close_price}，数量{buy_num}")
                        print(f"          - 止损价:{self.stop_loss_price} | 单笔风险金额:{self.risk_per_trade:.2f} | 风险比例:{self.stop_loss_ratio*100}%")
                else:
                    print(f"【买入失败】{datetime} - 以损定量计算可买数量为0（止损价{self.stop_loss_price} >= 买入价{close_price}）")
            
            # ===== 正常卖出信号执行 =====
            elif row['signal'] == -1 and position > 0:
                # 卖出全部持仓
                sell_num = position
                # 计算交易收入
                fee = sell_num * close_price * commission
                income = sell_num * close_price * (1 - commission)
                # 计算这笔交易的盈亏
                total_buy_cost = sell_num * buy_price + buy_fee  # 买入总成本（含手续费）
                total_sell_income = income - fee                # 卖出总收入（扣手续费）
                pnl = total_sell_income - total_buy_cost        # 单笔盈亏
                
                cash += income
                trade_records.append({
                    '时间': datetime,
                    '操作': '策略卖出',
                    '价格': close_price,
                    '数量': sell_num,
                    '费用': fee,
                    '单笔盈亏': pnl,
                    '单笔风险金额': self.risk_per_trade,  # 新增：记录单笔风险金额
                    '实际盈亏比例': pnl/current_total_asset*100  # 新增：实际盈亏占总资金比例
                })
                # 存储单笔盈亏用于计算盈亏比
                trade_pnl.append(pnl)
                
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
                '单笔风险金额': self.risk_per_trade if self.in_position else 0.0  # 新增：每日风险金额
            })
        
        # 整理回测结果
        self.backtest_result = pd.DataFrame(daily_results)
        self.backtest_result = self.backtest_result.set_index('时间')
        
        # 添加交易记录
        self.trade_records = pd.DataFrame(trade_records)
        # 存储单笔盈亏列表
        self.trade_pnl = trade_pnl
        
        # 计算核心指标（包含盈亏比）
        self.calc_backtest_metrics(init_cash)
        
        print(f"{self._ktype2name({'day':9, '30min':2}[period])}回测完成！")
        return self.backtest_result
    
    def calc_backtest_metrics(self, init_cash: float) -> Dict:
        """计算回测核心指标（新增以损定量相关统计）"""
        if self.backtest_result is None:
            return {}
        
        # 累计收益
        total_profit = self.backtest_result['总资产'].iloc[-1] - init_cash
        total_return = total_profit / init_cash * 100
        
        # 年化收益（按250个交易日/1440个30分钟周期（240个交易日*6个30分钟）计算）
        time_delta = (self.backtest_result.index[-1] - self.backtest_result.index[0])
        if '30min' in self.stock_data and not self.stock_data['30min'].empty:
            # 30分钟周期年化：按每年1440个30分钟交易时段（240天*6个30分钟）
            total_periods = time_delta.total_seconds() / 1800  # 总30分钟数
            annual_return = (pow((self.backtest_result['总资产'].iloc[-1] / init_cash), 1440/total_periods) - 1) * 100 if total_periods !=0 else 0
        else:
            # 日线周期年化
            days = time_delta.days
            annual_return = (pow((self.backtest_result['总资产'].iloc[-1] / init_cash), 365/days) - 1) * 100 if days !=0 else 0
        
        # 最大回撤
        max_drawdown = self.calc_max_drawdown(self.backtest_result['总资产'].values)
        
        # 胜率（盈利交易数/总交易数）
        win_rate = 0
        total_trades = 0
        if len(self.trade_records) > 0:
            win_trades = 0
            buy_price = 0
            # 区分正常交易和止损交易，都计入完整交易次数
            total_trades = sum(1 for _, trade in self.trade_records.iterrows() if trade['操作'] in ['策略卖出', '止损卖出'])
            # 统计盈利交易数
            for _, trade in self.trade_records.iterrows():
                if '单笔盈亏' in trade and trade['单笔盈亏'] > 0:
                    win_trades += 1
            win_rate = win_trades / total_trades * 100 if total_trades >=1 else 0
        
        # 盈亏比计算
        profit_loss_ratio = 0.0
        if hasattr(self, 'trade_pnl') and len(self.trade_pnl) > 0:
            # 分离盈利和亏损交易
            profitable_trades = [pnl for pnl in self.trade_pnl if pnl > 0]
            losing_trades = [abs(pnl) for pnl in self.trade_pnl if pnl < 0]
            
            if len(profitable_trades) > 0 and len(losing_trades) > 0:
                # 平均盈利 / 平均亏损 = 盈亏比
                avg_profit = np.mean(profitable_trades)
                avg_loss = np.mean(losing_trades)
                profit_loss_ratio = round(avg_profit / avg_loss, 2)
            elif len(profitable_trades) > 0 and len(losing_trades) == 0:
                # 只有盈利交易，盈亏比设为无穷大
                profit_loss_ratio = float('inf')
        
        # 新增：止损相关统计
        stop_loss_count = 0
        strategy_sell_count = 0
        total_risk_amount = 0.0  # 累计风险金额
        actual_loss_amount = 0.0 # 实际止损亏损总额
        if len(self.trade_records) > 0:
            stop_loss_count = sum(1 for _, trade in self.trade_records.iterrows() if trade['操作'] == '止损卖出')
            strategy_sell_count = sum(1 for _, trade in self.trade_records.iterrows() if trade['操作'] == '策略卖出')
            # 统计累计风险金额和实际亏损
            total_risk_amount = self.trade_records['单笔风险金额'].sum()
            stop_loss_trades = self.trade_records[self.trade_records['操作'] == '止损卖出']
            if not stop_loss_trades.empty:
                actual_loss_amount = stop_loss_trades['单笔盈亏'].sum()  # 亏损为负数
        
        # 存储指标（新增以损定量相关）
        self.metrics = {
            '初始资金': init_cash,
            '最终总资产': self.backtest_result['总资产'].iloc[-1],
            '累计收益': total_profit,
            '累计收益率(%)': round(total_return, 2),
            '年化收益率(%)': round(annual_return, 2),
            '最大回撤(%)': round(max_drawdown, 2),
            '总交易记录数': len(self.trade_records),
            '完整交易次数': total_trades,
            '策略卖出次数': strategy_sell_count,
            '止损卖出次数': stop_loss_count,
            '胜率(%)': round(win_rate, 2),
            '盈亏比': profit_loss_ratio,
            '止损百分比设置(%)': round(self.stop_loss_ratio*100, 2),  # 新增
            '累计风险金额': round(total_risk_amount, 2),            # 新增
            '实际止损亏损总额': round(actual_loss_amount, 2),       # 新增
            '平均单笔风险金额': round(total_risk_amount/max(total_trades,1), 2)  # 新增
        }
        
        # 打印指标
        print("\n===== 回测指标 =====")
        for k, v in self.metrics.items():
            print(f"{k}: {v}")
        
        return self.metrics
    
    @staticmethod
    def calc_max_drawdown(asset_values: np.ndarray) -> float:
        """计算最大回撤"""
        peak = asset_values[0]
        max_dd = 0
        for value in asset_values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak * 100
            if drawdown > max_dd:
                max_dd = drawdown
        return max_dd
    
    def plot_result(self, period: str = 'day'):
        """可视化指定周期的回测结果（新增以损定量相关标注）"""
        if self.backtest_result is None:
            print("无回测结果可展示")
            return
        
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
        
        # 子图1：价格、总资产和止损价
        ax1.plot(self.backtest_result.index, self.backtest_result['收盘价'], label=f'{period}价格', color='blue', alpha=0.7)
        # 绘制止损价格线（仅在持仓期间显示）
        stop_loss_series = self.backtest_result['止损价格']
        stop_loss_series = stop_loss_series[stop_loss_series > 0]
        if not stop_loss_series.empty:
            ax1.plot(stop_loss_series.index, stop_loss_series.values, 
                    label='止损价格', color='red', linestyle='--', alpha=0.8)
        
        ax1_twin = ax1.twinx()
        ax1_twin.plot(self.backtest_result.index, self.backtest_result['总资产'], label='总资产', color='red')
        ax1.set_ylabel('价格 (元)')
        ax1_twin.set_ylabel('总资产 (元)')
        ax1.set_title(f'{self._ktype2name({"day":9, "30min":2}[period])}价格、止损价与总资产走势')
        ax1.legend(loc='upper left')
        ax1_twin.legend(loc='upper right')
        ax1.grid(True, alpha=0.3)
        
        # 子图2：累计收益率
        ax2.bar(self.backtest_result.index, self.backtest_result['累计收益率'], color='green', alpha=0.6)
        ax2.set_ylabel('累计收益率 (%)')
        ax2.set_title('累计收益率走势')
        ax2.grid(True, alpha=0.3)
        
        # 子图3：单笔风险金额（新增）
        risk_series = self.backtest_result['单笔风险金额']
        risk_series = risk_series[risk_series > 0]
        if not risk_series.empty:
            ax3.plot(risk_series.index, risk_series.values, label='单笔风险金额', color='orange', marker='o', alpha=0.8)
        ax3.set_ylabel('单笔风险金额 (元)')
        ax3.set_xlabel('时间')
        ax3.set_title(f'单笔风险金额走势（止损比例：{self.stop_loss_ratio*100}%）')
        ax3.legend(loc='upper right')
        ax3.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    def close(self):
        """断开通达信连接"""
        self.api.disconnect()
        print("已断开通达信服务器连接")


def multi_period_strategy(min30_data: pd.DataFrame, min30_high_list: list, min30_low_list: list, day_data: pd.DataFrame) -> pd.DataFrame:
    """
    多周期策略：
    买入条件：
    1. 日线K线收盘价在日线60均线上
    2. 日线60均线当前值 > 3天前的日线60均线值（60均线向上）
    3. 30分钟K线收盘价在30分钟60均线上
    卖出条件：
    1. 30分钟K线收盘价在30分钟60均线以下，且连续两根K线都满足此条件
    2. 当前价格距离最后-1（底分型）所在距离 >= 最后-1到它前一个1（顶分型）之间的距离，且
       当前价格到最后-1之间k线最高价格没有高于最后-1前1的最高价
    3. 当前价格最低点跌破-1（底分型）代表的价格最低价
    （注：止损卖出逻辑在run_backtest中独立实现，优先级高于策略卖出）
    :param min30_data: 30分钟K线数据
    :param min30_high_list: 30分钟最高价列表
    :param min30_low_list: 30分钟最低价列表
    :param day_data: 日线数据
    :return: 带买卖信号的30分钟数据DataFrame
    """
    # 复制数据避免修改原数据
    min30_df = min30_data.copy()
    day_df = day_data.copy()
    
    # ====================== 打印原始数据基本信息 ======================
    print("="*80)
    print("【原始数据初始化】")
    print(f"日线数据(day_df)形状: {day_df.shape} | 列名: {day_df.columns.tolist()}")
    print(f"30分钟数据(min30_df)形状: {min30_df.shape} | 列名: {min30_df.columns.tolist()}")
    print("-"*80)
    
    # 1. 计算日线60均线及条件
    day_df['ma60'] = day_df['收盘价'].rolling(window=60).mean().bfill()  # 60日均线（向后填充空值）
    day_df['day_cond1'] = (day_df['收盘价'] > day_df['ma60']).fillna(False)  # 收盘价在60均线上
    day_df['ma60_shift3'] = day_df['ma60'].shift(3).bfill()  # 3天前的60均线
    day_df['day_cond2'] = (day_df['ma60'] > day_df['ma60_shift3']).fillna(False)  # 60均线向上
    day_df['day_cond'] = day_df['day_cond1'] & day_df['day_cond2']  # 日线总条件
    
    # ====================== 打印日线计算后的数据 ======================
    print("【日线数据计算后(day_df)】")
    print(f"day_df 最终形状: {day_df.shape}")
    print("day_df 关键列前5行：")
    print(day_df[['收盘价', 'ma60', 'ma60_shift3', 'day_cond1', 'day_cond2', 'day_cond']].head())
    print("\nday_df 关键列统计（True的数量/非空数量）：")
    print(f"- 日线60均线(ma60)非空数: {day_df['ma60'].notna().sum()}")
    print(f"- 收盘价在60均线上(day_cond1)为True的数量: {day_df['day_cond1'].sum()}")
    print(f"- 60均线向上(day_cond2)为True的数量: {day_df['day_cond2'].sum()}")
    print(f"- 日线总条件(day_cond)为True的数量: {day_df['day_cond'].sum()}")
    print("-"*80)
    
    # 2. 计算30分钟60均线及基础条件
    min30_df['ma60'] = min30_df['收盘价'].rolling(window=60).mean().bfill()  # 30分钟60均线
    min30_df['min30_buy_cond'] = (min30_df['收盘价'] > min30_df['ma60']).fillna(False)  # 30分钟买入基础条件
    min30_df['min30_sell_cond_base'] = (min30_df['收盘价'] < min30_df['ma60']).fillna(False)  # 30分钟卖出基础条件
    min30_df['min30_sell_cond_original'] = (min30_df['min30_sell_cond_base'] & min30_df['min30_sell_cond_base'].shift(1)).fillna(False)  # 原卖出条件
    
    # 3. 获取顶底分型数据
    data_len = len(min30_high_list)
    stock_data_frac = gupiaojichu.identify_turns(data_len, min30_high_list, min30_low_list)
    
    # 提取顶分型(1)和底分型(-1)的索引
    top_indices = [i for i, val in enumerate(stock_data_frac) if val == 1]  # 顶分型索引
    bottom_indices = [i for i, val in enumerate(stock_data_frac) if val == -1]  # 底分型索引
    
    print(f"\n【顶底分型数据】")
    print(f"顶分型(1)数量: {len(top_indices)}，索引示例: {top_indices[:5]}")
    print(f"底分型(-1)数量: {len(bottom_indices)}，索引示例: {bottom_indices[:5]}")
    
    # 4. 初始化新的卖出条件列
    min30_df['sell_cond_pattern1'] = False  # 条件一：距离+最高价限制
    min30_df['sell_cond_pattern2'] = False  # 条件二：跌破底分型最低价
    min30_df['new_sell_cond'] = False       # 新卖出条件（任一满足）
    
    # 5. 遍历每根K线，判断新的卖出条件
    for idx in range(len(min30_df)):
        # 跳过前60根K线（均线未形成）
        if idx < 60:
            continue
        
        # 确保分型列表有数据
        if not bottom_indices:
            continue
        
        # 找到当前位置之前的最后一个底分型
        last_bottom_idx = [i for i in bottom_indices if i <= idx][-1] if any(i <= idx for i in bottom_indices) else None
        if last_bottom_idx is None:
            continue
        
        # ===== 卖出条件二：当前K线最低价跌破最后底分型的最低价 =====
        current_low = min30_df.iloc[idx]['最低价']
        bottom_low = min30_low_list[last_bottom_idx]
        if current_low < bottom_low:
            min30_df.iloc[idx, min30_df.columns.get_loc('sell_cond_pattern2')] = True
        
        # ===== 卖出条件一：距离判断 + 最高价限制 =====
        # 找到最后底分型之前的最后一个顶分型
        prev_top_idx = [i for i in top_indices if i < last_bottom_idx][-1] if any(i < last_bottom_idx for i in top_indices) else None
        if prev_top_idx is None:
            continue
        
        # 计算距离：底分型到前顶分型的距离
        distance_btw = last_bottom_idx - prev_top_idx
        # 当前位置到底分型的距离
        current_distance = idx - last_bottom_idx
        
        # 检查距离条件
        if current_distance >= distance_btw:
            # 检查当前到最后底分型之间的最高价是否不超过前顶分型的最高价
            high_range = min30_high_list[last_bottom_idx:idx+1]
            prev_top_high = min30_high_list[prev_top_idx]
            if max(high_range) <= prev_top_high:
                min30_df.iloc[idx, min30_df.columns.get_loc('sell_cond_pattern1')] = True
    
    # ===================== 1. 定义四个独立开关（True=开启，False=关闭） =====================
    switch_sell_original = True   # 原卖出条件开关//    连续两根K线收盘价在60均线以下
    switch_sell_pattern1 = False   # 卖出模式1（sell_cond_pattern1）开关//距离+最高价限制
    switch_sell_pattern2 = False   # 卖出模式2（sell_cond_pattern2）开关//跌破底分型最低价

    # 合并所有卖出条件（原条件 + 两个新条件）
    min30_df['new_sell_cond'] = (
            # 原卖出条件：仅开关开启时生效
            (min30_df['min30_sell_cond_original'] & switch_sell_original) | 
            # 卖出模式1：仅开关开启时生效
            (min30_df['sell_cond_pattern1'] & switch_sell_pattern1) | 
            # 卖出模式2：仅开关开启时生效
            (min30_df['sell_cond_pattern2'] & switch_sell_pattern2) 
    )
    
    # 6. 将日线条件映射到30分钟数据
    day_df['date'] = day_df.index.date  # 日线提取日期（无时间）
    min30_df['date'] = min30_df.index.date  # 30分钟提取日期（无时间）
    day_cond_map = day_df.set_index('date')['day_cond'].to_dict()  # 日期->日线条件的映射
    min30_df['day_cond'] = min30_df['date'].map(day_cond_map).fillna(False)  # 映射到30分钟数据
    
    # 7. 合并买入条件
    min30_df['buy_cond'] = (min30_df['day_cond'] & min30_df['min30_buy_cond']).fillna(False)
    
    # 8. 生成买卖信号
    min30_df['signal'] = 0
    min30_df.loc[min30_df['buy_cond'], 'signal'] = 1  # 买入信号
    min30_df.loc[min30_df['new_sell_cond'], 'signal'] = -1  # 卖出信号（合并所有条件）
    
    # ====================== 打印30分钟计算后的数据 ======================
    print("【30分钟数据计算后(min30_df)】")
    print(f"min30_df 最终形状: {min30_df.shape}")
    print("min30_df 关键列前10行：")
    print(min30_df[['收盘价', '最低价', 'ma60', 'min30_buy_cond', 'day_cond', 'buy_cond', 
                    'sell_cond_pattern1', 'sell_cond_pattern2', 'new_sell_cond', 'signal']].head(10))
    print("\nmin30_df 关键列统计（True的数量/非空数量）：")
    print(f"- 30分钟60均线(ma60)非空数: {min30_df['ma60'].notna().sum()}")
    print(f"- 30分钟买入基础条件(min30_buy_cond)为True的数量: {min30_df['min30_buy_cond'].sum()}")
    print(f"- 映射后的日线条件(day_cond)为True的数量: {min30_df['day_cond'].sum()}")
    print(f"- 最终买入条件(buy_cond)为True的数量: {min30_df['buy_cond'].sum()}")
    print(f"- 原卖出条件(min30_sell_cond_original)为True的数量: {min30_df['min30_sell_cond_original'].sum()}")
    print(f"- 新卖出条件一(sell_cond_pattern1)为True的数量: {min30_df['sell_cond_pattern1'].sum()}")
    print(f"- 新卖出条件二(sell_cond_pattern2)为True的数量: {min30_df['sell_cond_pattern2'].sum()}")
    print(f"- 合并后卖出条件(new_sell_cond)为True的数量: {min30_df['new_sell_cond'].sum()}")
    print(f"- 买入信号(signal=1)数量: {min30_df[min30_df['signal']==1].shape[0]}")
    print(f"- 卖出信号(signal=-1)数量: {min30_df[min30_df['signal']==-1].shape[0]}")
    print("="*80)
    
    # 过滤掉均线未形成的初期数据（前60根K线）
    min30_df = min30_df.iloc[60:]
    
    # 安全删除临时列（只删存在的列）
    cols_to_drop = ['date', 'ma60', 'min30_buy_cond', 'min30_sell_cond_base', 
                    'min30_sell_cond_original', 'day_cond', 'buy_cond',
                    'sell_cond_pattern1', 'sell_cond_pattern2', 'new_sell_cond']
    existing_cols = [col for col in cols_to_drop if col in min30_df.columns]
    min30_df = min30_df.drop(existing_cols, axis=1)
    
    return min30_df


# ====================== 使用示例（新增以损定量配置） ======================
if __name__ == "__main__":
    # 1. 初始化回测框架
    backtest = TdxStockBacktest()
    
    # 2. 连接通达信服务器
    if backtest.connect_tdx():
        # 3. 一键获取日线+30分钟线数据（以振江股份 603536 为例）
        code = "600362"
        multi_data = backtest.get_multi_period_data(code=code, count=800)  # 增加数据量保证60均线有
        
        # 4. 运行30分钟周期的多策略回测（含以损定量+止损）
        if not multi_data['30min'].empty and not multi_data['day'].empty:
            print("\n===== 运行30分钟线多周期策略回测（含以损定量+止损+顶底分型卖出） =====")
            # 先通过策略函数处理数据
            min30_with_signal = multi_period_strategy(
                multi_data['30min'],
                multi_data['30min_high_list'],
                multi_data['30min_low_list'],
                multi_data['day']
            )
            # 将带信号的30分钟数据更新到stock_data中
            backtest.stock_data['30min'] = min30_with_signal
            
            # dummy策略兼容回测框架
            def dummy_strategy(data, day_data=None):
                return data
            
            # 执行回测（新增stop_loss_ratio参数，设置总资金止损百分比）
            min30_result = backtest.run_backtest(
                strategy_func=dummy_strategy, 
                period='30min', 
                init_cash=100000,
                commission=0.0003,
                stop_loss_ratio=0.02  # 每次止损总资金的2%，可自行调整（如0.01=1%，0.03=3%）
            )
            
            # 打印交易记录和可视化
            print("\n===== 30分钟线交易记录（含以损定量+止损+顶底分型卖出） =====")
            print(backtest.trade_records)
            backtest.plot_result(period='30min')
        
        # 5. 断开连接
        backtest.close()