import time
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pytdx.hq import TdxHq_API
from typing import Callable, Dict, List

# 忽略无关警告
warnings.filterwarnings('ignore')

class TdxStockBacktest:
    """基于pytdx的股票回测框架（支持多周期K线）"""
    
    def __init__(self):
        self.api = TdxHq_API()
        self.stock_data = {}  # 改为字典存储多周期数据: {'day': 日线数据, '30min': 30分钟数据}
        self.backtest_result = None  # 存储回测结果
        self.trade_records = None  # 交易记录
    
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
            return result_df
        except Exception as e:
            print(f"获取{self._ktype2name(ktype)}数据失败: {e}")
            return pd.DataFrame()
    
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
        day_data = self.get_stock_k_data(code, count=count, ktype=9)
        # 获取30分钟线数据
        min30_data = self.get_stock_k_data(code, count=count, ktype=2)
        
        return {
            'day': day_data,
            '30min': min30_data
        }
    
    def run_backtest(self, strategy_func: Callable, period: str = 'day', init_cash: float = 100000.0, commission: float = 0.0003) -> pd.DataFrame:
        """
        执行指定周期的策略回测
        :param strategy_func: 策略函数（输入data，输出买卖信号）
        :param period: 回测周期 ('day'=日线, '30min'=30分钟)
        :param init_cash: 初始资金
        :param commission: 交易佣金（默认0.03%）
        :return: 回测结果DataFrame
        """
        # 检查指定周期数据是否存在
        if period not in self.stock_data or self.stock_data[period].empty:
            print(f"请先获取{self._ktype2name({'day':9, '30min':2}[period])}数据")
            return pd.DataFrame()
        
        # 复制数据避免修改原数据
        data = self.stock_data[period].copy()
        
        # 初始化回测参数
        cash = init_cash  # 可用资金
        position = 0  # 持仓数量
        total_asset = init_cash  # 总资产（现金+持仓市值）
        trade_records = []  # 交易记录
        daily_results = []  # 每日/每30分钟结果
        # 新增：记录每笔交易的盈亏（用于计算盈亏比）
        trade_pnl = []
        
        # 1. 运行策略函数，获取买卖信号（多周期策略需要传入日线数据）
        if period == '30min' and 'day' in self.stock_data:
            data = strategy_func(data, self.stock_data['day'])
        else:
            data = strategy_func(data)
        
        # 检查是否有信号列
        if 'signal' not in data.columns:
            print("策略函数必须返回包含'signal'列的DataFrame（signal: 1=买入, -1=卖出, 0=持有）")
            return pd.DataFrame()
        
        # 2. 逐行执行回测
        buy_price = 0  # 记录买入价格
        buy_fee = 0    # 记录买入手续费
        for datetime, row in data.iterrows():
            close_price = row['收盘价']
            
            # 执行买入信号
            if row['signal'] == 1 and cash > close_price:
                # 计算可买数量（整手，1手=100股）
                buy_num = (cash // (close_price * 100)) * 100
                if buy_num > 0:
                    # 计算交易成本
                    cost = buy_num * close_price * (1 + commission)
                    fee = buy_num * close_price * commission
                    if cash >= cost:
                        position += buy_num
                        cash -= cost
                        buy_price = close_price  # 记录买入价
                        buy_fee = fee            # 记录买入手续费
                        trade_records.append({
                            '时间': datetime,
                            '操作': '买入',
                            '价格': close_price,
                            '数量': buy_num,
                            '费用': fee
                        })
            
            # 执行卖出信号
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
                    '操作': '卖出',
                    '价格': close_price,
                    '数量': sell_num,
                    '费用': fee,
                    '单笔盈亏': pnl  # 新增：记录单笔盈亏
                })
                # 新增：存储单笔盈亏用于计算盈亏比
                trade_pnl.append(pnl)
                position = 0
                buy_price = 0
                buy_fee = 0
            
            # 计算当前总资产
            total_asset = cash + position * close_price
            daily_results.append({
                '时间': datetime,
                '收盘价': close_price,
                '持仓数量': position,
                '可用现金': cash,
                '总资产': total_asset,
                '累计收益': total_asset - init_cash,
                '累计收益率': (total_asset - init_cash) / init_cash * 100
            })
        
        # 整理回测结果
        self.backtest_result = pd.DataFrame(daily_results)
        self.backtest_result = self.backtest_result.set_index('时间')
        
        # 添加交易记录
        self.trade_records = pd.DataFrame(trade_records)
        # 新增：存储单笔盈亏列表
        self.trade_pnl = trade_pnl
        
        # 计算核心指标（包含盈亏比）
        self.calc_backtest_metrics(init_cash)
        
        print(f"{self._ktype2name({'day':9, '30min':2}[period])}回测完成！")
        return self.backtest_result
    
    def calc_backtest_metrics(self, init_cash: float) -> Dict:
        """计算回测核心指标（新增盈亏比计算）"""
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
            total_trades = len(self.trade_records) // 2  # 每2笔（买+卖）算一次完整交易
            for idx, trade in self.trade_records.iterrows():
                if trade['操作'] == '买入':
                    buy_price = trade['价格']
                elif trade['操作'] == '卖出' and buy_price > 0:
                    if trade['价格'] > buy_price:
                        win_trades += 1
                    buy_price = 0
            win_rate = win_trades / total_trades * 100 if total_trades >=1 else 0
        
        # ========== 新增：盈亏比计算 ==========
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
            # 只有亏损交易时，盈亏比为0
        
        # 存储指标（新增盈亏比）
        self.metrics = {
            '初始资金': init_cash,
            '最终总资产': self.backtest_result['总资产'].iloc[-1],
            '累计收益': total_profit,
            '累计收益率(%)': round(total_return, 2),
            '年化收益率(%)': round(annual_return, 2),
            '最大回撤(%)': round(max_drawdown, 2),
            '交易次数': len(self.trade_records),
            '完整交易次数': total_trades,
            '胜率(%)': round(win_rate, 2),
            '盈亏比': profit_loss_ratio  # 新增盈亏比指标
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
        """可视化指定周期的回测结果"""
        if self.backtest_result is None:
            print("无回测结果可展示")
            return
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        
        # 子图1：价格和总资产
        ax1.plot(self.backtest_result.index, self.backtest_result['收盘价'], label=f'{period}价格', color='blue', alpha=0.7)
        ax1_twin = ax1.twinx()
        ax1_twin.plot(self.backtest_result.index, self.backtest_result['总资产'], label='总资产', color='red')
        ax1.set_ylabel('价格 (元)')
        ax1_twin.set_ylabel('总资产 (元)')
        ax1.set_title(f'{self._ktype2name({"day":9, "30min":2}[period])}价格与总资产走势')
        ax1.legend(loc='upper left')
        ax1_twin.legend(loc='upper right')
        ax1.grid(True, alpha=0.3)
        
        # 子图2：累计收益率
        ax2.bar(self.backtest_result.index, self.backtest_result['累计收益率'], color='green', alpha=0.6)
        ax2.set_ylabel('累计收益率 (%)')
        ax2.set_xlabel('时间')
        ax2.set_title('累计收益率走势')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    def close(self):
        """断开通达信连接"""
        self.api.disconnect()
        print("已断开通达信服务器连接")


def multi_period_strategy(min30_data: pd.DataFrame, day_data: pd.DataFrame) -> pd.DataFrame:
    """
    多周期策略：
    买入条件：
    1. 日线K线收盘价在日线60均线上
    2. 日线60均线当前值 > 3天前的日线60均线值（60均线向上）
    3. 30分钟K线收盘价在30分钟60均线上
    卖出条件：
    1. 30分钟K线收盘价在30分钟60均线以下，且连续两根K线都满足此条件
    :param min30_data: 30分钟K线数据
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
    min30_df['min30_sell_cond'] = (min30_df['min30_sell_cond_base'] & min30_df['min30_sell_cond_base'].shift(1)).fillna(False)  # 连续2根线下
    
    # 3. 将日线条件映射到30分钟数据
    day_df['date'] = day_df.index.date  # 日线提取日期（无时间）
    min30_df['date'] = min30_df.index.date  # 30分钟提取日期（无时间）
    day_cond_map = day_df.set_index('date')['day_cond'].to_dict()  # 日期->日线条件的映射
    min30_df['day_cond'] = min30_df['date'].map(day_cond_map).fillna(False)  # 映射到30分钟数据
    
    # 4. 合并买入条件
    min30_df['buy_cond'] = (min30_df['day_cond'] & min30_df['min30_buy_cond']).fillna(False)
    
    # 5. 生成买卖信号
    min30_df['signal'] = 0
    min30_df.loc[min30_df['buy_cond'], 'signal'] = 1  # 买入信号
    min30_df.loc[min30_df['min30_sell_cond'], 'signal'] = -1  # 卖出信号
    
    # ====================== 打印30分钟计算后的数据 ======================
    print("【30分钟数据计算后(min30_df)】")
    print(f"min30_df 最终形状: {min30_df.shape}")
    print("min30_df 关键列前5行：")
    print(min30_df[['收盘价', 'ma60', 'min30_buy_cond', 'day_cond', 'buy_cond', 'signal']].head(10))
    print("\nmin30_df 关键列统计（True的数量/非空数量）：")
    print(f"- 30分钟60均线(ma60)非空数: {min30_df['ma60'].notna().sum()}")
    print(f"- 30分钟买入基础条件(min30_buy_cond)为True的数量: {min30_df['min30_buy_cond'].sum()}")
    print(f"- 映射后的日线条件(day_cond)为True的数量: {min30_df['day_cond'].sum()}")
    print(f"- 最终买入条件(buy_cond)为True的数量: {min30_df['buy_cond'].sum()}")
    print(f"- 30分钟卖出条件(min30_sell_cond)为True的数量: {min30_df['min30_sell_cond'].sum()}")
    print(f"- 买入信号(signal=1)数量: {min30_df[min30_df['signal']==1].shape[0]}")
    print(f"- 卖出信号(signal=-1)数量: {min30_df[min30_df['signal']==-1].shape[0]}")
    print("="*80)
    
    # 过滤掉均线未形成的初期数据（前60根K线）
    min30_df = min30_df.iloc[60:]
    
    # 安全删除临时列（只删存在的列）
    cols_to_drop = ['date', 'ma60', 'min30_buy_cond', 'min30_sell_cond_base', 
                    'min30_sell_cond', 'day_cond', 'buy_cond']
    existing_cols = [col for col in cols_to_drop if col in min30_df.columns]
    min30_df = min30_df.drop(existing_cols, axis=1)
    
    return min30_df


# ====================== 使用示例 ======================
if __name__ == "__main__":
    # 1. 初始化回测框架
    backtest = TdxStockBacktest()
    
    # 2. 连接通达信服务器
    if backtest.connect_tdx():
        # 3. 一键获取日线+30分钟线数据（以贵州茅台 600519 为例）
        code = "000042"
        multi_data = backtest.get_multi_period_data(code=code, count=800)  # 增加数据量保证60均线有效
        
        # 4. 运行30分钟周期的多策略回测
        if not multi_data['30min'].empty and not multi_data['day'].empty:
            print("\n===== 运行30分钟线多周期策略回测 =====")
            # 先通过策略函数处理数据
            min30_with_signal = multi_period_strategy(multi_data['30min'], multi_data['day'])
            # 将带信号的30分钟数据更新到stock_data中
            backtest.stock_data['30min'] = min30_with_signal
            
            # 修复：dummy_strategy支持2个参数（兼容run_backtest的传参逻辑）
            def dummy_strategy(data, day_data=None):
                return data
            
            # 执行回测
            min30_result = backtest.run_backtest(
                strategy_func=dummy_strategy, 
                period='30min', 
                init_cash=100000,
                commission=0.0003
            )
            
            # 打印交易记录和可视化
            print("\n===== 30分钟线交易记录 =====")
            print(backtest.trade_records)
            backtest.plot_result(period='30min')
        
        # 5. 断开连接
        backtest.close()