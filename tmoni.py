import pandas as pd
import numpy as np
import random
from datetime import datetime

# 1. 加载数据
file_path = "板块回测汇总结果_含总笔数2026-04-08-11-44-57_涨幅0.5-3%_20251218_to_20260307_特定时段.xlsx"
df = pd.read_excel(file_path, sheet_name='所有交易明细')

# 转换时间格式
df['交易时间'] = pd.to_datetime(df['交易时间'])

# 2. 预处理：将买入和卖出配对
trades = []
for code, group in df.groupby('股票代码'):
    group = group.sort_values('交易时间')
    temp_buy = None
    for _, row in group.iterrows():
        if row['交易类型'] == '买入':
            temp_buy = row
        elif row['交易类型'] == '策略卖出' and temp_buy is not None:
            trades.append({
                '股票代码': code,
                '买入时间': temp_buy['交易时间'],
                '卖出时间': row['交易时间'],
                '单笔盈亏': row['单笔盈亏'],
                '实际盈亏比例': row['实际盈亏比例']
            })
            temp_buy = None

trades_df = pd.DataFrame(trades).sort_values('买入时间')

def run_single_simulation(all_trades, initial_capital=100000):
    """
    执行单次模拟：同一时间只持有一只票
    """
    current_time = datetime(2000, 1, 1)
    total_profit = 0
    executed_trades = []
    
    time_groups = all_trades.groupby('买入时间')
    unique_buy_times = sorted(all_trades['买入时间'].unique())
    
    for buy_time in unique_buy_times:
        if pd.to_datetime(buy_time) >= current_time:
            available_signals = time_groups.get_group(buy_time)
            if len(available_signals) == 0:
                continue
            selected_trade = available_signals.sample(n=1).iloc[0]
            total_profit += selected_trade['单笔盈亏']
            current_time = selected_trade['卖出时间']
            executed_trades.append(selected_trade)
            
    return total_profit, len(executed_trades)

# 3. 开始随机模拟
num_simulations = 500
results = []   # 每个元素为 (总盈亏, 交易次数)

print(f"正在进行 {num_simulations} 次随机模拟...")
for i in range(num_simulations):
    profit, count = run_single_simulation(trades_df)
    results.append((profit, count))

# 4. 提取盈亏和交易次数数组
profits = np.array([r[0] for r in results])
counts = np.array([r[1] for r in results])

print("\n" + "="*40)
print(f"统计指标 (基于 10w 初始本金，单仓位随机模拟)")
print("="*40)

print("\n【总盈亏 (元)】")
print(f"最高收益: {profits.max():,.2f} 元")
print(f"最低收益: {profits.min():,.2f} 元")
print(f"平均收益: {profits.mean():,.2f} 元")
print(f"收益中位数: {np.median(profits):,.2f} 元")
print(f"收益标准差: {profits.std():,.2f} 元")
print(f"正收益概率: {(profits > 0).sum() / num_simulations * 100:.2f}%")

print("\n【交易次数】")
print(f"最多交易次数: {counts.max()} 次")
print(f"最少交易次数: {counts.min()} 次")
print(f"平均交易次数: {counts.mean():.2f} 次")
print(f"交易次数中位数: {np.median(counts):.0f} 次")
print(f"交易次数标准差: {counts.std():.2f} 次")

print("="*40)