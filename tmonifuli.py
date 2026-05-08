import pandas as pd
import numpy as np
import random
from datetime import datetime

# 1. 加载数据
file_path = "板块回测汇总结果_含总笔数2026-05-08-18-23-34_正常_止损点移到底分型起爆.xlsx"  # 替换为你的文件路径
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
                # 核心：复利模拟必须依赖比例
                '实际盈亏比例': row['实际盈亏比例'] 
            })
            temp_buy = None

trades_df = pd.DataFrame(trades).sort_values('买入时间')

def run_single_simulation_compounding(all_trades, initial_capital=100000):
    """
    执行单次模拟：复利模式（同一时间只持有一只票，全仓滚动）
    """
    current_time = datetime(2000, 1, 1)
    current_balance = initial_capital # 账户初始余额
    
    pnl_ratios = []   # 存储每笔盈亏比例
    balance_history = [initial_capital] # 存储净值曲线数据
    
    time_groups = all_trades.groupby('买入时间')
    unique_buy_times = sorted(all_trades['买入时间'].unique())
    
    for buy_time in unique_buy_times:
        if pd.to_datetime(buy_time) >= current_time:
            available_signals = time_groups.get_group(buy_time)
            if len(available_signals) == 0:
                continue
                
            selected_trade = available_signals.sample(n=1).iloc[0]
            
            # --- 复利核心逻辑 ---
            ratio = selected_trade['实际盈亏比例'] / 100.0
            
            # 当前盈亏金额 = 当前本金 * 盈亏比例
            pnl_amount = current_balance * ratio
            # 更新账户总金额 (利滚利)
            current_balance += pnl_amount
            
            # 记录数据
            pnl_ratios.append(ratio)
            balance_history.append(current_balance)
            
            # 更新当前时间为卖出时间
            current_time = selected_trade['卖出时间']
            
    # --- 计算复利模式下的各项指标 ---
    trade_count = len(pnl_ratios)
    total_profit = current_balance - initial_capital
    pnl_array = np.array(pnl_ratios)
    
    win_rate = 0
    plt_ratio = 0
    sharpe = 0
    
    if trade_count > 0:
        # 1. 胜率
        win_rate = (pnl_array > 0).sum() / trade_count
        
        # 2. 盈亏比 (基于比例的平均盈利 / 平均亏损)
        wins = pnl_array[pnl_array > 0]
        losses = pnl_array[pnl_array < 0]
        avg_win = wins.mean() if len(wins) > 0 else 0
        avg_loss = abs(losses.mean()) if len(losses) > 0 else 0
        plt_ratio = avg_win / avg_loss if avg_loss != 0 else 0
        
        # 3. 夏普比率 (针对净值曲线的稳定性评估)
        # 使用对数收益率计算更准确，这里采用单笔比例简化计算
        if len(pnl_array) > 1 and pnl_array.std() != 0:
            sharpe = (pnl_array.mean() / pnl_array.std()) * np.sqrt(trade_count)

    return total_profit, trade_count, win_rate, plt_ratio, sharpe, current_balance

# 3. 开始随机模拟
num_simulations = 500
results = []

print(f"正在进行 {num_simulations} 次随机模拟（复利模式）...")
for i in range(num_simulations):
    profit, count, win_rate, plt_ratio, sharpe, final_balance = run_single_simulation_compounding(trades_df)
    results.append({
        'profits': profit,
        'counts': count,
        'win_rates': win_rate,
        'plt_ratios': plt_ratio,
        'sharpes': sharpe,
        'final_balance': final_balance
    })

res_df = pd.DataFrame(results)

# 4. 打印结果
print("\n" + "="*40)
print(f"统计指标 (初始本金 10w，单仓位全仓复利模拟)")
print("="*40)

def print_metric(label, series, is_percent=False):
    fmt = ".2%" if is_percent else ",.2f"
    print(f"\n【{label}】")
    print(f"最高: {series.max():{fmt}}")
    print(f"最低: {series.min():{fmt}}")
    print(f"平均: {series.mean():{fmt}}")
    print(f"中位数: {series.median():{fmt}}")
    print(f"标准差: {series.std():.2f}")

print_metric("最终账户余额 (元)", res_df['final_balance'])
print_metric("净利润 (元)", res_df['profits'])
print(f"正收益概率: {(res_df['profits'] > 0).sum() / num_simulations * 100:.2f}%")

print_metric("交易次数", res_df['counts'])
print_metric("胜率", res_df['win_rates'], is_percent=True)
print_metric("盈亏比", res_df['plt_ratios'])
print_metric("夏普比率", res_df['sharpes'])

print("\n" + "="*40)