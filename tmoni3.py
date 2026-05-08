import pandas as pd
import numpy as np
import random
from datetime import datetime

# ==================== 1. 加载与预处理 ====================
file_path = "板块回测汇总结果_含总笔数2026-05-08-18-23-34_正常_止损点移到底分型起爆.xlsx"
df = pd.read_excel(file_path, sheet_name='所有交易明细')
df['交易时间'] = pd.to_datetime(df['交易时间'])

# 配对买入和策略卖出
trades = []
for code, group in df.groupby('股票代码'):
    group = group.sort_values('交易时间')
    temp_buy = None
    for _, row in group.iterrows():
        if row['交易类型'] == '买入':
            temp_buy = row
        elif row['交易类型'] == '策略卖出' and temp_buy is not None:
            # 计算买入成本：价格*数量 + 费用
            buy_cost = temp_buy['价格'] * temp_buy['数量'] + temp_buy['费用']
            trades.append({
                '股票代码': code,
                '买入时间': temp_buy['交易时间'],
                '卖出时间': row['交易时间'],
                '单笔盈亏': row['单笔盈亏'],          # 已扣除双向费用后的净盈亏
                '实际盈亏比例': row['实际盈亏比例'],
                '买入成本': buy_cost                  # 本次开仓占用资金
            })
            temp_buy = None

trades_df = pd.DataFrame(trades).sort_values('买入时间')


# ==================== 2. 多仓位模拟函数 ====================
def run_multi_position_simulation(all_trades, initial_capital=100000):
    """
    事件驱动模拟：同一时间可持有多个仓位，只要现金足够就买入
    同时间买入信号随机排序，卖出优先处理
    """
    cash = initial_capital
    holdings = {}          # 用 trade_id 记录持仓：{id: (买入成本, 单笔盈亏)}
    executed_pnl = []      # 记录每笔实际盈亏

    # 生成交易 ID 并构造事件
    events = []
    for idx, trade in all_trades.iterrows():
        trade_id = idx
        # 买入事件
        events.append({
            'time': trade['买入时间'],
            'type': 'buy',
            'trade_id': trade_id,
            'cost': trade['买入成本'],
            'pnl': trade['单笔盈亏'],
            'random_key': random.random()   # 同时间买入的随机顺序
        })
        # 卖出事件
        events.append({
            'time': trade['卖出时间'],
            'type': 'sell',
            'trade_id': trade_id,
            'random_key': 0.0               # 卖出排序优先（0 < 任何正随机数）
        })

    # 排序规则：先按时间，再按 random_key（卖出固定0，买入随机>0）
    events = sorted(events, key=lambda x: (x['time'], x['random_key']))

    # 遍历事件
    for ev in events:
        if ev['type'] == 'sell':
            # 只有实际持仓的才卖出
            if ev['trade_id'] in holdings:
                cost, pnl = holdings.pop(ev['trade_id'])
                cash += cost + pnl           # 回收成本+盈亏
                executed_pnl.append(pnl)

        elif ev['type'] == 'buy':
            # 检查现金是否足够
            if cash >= ev['cost']:
                cash -= ev['cost']
                holdings[ev['trade_id']] = (ev['cost'], ev['pnl'])
            # 如果现金不足，该信号直接放弃（不买入）

    # 模拟结束，如果有未平仓持仓按原始成本回收（不计算盈亏）——通常策略有卖出配对，不会有残留
    for tid, (cost, _) in holdings.items():
        cash += cost

    # 计算指标
    total_profit = cash - initial_capital
    pnl_array = np.array(executed_pnl)
    trade_count = len(pnl_array)

    win_rate = 0.0
    plt_ratio = 0.0
    sharpe = 0.0

    if trade_count > 0:
        win_rate = (pnl_array > 0).sum() / trade_count

        wins = pnl_array[pnl_array > 0]
        losses = pnl_array[pnl_array < 0]
        avg_win = wins.mean() if len(wins) > 0 else 0
        avg_loss = abs(losses.mean()) if len(losses) > 0 else 0
        plt_ratio = avg_win / avg_loss if avg_loss != 0 else 0

        if trade_count > 1 and pnl_array.std() != 0:
            sharpe = (pnl_array.mean() / pnl_array.std()) * np.sqrt(trade_count)

    return total_profit, trade_count, win_rate, plt_ratio, sharpe


# ==================== 3. 随机模拟 ====================
num_simulations = 500
results = []

print(f"正在进行 {num_simulations} 次随机模拟（多仓位模式）...")
for i in range(num_simulations):
    profit, count, win_rate, plt_ratio, sharpe = run_multi_position_simulation(trades_df)
    results.append({
        'profits': profit,
        'counts': count,
        'win_rates': win_rate,
        'plt_ratios': plt_ratio,
        'sharpes': sharpe
    })

res_df = pd.DataFrame(results)


# ==================== 4. 输出统计 ====================
print("\n" + "="*40)
print("统计指标 (初始资金 10万，允许同时持有多仓)")
print("="*40)

def print_metric(label, series, is_percent=False):
    fmt = ".2%" if is_percent else ",.2f"
    print(f"\n【{label}】")
    print(f"最高: {series.max():{fmt}}")
    print(f"最低: {series.min():{fmt}}")
    print(f"平均: {series.mean():{fmt}}")
    print(f"中位数: {series.median():{fmt}}")
    print(f"标准差: {series.std():.2f}")

print_metric("总盈亏 (元)", res_df['profits'])
print(f"正收益概率: {(res_df['profits'] > 0).sum() / num_simulations * 100:.2f}%")

print_metric("交易次数", res_df['counts'])
print_metric("胜率", res_df['win_rates'], is_percent=True)
print_metric("盈亏比", res_df['plt_ratios'])
print_metric("夏普比率", res_df['sharpes'])

print("\n" + "="*40)