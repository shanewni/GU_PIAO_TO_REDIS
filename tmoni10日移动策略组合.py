import pandas as pd
import numpy as np
import random
from datetime import datetime

# 1. 加载数据
file_path = "板块回测汇总结果_含总笔数2026-04-27-17-00-38_正常_增加日线起爆位置_20040920_to_20241211_特定时段.xlsx"
df = pd.read_excel(file_path, sheet_name='所有交易明细')
df['交易时间'] = pd.to_datetime(df['交易时间'])
# 拼接策略组合名
df['策略组合'] = df['起爆点位置(日线)'].astype(str) + " + " + df['起爆点位置(30分)'].astype(str)

# 2. 预处理：计算每日收盘时的“准入白名单”
df_sell = df[df['交易类型'].str.contains('卖出', na=False)].copy()
df_sell = df_sell.sort_values('交易时间')
df_sell['日期'] = df_sell['交易时间'].dt.date

profit_col = '实际盈亏比例' if '实际盈亏比例' in df_sell.columns else '单笔盈亏'
df_sell['calc_profit'] = pd.to_numeric(df_sell[profit_col], errors='coerce').fillna(0)
df_sell['单笔盈亏'] = pd.to_numeric(df_sell['单笔盈亏'], errors='coerce').fillna(0)

def get_daily_white_list(df_sell):
    """
    计算每一天收盘后，哪些组合是达标的（用于指导次日及以后的买入）
    """
    unique_dates = sorted(df_sell['日期'].unique())
    daily_white_list = {}
    
    for current_date in unique_dates:
        # 回溯 10 个自然日
        start_date = current_date - pd.Timedelta(days=3)
        mask = (df_sell['日期'] > start_date) & (df_sell['日期'] <= current_date)
        window_data = df_sell[mask]
        
        if window_data.empty:
            daily_white_list[current_date] = set()
            continue
            
        stats = window_data.groupby('策略组合').agg(
            pnl=('单笔盈亏', 'sum'),
            mean_profit=('calc_profit', 'mean'),
            std_profit=('calc_profit', 'std')
        )
        
        # 计算 10 日移动夏普
        stats['sharpe'] = np.where(stats['std_profit'] > 0, stats['mean_profit'] / stats['std_profit'], 0)
        
        # 达标条件：夏普 > 0.1 且 利润 > 0
        qualified = stats[(stats['sharpe'] > 0.2) & (stats['pnl'] > 0)].index.tolist()
        daily_white_list[current_date] = set(qualified)
    return daily_white_list

white_list_db = get_daily_white_list(df_sell)

# 3. 配对交易：将买入和卖出关联
trades = []
for (code, combo), group in df.groupby(['股票代码', '策略组合']):
    group = group.sort_values('交易时间')
    temp_buy = None
    for _, row in group.iterrows():
        if row['交易类型'] == '买入':
            temp_buy = row
        elif '卖出' in row['交易类型'] and temp_buy is not None:
            trades.append({
                '策略组合': combo,
                '买入时间': temp_buy['交易时间'],
                '卖出时间': row['交易时间'],
                '单笔盈亏': row['单笔盈亏'],
                '实际盈亏比例': row['实际盈亏比例']
            })
            temp_buy = None

trades_df = pd.DataFrame(trades).sort_values('买入时间')

# 4. 执行单次模拟：基于昨日白名单
def run_single_simulation(all_trades, wl_db):
    current_finish_time = datetime(2000, 1, 1)
    executed_pnl = []
    
    time_groups = all_trades.groupby('买入时间')
    unique_buy_times = sorted(all_trades['买入时间'].unique())
    
    for buy_time in unique_buy_times:
        buy_datetime = pd.to_datetime(buy_time)
        if buy_datetime >= current_finish_time:
            # 获取“昨日”或“最近一个交易日”的白名单
            yesterday = (buy_datetime - pd.Timedelta(days=1)).date()
            available_dates = [d for d in wl_db.keys() if d <= yesterday]
            
            if not available_dates:
                continue
                
            current_white_list = wl_db[available_dates[-1]]
            
            # 过滤信号：必须在昨日达标名单中
            candidates = time_groups.get_group(buy_time)
            qualified = candidates[candidates['策略组合'].isin(current_white_list)]
            
            if qualified.empty:
                continue
            
            # 随机选一个符合准入条件的组合进行交易
            selected = qualified.sample(n=1).iloc[0]
            executed_pnl.append(selected['单笔盈亏'])
            current_finish_time = selected['卖出时间']
            
    pnl_array = np.array(executed_pnl)
    if len(pnl_array) == 0: return 0, 0, 0, 0, 0
    
    # 指标计算
    total_profit = pnl_array.sum()
    trade_count = len(pnl_array)
    win_rate = (pnl_array > 0).sum() / trade_count
    wins = pnl_array[pnl_array > 0]
    losses = pnl_array[pnl_array < 0]
    plt_ratio = (wins.mean() / abs(losses.mean())) if len(wins)>0 and len(losses)>0 else 0
    sharpe = (pnl_array.mean() / pnl_array.std() * np.sqrt(trade_count)) if len(pnl_array)>1 and pnl_array.std()!=0 else 0
    
    return total_profit, trade_count, win_rate, plt_ratio, sharpe

# 5. 执行 500 次随机模拟
num_simulations = 500
results = [run_single_simulation(trades_df, white_list_db) for _ in range(num_simulations)]
res_df = pd.DataFrame(results, columns=['profits', 'counts', 'win_rates', 'plt_ratios', 'sharpes'])

# 6. 输出结果
print("\n" + "="*40)
print(f"统计指标 (基于 10w 初始本金，昨日达标准入名单)")
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