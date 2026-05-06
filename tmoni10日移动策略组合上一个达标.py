import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def run_optimized_simulation():
    file_path = "板块回测汇总结果_含总笔数2026-05-04-18-21-42_港股.xlsx"
    df = pd.read_excel(file_path, sheet_name='所有交易明细')
    df['交易时间'] = pd.to_datetime(df['交易时间'])
    df['策略组合'] = df['起爆点位置(日线)'].astype(str) + " + " + df['起爆点位置(30分)'].astype(str)
    
    # 1. 预处理：计算每个时刻各组合的 10 日滚动状态 (基于平仓记录)
    df_sell = df[df['交易类型'].str.contains('卖出', na=False)].copy()
    df_sell = df_sell.sort_values('交易时间')
    
    profit_col = '实际盈亏比例' if '实际盈亏比例' in df_sell.columns else '单笔盈亏'
    df_sell['calc_profit'] = pd.to_numeric(df_sell[profit_col], errors='coerce').fillna(0)
    
    def get_rolling_status(group):
        temp = group.set_index('交易时间').sort_index()
        r_mean = temp['calc_profit'].rolling(window='10D').mean()
        r_std = temp['calc_profit'].rolling(window='10D').std()
        r_pnl = temp['单笔盈亏'].rolling(window='10D').sum()
        sharpe = np.where((r_std > 0) & (~np.isnan(r_std)), r_mean / r_std, 0)
        return pd.DataFrame({
            '策略组合': group.name,
            'rolling_sharpe': sharpe,
            'rolling_pnl': r_pnl.values
        }, index=temp.index)

    # 建立一个随时间变化的“准入数据库”
    rolling_db = df_sell.groupby('策略组合', group_keys=False).apply(get_rolling_status).sort_index()

    # 2. 配对交易：买入与卖出对应
    trades = []
    for (code, combo), group in df.groupby(['股票代码', '策略组合']):
        group = group.sort_values('交易时间')
        temp_buy = None
        for _, row in group.iterrows():
            if row['交易类型'] == '买入':
                temp_buy = row
            elif '卖出' in row['交易类型'] and temp_buy is not None:
                trades.append({
                    '股票代码': code,
                    '策略组合': combo,
                    '买入时间': temp_buy['交易时间'],
                    '卖出时间': row['交易时间'],
                    '单笔盈亏': row['单笔盈亏'],
                    '实际盈亏比例': row['实际盈亏比例']
                })
                temp_buy = None
    trades_df = pd.DataFrame(trades).sort_values('买入时间')

    # 3. 核心：带准入条件的模拟函数
    def run_single_simulation(all_trades, r_db, num_sims=500):
        current_time = datetime(2000, 1, 1)
        executed_trades_pnl = []
        
        # 将买入信号按时间分组
        time_groups = all_trades.groupby('买入时间')
        unique_buy_times = sorted(all_trades['买入时间'].unique())

        for buy_time in unique_buy_times:
            if pd.to_datetime(buy_time) >= current_time:
                # 获取该时间点所有候选信号
                candidates = time_groups.get_group(buy_time)
                
                # --- 准入过滤逻辑 ---
                qualified_candidates = []
                for _, trade in candidates.iterrows():
                    combo = trade['策略组合']
                    # 查找该组合在买入时间之前的最新滚动状态
                    # 必须使用买入前已平仓的数据计算出的结果
                    past_status = r_db[r_db['策略组合'] == combo].loc[:buy_time]
                    
                    if not past_status.empty:
                        latest = past_status.iloc[-1]
                        # 核心门槛：10日夏普 > 0.1 且 10日净利润 > 0
                        if latest['rolling_sharpe'] > 0.2 and latest['rolling_pnl'] > 0:
                            qualified_candidates.append(trade)
                
                if not qualified_candidates:
                    continue
                
                # 从达标的组合中随机选一个
                selected_trade = random.choice(qualified_candidates)
                executed_trades_pnl.append(selected_trade['单笔盈亏'])
                current_time = selected_trade['卖出时间']

        # 计算指标
        pnl_array = np.array(executed_trades_pnl)
        if len(pnl_array) == 0: return [0]*5
        
        trade_count = len(pnl_array)
        total_profit = pnl_array.sum()
        win_rate = (pnl_array > 0).sum() / trade_count
        
        wins = pnl_array[pnl_array > 0]
        losses = pnl_array[pnl_array < 0]
        plt_ratio = (wins.mean() / abs(losses.mean())) if len(wins)>0 and len(losses)>0 else 0
        
        sharpe = (pnl_array.mean() / pnl_array.std() * np.sqrt(trade_count)) if len(pnl_array)>1 and pnl_array.std()!=0 else 0
        
        return total_profit, trade_count, win_rate, plt_ratio, sharpe

    # 4. 执行多轮模拟并输出
    num_simulations = 500
    results = []
    print(f"正在进行 {num_simulations} 次【10日移动达标准入】随机模拟...")
    
    for i in range(num_simulations):
        res = run_single_simulation(trades_df, rolling_db)
        results.append(res)

    res_df = pd.DataFrame(results, columns=['profits', 'counts', 'win_rates', 'plt_ratios', 'sharpes'])
    
    # 打印结果（略，同原脚本输出逻辑）
    print("\n" + "="*40)
    print(f"统计指标 (基于 10日移动窗口达标准入)")
    print(f"平均总盈亏: {res_df['profits'].mean():,.2f} 元")
    print(f"正收益概率: {(res_df['profits'] > 0).sum() / num_simulations * 100:.2f}%")
    print(f"平均胜率: {res_df['win_rates'].mean():.2%}")
    print(f"平均夏普: {res_df['sharpes'].mean():.2f}")
    print("="*40)

if __name__ == "__main__":
    run_optimized_simulation()