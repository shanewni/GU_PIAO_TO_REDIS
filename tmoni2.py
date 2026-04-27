import pandas as pd
import numpy as np
from datetime import datetime

# ================= 配置区 =================
FILE_PATH = "板块回测汇总结果_含总笔数2026-04-25-12-00-08_正常.xlsx" # 请确保路径正确
# ALLOWED_BUY_TIMES = ["10:00","10:30", "11:00", "11:30", "13:30", "14:00", "14:30", "15:00"] 
ALLOWED_BUY_TIMES = ["13:30", "14:00", "14:30"] # 
# ALLOWED_BUY_TIMES = ["10:00","10:30", "11:00", "11:30"] # 

# 从图片识别出的所有起爆点位列表（你可以按需删减）
# ALLOWED_START_POINTS = [
#     '三买之上4', '三买之上9', '三买之上7', '三买之上6', 
#     '二买延续3', '三买延续3', '三买之上3', '二买延续4', 
#     '一买', '三买', '二买', '二买延续2', 
#     '三买之上2', '三买之上1', '二买延续1', '三买延续1', 
#     '三买延续2', '三买之上5', '二买延续5', '三买延续4', 
#     '三买之上8', '三买延续5'
# ]
ALLOWED_START_POINTS = [
    '三买'
]
# ==========================================

# --- 新增：日期筛选配置 ---
# 格式：YYYY-MM-DD，如果不想限制，可以设置为 None
START_DATE = "2025-01-01" 
END_DATE   = "2026-05-31" 
# ==========================================

# 1. 加载数据
df = pd.read_excel(FILE_PATH, sheet_name='所有交易明细')
df.columns = df.columns.str.strip() 
df['交易时间'] = pd.to_datetime(df['交易时间'])

# 2. 预处理
trades = []
for code, group in df.groupby('股票代码'):
    group = group.sort_values('交易时间')
    temp_buy = None
    
    for _, row in group.iterrows():
        if row['交易类型'] == '买入':
            temp_buy = row 
            
        elif row['交易类型'] == '策略卖出' and temp_buy is not None:
            point_val = str(row['起爆点位置']).strip()
            
            # --- 过滤逻辑 ---
            # 1. 起爆点过滤
            point_ok = (ALLOWED_START_POINTS is None) or (point_val in ALLOWED_START_POINTS)
            
            # 2. 买入时间点过滤 (HH:MM)
            buy_time_dt = temp_buy['交易时间']
            buy_hm = buy_time_dt.strftime('%H:%M')
            time_ok = buy_hm in ALLOWED_BUY_TIMES
            
            # 3. 新增：年月日时间段过滤
            date_ok = True
            if START_DATE:
                date_ok = date_ok and (buy_time_dt >= pd.to_datetime(START_DATE))
            if END_DATE:
                # 为了包含结束当天，通常会比对到 23:59:59，或者直接对比日期
                date_ok = date_ok and (buy_time_dt <= pd.to_datetime(END_DATE).replace(hour=23, minute=59, second=59))

            if point_ok and time_ok and date_ok:
                trades.append({
                    '股票代码': code,
                    '买入时间': buy_time_dt,
                    '卖出时间': row['交易时间'],
                    '单笔盈亏': row['单笔盈亏'],
                    '实际盈亏比例': row['实际盈亏比例'],
                    '起爆点位置': point_val
                })
            temp_buy = None

trades_df = pd.DataFrame(trades).sort_values('买入时间')

if trades_df.empty:
    print("❌ 未匹配到任何交易。请检查：1. ALLOWED_BUY_TIMES 是否包含数据中的买入时间；2. ALLOWED_START_POINTS 是否正确。")
else:
    print(f"✅ 成功匹配到 {len(trades_df)} 组有效交易。")
    # 打印前几行看看对不对
    print(trades_df[['股票代码', '买入时间', '起爆点位置', '单笔盈亏']].head())

def run_single_simulation(all_trades):
    """
    执行单次模拟：同一时间只持有一只票
    """
    current_time = datetime(2000, 1, 1)
    executed_trades_pnl = []
    
    # 按时间分组，提高采样效率
    time_groups = all_trades.groupby('买入时间')
    unique_buy_times = sorted(all_trades['买入时间'].unique())
    
    for buy_time in unique_buy_times:
        # 只有当前持仓已卖出，且到了新的买入时间点才开仓
        if pd.to_datetime(buy_time) >= current_time:
            available_signals = time_groups.get_group(buy_time)
            # 随机选一只
            selected_trade = available_signals.sample(n=1).iloc[0]
            
            executed_trades_pnl.append(selected_trade['单笔盈亏'])
            # 卖出时间不限制，直接更新当前时间锁，确保持仓不重叠
            current_time = selected_trade['卖出时间']
            
    pnl_array = np.array(executed_trades_pnl)
    trade_count = len(pnl_array)
    
    # 指标计算
    total_profit = pnl_array.sum()
    win_rate = (pnl_array > 0).sum() / trade_count if trade_count > 0 else 0
    
    wins = pnl_array[pnl_array > 0]
    losses = pnl_array[pnl_array < 0]
    avg_win = wins.mean() if len(wins) > 0 else 0
    avg_loss = abs(losses.mean()) if len(losses) > 0 else 0
    plt_ratio = avg_win / avg_loss if avg_loss != 0 else 0
    
    sharpe = 0
    if trade_count > 1 and pnl_array.std() != 0:
        sharpe = (pnl_array.mean() / pnl_array.std()) * np.sqrt(trade_count)

    return total_profit, trade_count, win_rate, plt_ratio, sharpe

NUM_SIMULATIONS = 500
# 3. 执行模拟
results = []
print(f"正在进行 {NUM_SIMULATIONS} 次模拟，请稍候...")

for _ in range(NUM_SIMULATIONS):
    if not trades_df.empty:
        res = run_single_simulation(trades_df)
        results.append(res)

res_df = pd.DataFrame(results, columns=['profits', 'counts', 'win_rates', 'plt_ratios', 'sharpes'])

# 4. 统计输出
def print_metric(label, series, is_percent=False):
    fmt = ".2%" if is_percent else ",.2f"
    print(f"\n【{label}】")
    print(f"最高: {series.max():{fmt}} | 最低: {series.min():{fmt}}")
    print(f"平均: {series.mean():{fmt}} | 中位数: {series.median():{fmt}}")

print("\n" + "="*50)
print(f"策略回测报告 (买入时段: {ALLOWED_BUY_TIMES} | 起爆点: {ALLOWED_START_POINTS}) | 日期范围: {START_DATE} ~ {END_DATE}")
print("="*50)

if not res_df.empty:
    print_metric("总盈亏 (元)", res_df['profits'])
    print(f"\n正收益概率: {(res_df['profits'] > 0).sum() / NUM_SIMULATIONS:.2%}")
    print_metric("交易次数", res_df['counts'])
    print_metric("平均胜率", res_df['win_rates'], True)
    print_metric("盈亏比", res_df['plt_ratios'])
    print_metric("夏普比率", res_df['sharpes'])
else:
    print("无模拟数据。")
print("\n" + "="*50)