import pandas as pd
import numpy as np

# 1. 加载数据 - 修正为读取 CSV 
file_path = '板块回测汇总结果_含总笔数2026-04-17-16-25-06_30f起爆，日60均线限制，创业科创板.xlsx'
df = pd.read_excel(file_path, sheet_name='所有交易明细')

# 2. 预处理
# 关键修正：在原始数据中，“单k涨幅”通常记录在【买入】那一行，而“单笔盈亏”记录在【卖出】那一行。
# 我们需要把买入时的涨幅“传递”给对应的卖出交易，或者直接统计所有非空涨幅的行。
# 这里采用：先筛选出有涨幅的行，并关联其对应的盈亏结果。

# 转换数值
df['单k涨幅'] = pd.to_numeric(df['单k涨幅'], errors='coerce')
df['单笔盈亏'] = pd.to_numeric(df['单笔盈亏'], errors='coerce')

# 核心逻辑：因为买入行有涨幅没盈亏，卖出行有盈亏没涨幅。
# 我们通过“股票代码”和位置进行简单的向上填充(bfill)，让卖出行也能拿涨幅数据。
df['单k涨幅'] = df.groupby('股票代码')['单k涨幅'].ffill() 

# 现在过滤出卖出记录进行统计
df_trades = df[df['交易类型'].str.contains('卖出|止损|平仓', na=False)].copy()

# 3. 定义区间
bins = list(range(11))
labels = [f"{i}-{i+1}" for i in range(10)]
df_trades['涨幅区间'] = pd.cut(df_trades['单k涨幅'], bins=bins, labels=labels, right=False)

# 4. 统计函数
def get_stats(group):
    total_count = len(group)
    if total_count == 0:
        return pd.Series([0, 0, 0, 0], index=['交易笔数', '总净利润', '胜率(%)', '盈亏比'])
    
    total_profit = group['单笔盈亏'].sum()
    win_count = (group['单笔盈亏'] > 0).sum()
    win_rate = (win_count / total_count) * 100
    
    pos_trades = group[group['单笔盈亏'] > 0]['单笔盈亏']
    neg_trades = group[group['单笔盈亏'] < 0]['单笔盈亏']
    
    avg_win = pos_trades.mean() if not pos_trades.empty else 0
    avg_loss = abs(neg_trades.mean()) if not neg_trades.empty else 0
    profit_loss_ratio = avg_win / avg_loss if avg_loss != 0 else (np.inf if avg_win > 0 else 0)
    
    return pd.Series([total_count, total_profit, win_rate, profit_loss_ratio], 
                     index=['交易笔数', '总净利润', '胜率(%)', '盈亏比'])

# 5. 分组并格式化
result = df_trades.groupby('涨幅区间', observed=False).apply(get_stats)

# 转换格式方便查看
formatted_res = result.copy()
formatted_res['总净利润'] = formatted_res['总净利润'].map('{:,.2f}'.format)
formatted_res['胜率(%)'] = formatted_res['胜率(%)'].map('{:.2f}%'.format)
formatted_res['盈亏比'] = formatted_res['盈亏比'].map('{:.2f}'.format)

print("==========================================================")
print("          单K涨幅区间多维度统计汇总")
print("==========================================================")
print(formatted_res)