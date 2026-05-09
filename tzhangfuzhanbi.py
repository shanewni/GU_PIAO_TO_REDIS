import pandas as pd
import numpy as np

# 1. 加载数据 - 修正为读取 CSV 
file_path = '板块回测汇总结果_含总笔数2026-05-08-23-45-39_正常_止损点移到底分型起爆_2%风险金.xlsx'
df = pd.read_excel(file_path, sheet_name='所有交易明细')

# 2. 预处理
# 转换数值
df['单k涨幅'] = pd.to_numeric(df['单k涨幅'], errors='coerce')
df['单笔盈亏'] = pd.to_numeric(df['单笔盈亏'], errors='coerce')

# 核心逻辑：买入行有“单k涨幅”，卖出行有“单笔盈亏”。
# 使用 ffill() 将买入行的涨幅数据向下填充到同一只股票的卖出行
df['单k涨幅'] = df.groupby('股票代码')['单k涨幅'].ffill() 

# 过滤出卖出记录进行统计（这些行现在既有涨幅区间，又有盈亏金额）
df_trades = df[df['交易类型'].str.contains('卖出|止损|平仓', na=False)].copy()

# 3. 定义区间 (0-1, 1-2, ..., 9-10)
bins = list(range(11))
labels = [f"{i}-{i+1}" for i in range(10)]
df_trades['涨幅区间'] = pd.cut(df_trades['单k涨幅'], bins=bins, labels=labels, right=False)

# 4. 统计函数
def get_stats(group):
    total_count = len(group)
    if total_count == 0:
        return pd.Series([0, 0, 0, 0, 0], index=['交易笔数', '总净利润', '平均单笔盈利', '胜率(%)', '盈亏比'])
    
    total_profit = group['单笔盈亏'].sum()
    avg_trade_profit = group['单笔盈亏'].mean()  # 新增：单笔盈利数据
    
    win_count = (group['单笔盈亏'] > 0).sum()
    win_rate = (win_count / total_count) * 100
    
    # 盈亏比计算
    pos_trades = group[group['单笔盈亏'] > 0]['单笔盈亏']
    neg_trades = group[group['单笔盈亏'] < 0]['单笔盈亏']
    
    avg_win = pos_trades.mean() if not pos_trades.empty else 0
    avg_loss = abs(neg_trades.mean()) if not neg_trades.empty else 0
    profit_loss_ratio = avg_win / avg_loss if avg_loss != 0 else (np.inf if avg_win > 0 else 0)
    
    return pd.Series([
        total_count, 
        total_profit, 
        avg_trade_profit, 
        win_rate, 
        profit_loss_ratio
    ], index=['交易笔数', '总净利润', '平均单笔盈利', '胜率(%)', '盈亏比'])

# 5. 执行分组统计
result = df_trades.groupby('涨幅区间', observed=False).apply(get_stats)

# 6. 格式化输出
formatted_res = result.copy()
formatted_res['总净利润'] = formatted_res['总净利润'].map('{:,.2f}'.format)
formatted_res['平均单笔盈利'] = formatted_res['平均单笔盈利'].map('{:,.2f}'.format)
formatted_res['胜率(%)'] = formatted_res['胜率(%)'].map('{:.2f}%'.format)
formatted_res['盈亏比'] = formatted_res['盈亏比'].map('{:.2f}'.format)

print("========================================================================")
print("              单K涨幅区间多维度统计汇总 (含单笔平均盈利)")
print("========================================================================")
print(formatted_res)