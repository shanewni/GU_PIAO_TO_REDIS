import pandas as pd
import numpy as np

# 1. 加载数据
file_path = '板块回测汇总结果_含总笔数2026-03-30-19-04-09_涨幅0.5-3%_14点_mfi_昨日与平均线比较_3k保本_2024-2026.xlsx'
sheet_name = '所有交易明细' 

try:
    df = pd.read_excel(file_path, sheet_name=sheet_name)
except Exception as e:
    print(f"读取失败: {e}")
    exit()

# 2. 转换时间格式
df['交易时间'] = pd.to_datetime(df['交易时间'])

# --- 修改部分：手动计算半小时区间，避免使用 30T 或 30min 报错 ---
def get_30min_slot(dt):
    hour = dt.hour
    minute = "00" if dt.minute < 30 else "30"
    return f"{hour:02d}:{minute}"

df['时段标签'] = df['交易时间'].apply(get_30min_slot)

# 3. 筛选买入动作并关联盈亏
buy_df = df[df['交易类型'] == '买入'].copy()

def get_pnl_value(idx):
    try:
        # 获取买入行的下一行数据
        sell_row = df.iloc[idx + 1]
        if sell_row['股票代码'] == df.loc[idx, '股票代码']:
            return pd.to_numeric(sell_row['单笔盈亏'], errors='coerce')
    except:
        return None
    return None

buy_df['单笔盈亏数值'] = [get_pnl_value(i) for i in buy_df.index]
valid_buys = buy_df.dropna(subset=['单笔盈亏数值'])

# 4. 统计分析
stats = valid_buys.groupby('时段标签').agg(
    交易笔数=('单笔盈亏数值', 'count'),
    盈利笔数=('单笔盈亏数值', lambda x: (x > 0).sum()),
    总盈亏=('单笔盈亏数值', 'sum'),
    平均盈利=('单笔盈亏数值', lambda x: x[x > 0].mean() if (x > 0).any() else 0),
    平均亏损=('单笔盈亏数值', lambda x: x[x < 0].mean() if (x < 0).any() else 0)
)

# 5. 计算衍生指标
stats['胜率(%)'] = (stats['盈利笔数'] / stats['交易笔数'] * 100).round(2)
stats['盈亏比'] = (stats['平均盈利'] / stats['平均亏损'].abs()).round(2)

# 按时间顺序排列
stats = stats.sort_index()

print("\n=== 每30分钟颗粒度【盈利质量】分析报告 ===")
print(stats[['交易笔数', '胜率(%)', '盈亏比', '总盈亏']])

# 可选：保存到 Excel 方便查看
# stats.to_excel("分时段分析结果.xlsx")