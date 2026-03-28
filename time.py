import pandas as pd

# 1. 加载数据 - 注意这里改为 pd.read_excel，并指定 sheet_name
file_path = '板块回测汇总结果_含总笔数2026-03-19-22-17-08_上市一年以上_增加买入盈亏_行业涨幅前60.xlsx'
# 如果你的工作表名字不完全叫这个，请修改 sheet_name 的值
sheet_name = '所有交易明细' 

try:
    df = pd.read_excel(file_path, sheet_name=sheet_name)
except Exception as e:
    print(f"读取失败，请检查文件名或工作表名称是否正确: {e}")
    # 如果不知道确切sheet名，可以先用下行代码列出所有sheet名看看
    # print(pd.ExcelFile(file_path).sheet_names)
    exit()

# 2. 转换时间格式并提取小时/分钟
# 确保交易时间列是 datetime 类型
df['交易时间'] = pd.to_datetime(df['交易时间'])
df['时分'] = df['交易时间'].dt.time

# 3. 定义时间段划分逻辑
def assign_time_slot(t):
    if t <= pd.to_datetime('10:30:00').time():
        return '09:30-10:30 (早盘确认)'
    elif t <= pd.to_datetime('11:30:00').time():
        return '10:30-11:30 (午前惯性)'
    elif t <= pd.to_datetime('14:00:00').time():
        return '13:00-14:00 (午后博弈)'
    else:
        return '14:00-15:00 (尾盘效应)'

# 4. 筛选买入动作
buy_df = df[df['交易类型'] == '买入'].copy()
buy_df['时段标签'] = buy_df['时分'].apply(assign_time_slot)

# 5. 关联卖出时的盈亏结果 (改进版：确保数值化)
def get_pnl_value(row_index):
    try:
        # 获取买入行的下一行数据（通常是卖出行）
        sell_row = df.iloc[row_index + 1]
        
        # 验证是否为同一只股票
        if sell_row['股票代码'] == df.loc[row_index, '股票代码']:
            # 强制转换为浮点数，处理可能的空字符串或格式问题
            val = pd.to_numeric(sell_row['单笔盈亏'], errors='coerce')
            return val
    except:
        return None

buy_df['单笔盈亏数值'] = [get_pnl_value(i) for i in buy_df.index]

# 6. 分时段汇总统计 (改进版：处理 NaN)
# 过滤掉无法获取盈亏的异常行
valid_buys = buy_df.dropna(subset=['单笔盈亏数值'])

stats = valid_buys.groupby('时段标签').agg(
    交易笔数=('单笔盈亏数值', 'count'),
    盈利笔数=('单笔盈亏数值', lambda x: (x > 0).sum()),
    平均盈利金额=('单笔盈亏数值', lambda x: x[x > 0].mean()),
    平均亏损金额=('单笔盈亏数值', lambda x: x[x < 0].mean())
)

# 7. 计算盈亏比与胜率
stats['平均盈亏比'] = (stats['平均盈利金额'] / stats['平均亏损金额'].abs()).round(2)
stats['胜率(%)'] = (stats['盈利笔数'] / stats['交易笔数'] * 100).round(2)

print("=== 分时段【盈利质量】深度分析报告 ===")
print(stats[['交易笔数', '胜率(%)', '平均盈亏比', '平均盈利金额', '平均亏损金额']])