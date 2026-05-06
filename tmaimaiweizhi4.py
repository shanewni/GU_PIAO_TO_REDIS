import pandas as pd
import numpy as np
import os

def analyze_and_save_to_excel_daily_fix():
    # 严格参照文件名[cite: 1]
    file_name = "板块回测汇总结果_含总笔数2026-04-27-17-00-38_正常_增加日线起爆位置.xlsx"
    new_sheet_name = "每日10日移动组合变化"
    
    if not os.path.exists(file_name):
        print(f"错误：未找到文件 '{file_name}'，请确认文件名拼写及路径。")
        return

    print(f"🚀 正在按“交易日”维度计算 10 日移动窗口演变...")

    # 1. 读取数据[cite: 1]
    try:
        df = pd.read_excel(file_name, sheet_name="所有交易明细")
    except Exception as e:
        print(f"读取失败: {e}")
        return

    # 2. 预处理[cite: 1]
    time_col = '交易时间'
    df_sell = df[df['交易类型'].str.contains('卖出', na=False)].copy()
    df_sell['交易时间'] = pd.to_datetime(df_sell[time_col])
    
    # 【细节修正】：统一转化为日期对象，确保按“日”统计[cite: 1]
    df_sell['日期'] = df_sell['交易时间'].dt.date
    df_sell = df_sell.sort_values('交易时间')
    
    # 策略名拼接[cite: 1]
    df_sell['策略组合'] = df_sell['起爆点位置(日线)'].astype(str) + " + " + df_sell['起爆点位置(30分)'].astype(str)
    
    # 收益口径适配[cite: 1]
    profit_col = '实际盈亏比例' if '实际盈亏比例' in df_sell.columns else '单笔盈亏'
    df_sell['calc_profit'] = pd.to_numeric(df_sell[profit_col], errors='coerce').fillna(0)
    df_sell['单笔盈亏'] = pd.to_numeric(df_sell['单笔盈亏'], errors='coerce').fillna(0)

    # 3. 获取所有唯一的交易日期序列
    all_trading_days = sorted(df_sell['日期'].unique())
    daily_top_list = []

    # 4. 核心：遍历每一个交易日，回溯过去 10 天
    for current_day in all_trading_days:
        # 回溯 10 个自然日[cite: 1]
        start_date = current_day - pd.Timedelta(days=10)
        
        # 筛选 [T-10, T] 范围内的平仓单
        mask = (df_sell['日期'] > start_date) & (df_sell['日期'] <= current_day)
        window_data = df_sell[mask]
        
        if window_data.empty:
            continue
            
        # 按组合聚合[cite: 1]
        stats = window_data.groupby('策略组合').agg(
            笔数=('单笔盈亏', 'count'),
            总净利润=('单笔盈亏', 'sum'),
            均值=('calc_profit', 'mean'),
            标准差=('calc_profit', 'std')
        ).reset_index()
        
        # 计算夏普[cite: 1]
        stats['10日夏普'] = np.where(
            (stats['标准差'] > 0) & (stats['标准差'].notna()),
            stats['均值'] / stats['标准差'],
            0
        )
        
        # 5. 应用筛选：10日夏普 > 0.1 且 总净利润 > 0[cite: 1]
        valid = stats[(stats['10日夏普'] > 0.1) & (stats['总净利润'] > 0)].copy()
        
        if not valid.empty:
            # 取 Top 15[cite: 1]
            top_15 = valid.sort_values(by=['10日夏普', '总净利润'], ascending=False).head(15)
            top_15['日期'] = current_day
            daily_top_list.append(top_15[['日期', '策略组合', '笔数', '10日夏普', '总净利润']])

    if not daily_top_list:
        print("未发现符合条件的每日组合。")
        return

    final_df = pd.concat(daily_top_list).reset_index(drop=True)

    # 6. 写入 Excel 新页[cite: 1]
    print(f"📝 正在保存结果到 Excel...")
    try:
        with pd.ExcelWriter(file_name, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            final_df.to_excel(writer, sheet_name=new_sheet_name, index=False)
        print(f"✅ 成功！新页 '{new_sheet_name}' 已添加至 {file_name}")
    except Exception as e:
        print(f"写入失败: {e}")

if __name__ == "__main__":
    analyze_and_save_to_excel_daily_fix()