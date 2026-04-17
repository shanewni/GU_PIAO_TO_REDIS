import pandas as pd
import os

def filter_excel_by_date_and_time(file_path, date_ranges, target_times=None):
    """
    按日期范围和具体时刻筛选买入对
    :param target_times: 列表格式，如 ['10:30', '14:00']。如果为 None 则不筛选具体时刻。
    """
    print(f"正在读取文件: {file_path}")
    
    # 1. 读取并处理股票代码格式
    df = pd.read_excel(file_path, sheet_name='所有交易明细', dtype={'股票代码': str})
    df['股票代码'] = df['股票代码'].apply(lambda x: str(x).zfill(6) if pd.notnull(x) else x)
    
    # 2. 转换时间
    df['交易时间'] = pd.to_datetime(df['交易时间'])
    
    file_dir = os.path.dirname(file_path)
    file_name_base = os.path.splitext(os.path.basename(file_path))[0]

    for start_date, end_date in date_ranges:
        print(f"正在处理区间: {start_date} 至 {end_date}...")
        
        # 3. 基础日期筛选条件
        mask = (df['交易时间'] >= start_date) & (df['交易时间'] <= end_date) & (df['交易类型'] == '买入')
        
        # 4. 增加具体时刻筛选 (例如 10:30)
        if target_times:
            # 提取时间部分并转换为 'HH:MM' 字符串格式进行匹配
            df_time_str = df['交易时间'].dt.strftime('%H:%M')
            mask = mask & (df_time_str.isin(target_times))
            time_suffix = "_特定时段"
        else:
            time_suffix = ""

        # 5. 执行筛选并获取成对索引
        buy_indices = df[mask].index

        if len(buy_indices) == 0:
            print(f"区间 {start_date} 内没有符合时间要求的买入记录，跳过。")
            continue

        sell_indices = buy_indices + 1
        combined_indices = sorted(list(set(buy_indices.union(sell_indices))))
        combined_indices = [i for i in combined_indices if i in df.index]
        
        filtered_df = df.loc[combined_indices].copy()

        # 6. 生成文件名 (加入时间后缀)
        new_file_name = f"{file_name_base}_{start_date.replace('-','')}_to_{end_date.replace('-','')}{time_suffix}.xlsx"
        output_path = os.path.join(file_dir, new_file_name)
        
        # 7. 写入 Excel 并锁定股票代码文本格式
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            filtered_df.to_excel(writer, index=False, sheet_name='所有交易明细')
            workbook  = writer.book
            worksheet = writer.sheets['所有交易明细']
            text_format = workbook.add_format({'num_format': '@'})
            col_idx = filtered_df.columns.get_loc('股票代码')
            worksheet.set_column(col_idx, col_idx, None, text_format)

        print(f"成功保存: {new_file_name} (共 {len(buy_indices)} 笔交易)")

# --- 配置区域 ---
input_file = "板块回测汇总结果_含总笔数2026-04-16-22-00-24_30f起爆，日60均线限制.xlsx"  # 替换为你的文件路径

# 1. 设定日期范围
target_ranges = [
    ("2004-05-01", "2004-09-20"),
    ("2004-09-20", "2024-12-11"),
    ("2024-12-11", "2025-04-08"),
    ("2025-04-08", "2025-08-26"),
    ("2025-08-26", "2025-12-18"),
    ("2025-12-18", "2026-05-07")
]

# 2. 设定具体的买入时刻 (只需列出你需要保留的时刻)
# 如果你想筛选全部，就保持这个列表完整；如果只想看早盘，可以只写 ['10:00', '10:30']
selected_times = ['10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30', '15:00']
# selected_times = ['14:00']

if __name__ == "__main__":
    if os.path.exists(input_file):
        # 运行筛选
        filter_excel_by_date_and_time(input_file, target_ranges, target_times=selected_times)
    else:
        print("未找到 Excel 文件。")