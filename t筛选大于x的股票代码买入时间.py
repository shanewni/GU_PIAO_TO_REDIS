import pandas as pd

def filter_profitable_stocks(file_path):
    # 1. 加载 Excel 文件
    # 使用 converters 强制将 '股票代码' 列读取为字符串，防止丢零
    df = pd.read_excel(
        file_path, 
        sheet_name='所有交易明细',
        converters={'股票代码': str}  # <--- 核心修改点
    )

    # 2. 清洗列名
    df.columns = df.columns.str.strip()

    # 3. 核心筛选逻辑
    # 提示：如果你的 Excel 中 10 代表 10%（即显示为 0.1），请将 10 改为 0.1
    condition = (df['实际盈亏比例'] > 10)
    
    filtered_df = df[condition].copy()

    # 4. 格式化处理（二次保险）
    # 确保代码是 6 位，不足的在前面补 0（处理某些可能已经变成数字的数据）
    filtered_df['股票代码'] = filtered_df['股票代码'].str.zfill(6)

    # 5. 提取指定列
    result = filtered_df[['股票代码', '交易时间', '实际盈亏比例']]

    # 6. 输出结果
    if not result.empty:
        print(f"--- 筛选结果 (共 {len(result)} 笔) ---")
        print(result.to_string(index=False))
    else:
        print("未发现实际盈亏比例大于 10 的记录。")

# 运行
filter_profitable_stocks('板块回测汇总结果_含总笔数2026-04-22-12-50-17_前复权.xlsx')