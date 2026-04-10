import pandas as pd

def convert_to_daily_signals(excel_file, sheet_name='Sheet1', output_file='tdx_signals.txt'):
    """
    将交易明细Excel转换为通达信日线信号TXT文件
    格式：市场代码|股票代码|日期|信号值
    买入=1，卖出=-1
    支持同一天多个信号（保留所有信号）
    """
    
    # 读取Excel文件
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    
    # 打印列名，方便调试
    print("Excel列名：", df.columns.tolist())
    
    # 列名映射（根据你的实际列名调整）
    column_mapping = {
        '股票代码': 'stock_code',
        '交易时间': 'trade_time', 
        '交易类型': 'trade_type',
        '价格': 'price'
    }
    
    # 重命名列
    df = df.rename(columns=column_mapping)
    
    # 转换股票代码为字符串，并确保是6位（补零）
    df['stock_code'] = df['stock_code'].astype(str).str.zfill(6)
    
    # 转换交易时间为datetime格式
    df['trade_time'] = pd.to_datetime(df['trade_time'])
    
    # 提取日期（格式：YYYYMMDD）
    df['trade_date'] = df['trade_time'].dt.strftime('%Y%m%d')
    
    # 转换交易类型为信号值：买入=1，卖出=-1
    df['signal'] = df['trade_type'].apply(lambda x: 1 if '买入' in str(x) else -1)
    
    # 添加市场代码：上海股票（60开头）=1，深圳股票（00/30开头）=0
    df['market_code'] = df['stock_code'].apply(
        lambda x: '1' if x.startswith('60') else '0'
    )
    
    # 重要修改：保留所有信号，不对同一天进行合并
    # 直接使用原始数据，每条交易记录都生成一条信号
    output_lines = df.apply(
        lambda row: f"{row['market_code']}|{row['stock_code']}|{row['trade_date']}|{row['signal']}",
        axis=1
    )
    
    # 保存为TXT文件（ANSI编码，通达信兼容）
    with open(output_file, 'w', encoding='ansi') as f:
        f.write('\n'.join(output_lines))
    
    # 打印统计信息
    print(f"\n✅ 转换完成！")
    print(f"输出文件：{output_file}")
    print(f"总信号数：{len(output_lines)}")
    print(f"\n信号统计：")
    print(f"买入信号：{(df['signal'] == 1).sum()}")
    print(f"卖出信号：{(df['signal'] == -1).sum()}")
    print(f"\n涉及股票：{df['stock_code'].nunique()} 只")
    
    # 显示股票代码示例
    print(f"\n股票代码示例：")
    for code in df['stock_code'].unique()[:5]:
        print(f"  {code}")
    
    # 显示前10条数据预览
    print(f"\n数据预览（前10条）：")
    for i, line in enumerate(output_lines[:10]):
        print(f"{i+1}. {line}")
    
    # 检查同一天多个信号的情况
    same_day_signals = df.groupby(['stock_code', 'trade_date']).size()
    multi_signals = same_day_signals[same_day_signals > 1]
    if len(multi_signals) > 0:
        print(f"\n⚠️  注意：发现同一天多个信号的情况：")
        for (code, date), count in multi_signals.items():
            print(f"  股票 {code} 在 {date} 有 {count} 个信号")
    
    return output_file

# ========== 使用示例 ==========
if __name__ == "__main__":
    # 修改为你的实际文件名
    excel_file = "板块回测汇总结果_含总笔数2026-04-10-16-28-42.xlsx"  # Excel文件名
    sheet_name = "所有交易明细"        # 工作表名称
    output_file = "tdx_signals.txt"  # 输出文件名
    
    try:
        convert_to_daily_signals(excel_file, sheet_name, output_file)
        print(f"\n📌 下一步操作：")
        print("1. 打开通达信，按 .901 打开自定义数据管理器")
        print("2. 点击'新建'，类型选择'序列数据（日期，数值）'")
        print("3. 记住数据号，点击'修改数据' → '导入'")
        print(f"4. 选择生成的 {output_file} 文件导入")
        print("5. 编写公式：SIGNALS_USER(数据号, 0)")
    except FileNotFoundError:
        print(f"❌ 错误：找不到文件 {excel_file}")
        print("请确保Excel文件在当前目录下，或修改excel_file变量为正确路径")
    except Exception as e:
        print(f"❌ 错误：{e}")
        print("请检查Excel文件的列名是否正确，需要包含：股票代码、交易时间、交易类型、价格")
