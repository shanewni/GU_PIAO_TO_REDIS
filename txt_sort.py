import pandas as pd

def export_to_tdx_custom(excel_path, output_filename='to_tdx_sort.txt'):
    df = pd.read_excel(excel_path)
    
    def format_code(x):
        code = str(x).split('.')[0].zfill(6)
        if code.startswith('6'):
            return '1|' + code
        else:
            return '0|' + code

    df['tdx_code'] = df['股票代码'].apply(format_code)
    
    if '日期' not in df.columns:
        df['date'] = '20240101'
    else:
        df['date'] = df['日期'].astype(str).str.replace('-', '')
    
    # 手动拼接每行内容，避免 pandas 添加引号
    with open(output_filename, 'w', encoding='utf-8') as f:
        for _, row in df.iterrows():
            line = f"{row['tdx_code']}|{row['date']}|{row['累计收益']}"
            f.write(line + '\n')
    
    print(f"转换完成！请将 {output_filename} 导入通达信。")

# 使用示例
export_to_tdx_custom('板块回测汇总结果_含总笔数2026-04-09-16-52-11_30f60均线上底分型3根买入_跌破60均线_笔高度高于60均线_日线.xlsx')