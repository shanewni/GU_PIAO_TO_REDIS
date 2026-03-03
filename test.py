from mootdx.reader import Reader
import pandas as pd

def get_exact_tdx_30min(symbol, tdx_path=r"D:\zd_hbzq"):
    reader = Reader.factory(market='std', tdxdir=tdx_path)
    df_5m = reader.fzline(symbol=symbol)
    
    if df_5m is None or df_5m.empty:
        return pd.DataFrame()

    # 1. 基础清洗与重命名
    df_5m = df_5m.rename(columns={
        'open': '开盘价', 'high': '最高价', 'low': '最低价',
        'close': '收盘价', 'volume': '成交量', 'amount': '成交额'
    })
    
    # 2. 预处理精度（极其重要，解决 41.79999 问题）
    price_cols = ['开盘价', '最高价', '最低价', '收盘价']
    df_5m[price_cols] = df_5m[price_cols].round(2)

    # 3. 核心合成逻辑：为了对齐通达信的 11:30 和 15:00
    # 我们使用 origin='start_day' 并配合 offset 来强制对齐 A 股开盘时间
    df_30m = df_5m.resample(
        '30Min', 
        closed='right',   # 每根K线包含右侧边界（如 10:00 包含 10:00 那一分钟）
        label='right',    # 用右侧时间命名（显示为 10:00）
        origin='start_day' # 从每日 00:00 开始计算偏移
    ).agg({
        '开盘价': 'first',
        '最高价': 'max',
        '最低价': 'min',
        '收盘价': 'last',
        '成交量': 'sum',
        '成交额': 'sum'
    })

    # 4. 剔除中午休市和非交易时段的空行
    # 通达信 30 分钟线只有 10:00, 10:30, 11:00, 11:30, 13:30, 14:00, 14:30, 15:00
    df_30m = df_30m.dropna(subset=['开盘价'])
    
    # 过滤掉非正常的分钟点（可选，确保纯净）
    valid_times = ['10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30', '15:00']
    df_30m = df_30m[df_30m.index.strftime('%H:%M').isin(valid_times)]

    # 5. 最后修正一遍精度
    df_30m[price_cols] = df_30m[price_cols].round(2)
    
    return df_30m

# --- 测试验证 ---
code = '002138'
df_test = get_exact_tdx_30min(code)
print(f"\n>>> 股票 {code} 30分钟线合成完成")
print(f">>> 总量: {len(df_test)} 根")
print("\n>>> 结尾最后五行数据（请对比通达信 30分钟 界面）：")
print(df_test.tail(5))