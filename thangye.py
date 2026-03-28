import pandas as pd
import akshare as ak
import time
from datetime import timedelta

# 1. 配置路径
file_path = '板块回测汇总结果_含总笔数2026-03-26-18-58-35_涨幅0.5-3%.xlsx'
output_path = '交易非技术特征画像报告.xlsx'

def get_stock_daily_features(symbol, trade_date):
    """
    获取指定股票在交易日前一天的非技术特征
    """
    try:
        # 格式化代码：AkShare通常需要 6 位数字
        symbol = str(symbol).zfill(6)
        # 获取前复权日频数据
        # 实际操作中建议先下载全量数据到本地缓存，避免频繁调用API被封
        df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
                                    start_date=(trade_date - timedelta(days=10)).strftime('%Y%m%d'),
                                    end_date=trade_date.strftime('%Y%m%d'), 
                                    adjust="qfq")
        
        if df_hist.empty: return None
        
        # 找到交易日当天或前一天的行
        last_day = df_hist.iloc[-2] # 前一日数据
        current_day = df_hist.iloc[-1] # 交易当日数据
        
        return {
            '前日换手率': last_day['换手率'],
            '当日成交额': current_day['成交额'],
            '前日振幅': last_day['振幅'],
            '当日涨跌幅': current_day['涨跌幅']
        }
    except:
        return None

# 2. 读取回测明细
print("正在读取回测数据...")
df_detail = pd.read_excel(file_path, sheet_name='所有交易明细')

# 3. 筛选出下午的交易单（基于你上午不买的逻辑）
df_detail['交易时间'] = pd.to_datetime(df_detail['交易时间'])
afternoon_buys = df_detail[(df_detail['交易类型'] == '买入') & 
                           (df_detail['交易时间'].dt.hour >= 13)].copy()

# 4. 关联 AkShare 数据（演示前 50 笔，实战建议全量处理并加 Time.sleep）
print(f"开始关联非技术特征，共需处理 {len(afternoon_buys)} 笔交易...")
results = []

for index, row in afternoon_buys.iterrows(): # 演示前50笔
    stock_code = row['股票代码']
    trade_time = row['交易时间']
    
    # 获取卖出结果（下一行）
    try:
        sell_row = df_detail.iloc[index + 1]
        pnl = sell_row['单笔盈亏']
    except:
        pnl = 0

    features = get_stock_daily_features(stock_code, trade_time)
    
    trade_profile = {
        '股票代码': stock_code,
        '交易时间': trade_time,
        '盈亏金额': pnl,
        '结果标签': '盈利' if pnl > 0 else '亏损'
    }
    
    if features:
        trade_profile.update(features)
    
    results.append(trade_profile)
    time.sleep(0.2) # 礼貌访问接口

# 5. 生成画像报告
df_final = pd.DataFrame(results)

# 6. 非技术特征统计分析（直观看到提升点）
if not df_final.empty:
    analysis = df_final.groupby('结果标签').agg({
        '前日换手率': 'mean',
        '当日成交额': 'mean',
        '前日振幅': 'mean'
    })
    print("\n=== 非技术特征对比分析 ===")
    print(analysis)
    
    # 保存结果
    df_final.to_excel(output_path, index=False)
    print(f"\n报告已生成至: {output_path}")