import pandas as pd
import akshare as ak
import time
from datetime import timedelta
import os

# 1. 配置路径
file_path = '板块回测汇总结果_含总笔数2026-03-26-18-58-35_涨幅0.5-3%.xlsx'
output_path = '交易行情特征画像分析_纯净版.xlsx'

def get_market_features(symbol, trade_date):
    """仅获取行情与市值特征，避开财务接口"""
    try:
        symbol = str(symbol).zfill(6)
        date_str = trade_date.strftime('%Y%m%d')
        
        # --- A. 行情数据 (AkShare 最稳接口) ---
        # 获取包含交易日前后的数据
        df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
                                    start_date=(trade_date - timedelta(days=15)).strftime('%Y%m%d'),
                                    end_date=date_str, adjust="qfq")
        
        if df_hist.empty or len(df_hist) < 2: 
            return None
        
        # 前一日与当日行
        last_day = df_hist.iloc[-2]
        current_day = df_hist.iloc[-1]

        res = {
            '前日换手率': last_day['换手率'],
            '前日振幅': last_day['振幅'],
            '前日涨跌幅': last_day['涨跌幅'],
            '当日成交额': current_day['成交额'],
            '当日涨跌幅': current_day['涨跌幅'],
            '当日换手率': current_day['换手率'],
            '总市值(亿)': 0,
            '流通市值(亿)': 0
        }

        # --- B. 市值指标 (独立接口，通常比财务报表更稳) ---
        try:
            df_indicator = ak.stock_a_indicator_lg(symbol=symbol)
            df_indicator['trade_date'] = pd.to_datetime(df_indicator['trade_date'])
            # 匹配最接近交易日的指标
            indicator_match = df_indicator[df_indicator['trade_date'] <= trade_date]
            if not indicator_match.empty:
                latest_ind = indicator_match.iloc[-1]
                res['总市值(亿)'] = round(latest_ind['total_mv'] / 10000, 2)
                res['流通市值(亿)'] = round(latest_ind['circ_mv'] / 10000, 2)
        except:
            pass

        return res
    except Exception as e:
        print(f"解析 {symbol} 出错: {e}")
        return None

# 2. 读取回测数据
print("正在读取回测数据并筛选下午交易...")
df_detail = pd.read_excel(file_path, sheet_name='所有交易明细')
df_detail['交易时间'] = pd.to_datetime(df_detail['交易时间'])

# 筛选下午（13:00后）买入的单子
afternoon_buys = df_detail[(df_detail['交易类型'] == '买入') & 
                           (df_detail['交易时间'].dt.hour >= 13)].copy()

# 3. 循环处理
results = []
# 如果你想跑全量，可以将 head(50) 去掉
process_count = 10000
print(f"开始分析特征，预计处理 {process_count} 笔大收益单...")

for index, row in afternoon_buys.iterrows():
    # 获取卖出后的结果（下一行通常是平仓单）
    try:
        sell_row = df_detail.iloc[index + 1]
        pnl = sell_row['单笔盈亏']
        hold_k = sell_row['持仓K线数量']
    except:
        pnl, hold_k = 0, 0

    features = get_market_features(row['股票代码'], row['交易时间'])
    if features:
        # 合并交易信息与行情特征
        features.update({
            '股票代码': row['股票代码'],
            '买入时间': row['交易时间'],
            '盈亏金额': pnl,
            '持仓K线': hold_k
        })
        results.append(features)
        print(f"已处理: {row['股票代码']} | 盈亏: {pnl:.2f}")

    # 达到处理笔数上限跳出（演示用）
    if len(results) >= process_count:
        break
    
    time.sleep(0.3) # 避免触发频率限制

# 4. 排序与保存
if results:
    df_final = pd.DataFrame(results)
    # 核心步骤：按盈亏金额降序排列，收益最高的在前
    df_final = df_final.sort_values(by='盈亏金额', ascending=False)
    
    # 保存结果
    try:
        df_final.to_excel(output_path, index=False)
        print(f"\n✅ 分析完成！结果已按收益排序存入: {output_path}")
    except PermissionError:
        print("\n❌ 错误：Excel文件被占用，请关闭后重新运行。")
else:
    print("未获取到有效数据。")