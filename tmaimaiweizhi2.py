import pandas as pd
import glob
import os
import numpy as np

def analyze_advanced_performance():
    # 1. 自动定位最新的回测结果文件 (请根据实际情况修改匹配字符串)
    files = glob.glob("板块回测汇总结果_含总笔数2026-04-27-17-00-38_正常_增加日线起爆位置.xlsx")  
    if not files:
        print("错误：未找到回测结果 Excel 文件。")
        return
    
    latest_file = max(files, key=os.path.getctime)
    print(f"正在读取文件: {latest_file}\n")

    try:
        df = pd.read_excel(latest_file, sheet_name="所有交易明细")
    except Exception as e:
        print(f"读取失败: {e}")
        return

    # 2. 筛选有效平仓记录
    df_sell = df[df['交易类型'].str.contains('卖出', na=False)].copy()
    if df_sell.empty:
        print("提示：明细表中没有卖出记录，无法统计收益。")
        return

    # 填充可能为空的K线涨跌幅数据为0
    k_cols = ['买点前3K涨跌幅(%)', '买点前2K涨跌幅(%)', '买点前1K涨跌幅(%)', 
              '买点后1K涨跌幅(%)', '买点后2K涨跌幅(%)', '买点后3K涨跌幅(%)']
    for col in k_cols:
        if col in df_sell.columns:
            df_sell[col] = pd.to_numeric(df_sell[col], errors='coerce').fillna(0)

    # ==========================================
    # 核心聚合计算函数
    # ==========================================
    def get_stats(group):
        profits = group['单笔盈亏']
        total = len(profits)
        win_trades = profits[profits > 0]
        loss_trades = profits[profits < 0]
        
        # 1. 基础指标
        win_rate = (len(win_trades) / total * 100) if total > 0 else 0
        avg_win = win_trades.mean() if not win_trades.empty else 0
        avg_loss = abs(loss_trades.mean()) if not loss_trades.empty else 0
        pl_ratio = (avg_win / avg_loss) if avg_loss != 0 else 0
        avg_profit = profits.mean()
        
        # 2. 夏普比率 (简化版：单笔收益均值 / 单笔收益标准差)
        # 这里的夏普代表了交易表现的稳定性，值越高说明收益越整齐，不是靠一两笔暴利拉起来的
        std_profit = profits.std()
        sharpe = (avg_profit / std_profit) if (std_profit != 0 and not np.isnan(std_profit)) else 0
        
        # 3. 性价比 (综合得分)
        # 公式：(胜率 * 盈亏比) * log10(交易笔数)
        # 解释：胜率盈亏比决定基因，交易笔数决定该规律的统计可靠性
        performance_score = (win_rate / 100 * pl_ratio) * np.log10(total + 1) if total > 0 else 0
        
        return pd.Series({
            '交易笔数': int(total),
            '胜率(%)': round(win_rate, 2),
            '盈亏比': round(pl_ratio, 2),
            '夏普': round(sharpe, 3),
            '性价比': round(performance_score, 3),
            '平均单笔': round(avg_profit, 2),
            '总净利润': round(profits.sum(), 2)
        })

    # 设置Pandas打印格式
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1500)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)

    # ==========================================
    # 维度一：多周期位置共振统计 (升级排序版)
    # ==========================================
    print("=" * 120)
    print(f"{'维度一：多周期位置共振统计 (按【性价比】降序，仅展示交易>5笔的组合)':^120}")
    print("-" * 120)
    if '起爆点位置(日线)' in df_sell.columns and '起爆点位置(30分)' in df_sell.columns:
        res_multi = df_sell.groupby(['起爆点位置(日线)', '起爆点位置(30分)']).apply(get_stats).reset_index()
        
        # 过滤掉偶然样本
        res_multi = res_multi[res_multi['交易笔数'] >= 5]
        
        # 【核心修改】按照性价比排序
        res_multi = res_multi.sort_values(by='性价比', ascending=False)
        
        print(res_multi.to_string(index=False))
    else:
        print("未检测到日线或30分位置字段，请确保回测代码已更新。")

    # ==========================================
    # 维度二：起爆前后微观动能特征 (盈利组 vs 亏损组)
    # ==========================================
    print("\n" + "=" * 100)
    print(f"{'维度二：起爆前后 K线涨跌幅特征画像 (盈利单 vs 亏损单)':^100}")
    print("解读：对比成功的突破和失败的突破，在买入前后几根K线上有什么不同表现？")
    print("-" * 100)
    
    # 将结果分为盈利和亏损两组，计算这几根K线的平均涨跌幅
    if all(c in df_sell.columns for c in k_cols):
        df_sell['是否盈利标签'] = np.where(df_sell['单笔盈亏'] > 0, '✅ 盈利组 (赚钱出局)', '❌ 亏损组 (止损/失败)')
        k_features = df_sell.groupby('是否盈利标签')[k_cols].mean().round(2)
        print(k_features.to_string())
    else:
        print("未检测到完整的买点前后K线涨跌幅字段。")

    # ==========================================
    # 维度三：起爆后第1根K线（确认K）强度对最终胜率的指引
    # ==========================================
    print("\n" + "=" * 100)
    print(f"{'维度三：起爆后第1根K线（确认K）强度对最终胜率的指引':^100}")
    print("解读：买入后紧接着的半小时(后1K)，走势强度如何影响最终这笔交易的成败？")
    print("-" * 100)
    
    if '买点后1K涨跌幅(%)' in df_sell.columns:
        # 对买点后1K的涨跌幅进行分箱
        def categorize_post1k(val):
            if val >= 2.0: return '1. 极强 (涨幅 >= 2%)'
            elif val >= 0.5: return '2. 偏强 (0.5% ~ 2%)'
            elif val >= -0.5: return '3. 震荡 (-0.5% ~ 0.5%)'
            elif val >= -2.0: return '4. 偏弱 (-2% ~ -0.5%)'
            else: return '5. 极弱 (跌幅 <= -2%)'
            
        df_sell['后1K强度分组'] = df_sell['买点后1K涨跌幅(%)'].apply(categorize_post1k)
        res_post1k = df_sell.groupby('后1K强度分组').apply(get_stats).reset_index()
        res_post1k = res_post1k.sort_values(by='后1K强度分组')
        print(res_post1k.to_string(index=False))
        
    print("=" * 100)

if __name__ == "__main__":
    analyze_advanced_performance()