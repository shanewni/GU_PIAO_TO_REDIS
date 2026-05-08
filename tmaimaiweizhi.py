import pandas as pd
import glob
import os
import numpy as np

def analyze_trade_performance():
    # 1. 自动定位最新的回测结果文件
    files = glob.glob("板块回测汇总结果_含总笔数2026-05-08-16-39-48_3k起爆_test.xlsx")  # 替换为你的文件路径模式
    if not files:
        print("错误：未找到回测结果 Excel 文件。")
        return
    
    latest_file = max(files, key=os.path.getctime)
    print(f"正在读取文件: {latest_file}\n")

    try:
        # 读取交易明细页
        df = pd.read_excel(latest_file, sheet_name="所有交易明细")
    except Exception as e:
        print(f"读取失败: {e}")
        return

    # 2. 筛选出卖出记录（包含完整盈亏）
    # 确保你的明细表中有'交易类型'、'单笔盈亏'、'起爆点位置'这几列
    df_sell = df[df['交易类型'].str.contains('卖出', na=False)].copy()
    
    if df_sell.empty:
        print("提示：明细表中没有卖出记录，无法统计收益。")
        return

    # 3. 定义聚合计算函数
    def get_stats(group):
        profits = group['单笔盈亏']
        total_trades = len(profits)
        win_trades = profits[profits > 0]
        loss_trades = profits[profits < 0]
        
        # 胜率
        win_rate = (len(win_trades) / total_trades * 100) if total_trades > 0 else 0
        
        # 盈亏比 (总盈利均值 / 总亏损均值绝对值)
        avg_win = win_trades.mean() if not win_trades.empty else 0
        avg_loss_val = abs(loss_trades.mean()) if not loss_trades.empty else 0
        profit_loss_ratio = (avg_win / avg_loss_val) if avg_loss_val != 0 else 0
        
        # 收益与极值
        total_net = profits.sum()
        avg_profit = profits.mean()
        max_loss = abs(profits.min()) if total_trades > 0 else 0
        max_gain = profits.max() if total_trades > 0 else 0
        
        # 夏普比率 (单笔收益均值 / 收益标准差)
        std_dev = profits.std()
        sharpe = (avg_profit / std_dev) if (std_dev > 0 and not pd.isna(std_dev)) else 0
        
        # 性价比 (平均收益 / 最大亏损风险)
        efficiency = (avg_profit / max_loss) if max_loss > 0 else 0
        
        return pd.Series({
            '交易笔数': int(total_trades),
            '胜率%': round(win_rate, 2),
            '盈亏比': round(profit_loss_ratio, 2),
            '总净利润': round(total_net, 2),
            '平均单笔收益': round(avg_profit, 2),
            '最大亏损': round(max_loss, 2),
            '最大盈利': round(max_gain, 2),
            '夏普': round(sharpe, 4),
            '性价比': round(efficiency, 4)
        })

    # 4. 分组并计算
    result = df_sell.groupby('起爆点位置(30分)').apply(get_stats)

    # 5. 按照性价比降序排列
    result = result.sort_values(by='性价比', ascending=False)

    # 6. 终端美化打印
    print("=" * 135)
    print(f"{'起爆点位置绩效多维统计表 (按性价比降序)':^135}")
    print("-" * 135)
    
    # 强制 Pandas 显示所有列且不换行
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    
    print(result.to_string())
    print("-" * 135)
    print(f"总计分析交易笔数: {len(df_sell)}")
    print("=" * 135)

if __name__ == "__main__":
    analyze_trade_performance()