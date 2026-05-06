import time
import pandas as pd
from datetime import datetime
from pytdx.hq import TdxHq_API
import gupiaojichu
import winsound

from 回测供脚本使用 import TdxStockBacktest 

class SellWarningMonitor:
    def __init__(self, position_data: dict):
        """
        position_data 格式: 
        {
            '600000': {'buy_price': 10.50, 'hold_k_count': 15},
            '000001': {'buy_price': 15.20, 'hold_k_count': 32}
        }
        """
        self.api = TdxHq_API()
        self.positions = position_data
        self.strategy = TdxStockBacktest()
        self.sold_stocks = set() # 记录已发过卖出通知的股票

    def connect(self, ip="36.153.42.16", port=7709):
        if self.api.connect(ip, port):
            print("行情服务器连接成功，开启卖出监控...")
            return True
        return False

    def check_sell_signals(self):
        for code, info in self.positions.items():
            if code in self.sold_stocks:
                continue
                
            market = 1 if code.startswith('6') else 0
            # 获取数据，确保包含足够计算 MA60 的数量
            data = self.api.get_security_bars(2, market, code, 0, 250)
            if not data: continue
            
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # 计算 MA60[cite: 3]
            df['ma60'] = df['close'].rolling(window=60).mean().bfill()
            
            high_full = df['high'].astype(float).tolist()
            low_full = df['low'].astype(float).tolist()
            close_full = df['close'].astype(float).tolist()
            ma60_full = df['ma60'].tolist()
            
            current_idx = len(df) - 1
            
            # 计算顶底分型 (包含最新未完成的K线)
            min30_frac = gupiaojichu.identify_turns(len(high_full), high_full, low_full)
            
            # 组装虚拟买入索引 (当前K线索引 - 已持仓的K线数量)
            virtual_buy_idx = current_idx - info['hold_k_count']
            
            # 判定卖出条件[cite: 3]
            is_sell, reason = self.strategy.check_dynamic_sell_condition(
                current_idx=current_idx,
                high_full=high_full,
                low_full=low_full,
                close_full=close_full,
                ma60_full=ma60_full,
                buy_price=info['buy_price'],
                buy_idx=virtual_buy_idx,
                min30_frac=min30_frac
            )
            
            if is_sell:
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                current_price = close_full[-1]
                print(f"\n[🚨 强制卖出通知] {now_str} | {code} 触发卖出！当前价: {current_price}")
                print(f"   => 卖出原因：{reason}")
                winsound.Beep(800, 1500) # 警报音
                self.sold_stocks.add(code) # 通知后不再重复报警

    def run(self):
        if not self.connect():
            return
            
        while True:
            now = datetime.now()
            # 盘中监控
            if (now.hour == 9 and now.minute >= 30) or (10 <= now.hour <= 11) or (13 <= now.hour < 15):
                try:
                    self.check_sell_signals()
                except Exception as e:
                    print(f"卖出监控异常: {e}")
                time.sleep(10) # 卖出监控频率可略高
            elif now.hour == 15 and now.minute > 5:
                print("今日交易结束，退出卖出监控。")
                self.api.disconnect()
                break
            else:
                time.sleep(60)

if __name__ == "__main__":
    # 根据你当前的真实持仓手动输入：股票代码、买入价、目前已经持有的30分钟K线数量
    my_positions = {
        '605198': {'buy_price': 46.67, 'hold_k_count': 12}
    }
    monitor = SellWarningMonitor(my_positions)
    monitor.run()