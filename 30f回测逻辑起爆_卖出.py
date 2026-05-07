import time
import pandas as pd
import os
from datetime import datetime
from pytdx.hq import TdxHq_API
import gupiaojichu
import winsound

from 回测供脚本使用 import TdxStockBacktest 

class SellWarningMonitor:
    def __init__(self, position_file: str):
        self.api = TdxHq_API()
        self.position_file = position_file
        self.strategy = TdxStockBacktest()
        self.sold_stocks = set() 
        # 初始加载持仓
        self.positions = self.load_positions_from_file()

    def load_positions_from_file(self) -> dict:
        """从txt文件加载持仓数据"""
        pos_dict = {}
        if not os.path.exists(self.position_file):
            print(f"错误：找不到文件 {self.position_file}")
            return pos_dict

        try:
            with open(self.position_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue # 跳过空行和注释
                    
                    # 解析：代码, 买入价, 持仓K线数
                    parts = line.split(',')
                    if len(parts) == 3:
                        code = parts[0].strip()
                        buy_price = float(parts[1].strip())
                        hold_k_count = int(parts[2].strip())
                        
                        pos_dict[code] = {
                            'buy_price': buy_price, 
                            'hold_k_count': hold_k_count
                        }
            print(f"成功加载持仓数据：{list(pos_dict.keys())}")
        except Exception as e:
            print(f"加载配置文件出错: {e}")
            
        return pos_dict

    def connect(self, ip="36.153.42.16", port=7709):
        if self.api.connect(ip, port):
            print("行情服务器连接成功，开启卖出监控...")
            return True
        return False

    def check_sell_signals(self):
        # 每次检测前可以考虑重新加载文件，实现实盘动态增减持仓
        # self.positions = self.load_positions_from_file() 

        for code, info in self.positions.items():
            if code in self.sold_stocks:
                continue
                
            market = 1 if code.startswith('6') else 0
            data = self.api.get_security_bars(2, market, code, 0, 250)
            if not data: continue
            
            df = pd.DataFrame(data)
            df['ma60'] = df['close'].rolling(window=60).mean().bfill()
            
            high_full = df['high'].astype(float).tolist()
            low_full = df['low'].astype(float).tolist()
            close_full = df['close'].astype(float).tolist()
            ma60_full = df['ma60'].tolist()
            
            current_idx = len(df) - 1
            min30_frac = gupiaojichu.identify_turns(len(high_full), high_full, low_full)
            virtual_buy_idx = current_idx - info['hold_k_count']
            
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
                print(f"\n[🚨 强制卖出通知] {now_str} | {code} 触发卖出！价格: {close_full[-1]}")
                print(f"   => 原因：{reason}")
                winsound.Beep(800, 1500)
                self.sold_stocks.add(code)

    def run(self):
        if not self.connect(): return
        while True:
            self.positions = self.load_positions_from_file()
            now = datetime.now()
            if (now.hour == 9 and now.minute >= 20) or (10 <= now.hour <= 11) or (13 <= now.hour <= 15):
                try:
                    self.check_sell_signals()
                except Exception as e:
                    print(f"监控异常: {e}")
                time.sleep(15)
            elif now.hour == 15 and now.minute > 5:
                break
            else:
                time.sleep(2)

if __name__ == "__main__":
    # 指定你的txt文件路径
    CONFIG_FILE = "positions.txt" 
    monitor = SellWarningMonitor(CONFIG_FILE)
    monitor.run()