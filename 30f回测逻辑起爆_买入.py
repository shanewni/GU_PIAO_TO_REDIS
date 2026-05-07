import time
import pandas as pd
from datetime import datetime
from pytdx.hq import TdxHq_API
import gupiaojichu
import winsound
import random

# 假设你的策略逻辑封装在 TdxStockBacktest 类中
from 回测供脚本使用 import TdxStockBacktest 

# 通达信行情服务器列表 (常用且相对稳定)
TDX_SERVERS = [
    {'ip': '152.136.167.10', 'port': 7709},
    {'ip': '36.153.42.16', 'port': 7709}
]

class IPManager:
    def __init__(self, servers):
        self.servers = servers
        self.current_index = random.randint(0, len(servers) - 1)

    def get_next_server(self):
        """轮询或随机获取下一个服务器"""
        self.current_index = (self.current_index + 1) % len(self.servers)
        return self.servers[self.current_index]

class GoldenListMonitor:
    def __init__(self, stock_list):
        self.api = TdxHq_API()
        self.ip_manager = IPManager(TDX_SERVERS)
        self.stock_list = stock_list
        self.strategy = TdxStockBacktest()
        # 实时同步你代码中的白名单
        self.GOLDEN_COMBINATIONS = {
            ('二买延续4', '二买'), ('二买延续2', '二买延续3'), ('二买延续2', '二买延续1'),
            ('一买', '三买'), ('一买', '三买延续2'), ('二买延续2', '三买'),
            ('二买延续1', '三买'), ('三买之上1', '一买'),
            ('二买', '三买'), ('三买延续2', '三买延续1'),
            ('一买', '三买之上1'), ('三买', '二买延续2'), 
            ('二买延续2', '一买'), ('二买', '三买之上1'),
            ('二买延续3', '二买')
        }
        self.warned_today = set()

    def reconnect(self):
        """断开当前连接并尝试连接新 IP"""
        try:
            self.api.disconnect()
        except:
            pass
        
        connected = False
        while not connected:
            server = self.ip_manager.get_next_server()
            print(f"🔄 正在尝试切换服务器至: {server['ip']}:{server['port']}...")
            try:
                if self.api.connect(server['ip'], server['port'], time_out=5):
                    print(f"✅ 连接成功！")
                    connected = True
                else:
                    print(f"❌ 连接失败，尝试下一个...")
            except Exception as e:
                print(f"❌ 异常: {e}，正在重试...")
                time.sleep(1)

    def is_30min_closing_time(self) -> bool:
        """判断当前是否正好是30分钟K线的收盘时间点"""
        now = datetime.now()
        time_str = now.strftime("%H:%M")
        closing_times = ['10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30', '15:00']
        return time_str in closing_times
    
    def get_realtime_data(self, market, code):
        """获取数据，如果失败则触发切换 IP 逻辑"""
        max_retries = 2
        for _ in range(max_retries):
            try:
                # 获取日线
                day_data = self.api.get_security_bars(9, market, code, 0, 300)
                # 获取30分钟线
                min30_data = self.api.get_security_bars(2, market, code, 0, 300)

                # 如果返回 None，通常是 API 连接失效
                if day_data is None or min30_data is None:
                    print(f"⚠️  [{code}] 数据返回为空，触发 IP 切换...")
                    self.reconnect()
                    continue

                if len(day_data) < 60 or len(min30_data) < 60:
                    return None, None

                return day_data, min30_data

            except Exception as e:
                print(f"❌ 严重错误: {code} 获取数据异常: {e}，准备更换 IP")
                self.reconnect()
                
        return None, None

    def calculate_three_buy_signals(self,code,high_full, low_full, close_full, open_full):
        """
        遍历完整数据序列，逐段计算三买变体买点信号
        优化：每次计算仅使用最近 200 根 K 线以提升效率
        """
        # 校验全量数据长度一致
        if len(high_full) != len(low_full) or len(high_full) != len(close_full):
            raise ValueError("high_full、low_full、close_full必须长度一致")
        

        # 计算当前窗口内的转折点
        # 注意：window_end 在 identify_turns 中通常作为长度参考
        frac_window = gupiaojichu.identify_turns(len(high_full), high_full, low_full)
        
        # 调用三买变体函数
        try:
            window_signal = self.strategy.three_buy_variant(frac_window, high_full, low_full)
        except Exception:
            print(f"计算 {code} 三买信号失败")
            
        # 如果当前窗口最后一个位置有信号，执行收盘价确认逻辑
        if window_signal[-1] == 1.0:
            current_close = close_full[-1]
            
            # 在当前 frac_window 中找最后一个顶分型（值为1.0）
            # 寻找的是“突破K线”之前最近的一个顶
            last_top_idx = -1
            for i in range(len(frac_window) - 1, -1, -1):
                if frac_window[i] == 1.0:
                    # 排除掉当前K线本身（如果是顶的话）
                    if i < len(frac_window) - 1:
                        last_top_idx = i
                        break
            
            if last_top_idx != -1:
                last_top_high = high_full[last_top_idx]
                # 条件：收盘价必须高于前顶分型最高价，否则撤销信号
                if current_close <= last_top_high:
                    window_signal[-1] = 0.0

                            # 3. 原有的止损价计算逻辑
            
            prev_close = close_full[-2]  # 前一根K线的收盘价
            loss_price = min(low_full[-1], prev_close)
            close_price = close_full[-1]

            if (close_price-loss_price)/loss_price*100 > 3:
                window_signal[-1] = 0.0
            elif (close_price-loss_price)/loss_price*100 < 0.5:
                window_signal[-1] = 0.0
                
            red = close_price <=  open_full[-1]
            if red:
                window_signal[-1] = 0.0
        
        return window_signal
    
    def check_golden_signal(self):
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))} 30分钟信号循环开始...")
        for codes in self.stock_list:
            code = codes[1]
            market = 1 if code.startswith('6') else 0
            day_raw, min30_raw = self.get_realtime_data(market, code)
            
            if not day_raw or not min30_raw: continue

            # 1. 计算日线位置 (buy_pos_day)
            # 这里需要调用你回测中生成 data['day_buy_position'] 的逻辑
            df_day = pd.DataFrame(day_raw)
            day_frac = gupiaojichu.identify_turns(len(df_day), df_day['high'], df_day['low'])
            buy_pos_day = self.strategy.classify_buy_position(day_frac, df_day['high'], df_day['low'])

            # 2. 计算30分钟线信号及位置
            df_30m = pd.DataFrame(min30_raw)
            close_30 = df_30m['close'].astype(float).tolist()
            high_30 = df_30m['high'].astype(float).tolist()
            low_30 = df_30m['low'].astype(float).tolist()
            open_30 = df_30m['open'].astype(float).tolist()

            # 计算30分钟买入信号 (基于你代码中的 calculate_three_buy_signals)
            buy_signals_30 = self.calculate_three_buy_signals(code, high_30, low_30, close_30, open_30)
            # print(f"{code} 30分钟信号")
            # 如果30分钟最新一根K线有基础买入信号
            if buy_signals_30[-1] == 1.0:
                # 计算30分钟当前的结构位置 (buy_pos_30m)
                frac_30 = gupiaojichu.identify_turns(len(high_30), high_30, low_30)
                buy_pos_30m = self.strategy.classify_buy_position(frac_30, high_30, low_30)

                # 3. 【白名单核心过滤】
                combination = (buy_pos_day, buy_pos_30m)
                # print(f"{code} 进入白名单筛选: 日线位置={buy_pos_day}, 30分位置={buy_pos_30m}, 组合={combination}")
                if combination in self.GOLDEN_COMBINATIONS:
                    self.trigger_alert(code, buy_pos_day, buy_pos_30m, close_30[-1])

    def trigger_alert(self, code, pos_day, pos_30, price):
        alert_id = f"{code}_{datetime.now().strftime('%H%M')}"
        if alert_id not in self.warned_today:
            print(f"\n✨ [黄金组合预警] ✨")
            print(f"股票代码: {code} | 当前价: {price}")
            print(f"结构匹配: 日线({pos_day}) + 30分({pos_30})")
            print(f"状态: 满足白名单，建议关注！")
            winsound.PlaySound("SystemExit", winsound.SND_ALIAS) 
            winsound.Beep(800, 600) # 警报音
            self.warned_today.add(alert_id)

    def run(self):
        self.reconnect()
        while True:
            # 限制在交易时间
            now = datetime.now().time()
            # 交易时间判断
            is_trade_time = (datetime.strptime("09:15", "%H:%M").time() <= now <= datetime.strptime("11:35", "%H:%M").time()) or \
                            (datetime.strptime("13:00", "%H:%M").time() <= now <= datetime.strptime("15:05", "%H:%M").time())
            
            if is_trade_time:
                self.check_golden_signal()
                time.sleep(5) 
            else:
                # 非交易时间，休眠 60 秒
                time.sleep(60)

def load_stock_list(blk_file_path):
    """
    从blk文件加载股票列表
    
    Returns:
        list: 股票代码列表，格式为 [('市场代码', '股票代码', '完整代码'), ...]
    """
    stock_list = []
    
    try:
        with open(blk_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        # 跳过第一行空白，从第二行开始处理
        for line in lines[1:]:
            line = line.strip()
            if line:
                # 第一位是市场代码，后六位是股票代码
                market_code = line[0]
                stock_code = line[1:7]
                
                # 将市场代码转换为pytdx需要的格式
                # 0: 深圳, 1: 上海
                if market_code == '0':
                    market = 0  # 深圳
                    market_prefix = 'sz'
                else:
                    market = 1  # 上海
                    market_prefix = 'sh'
                
                full_code = f"{market_prefix}{stock_code}"
                stock_list.append((market, stock_code, full_code))
                
                # print(f"加载股票: {full_code}")
                
    except Exception as e:
        print(f"读取blk文件失败: {e}")
        
    return stock_list

if __name__ == "__main__":
    # 从你的 blk 文件解析自选股列表
    BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\60RJXS.blk"
    # BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\ZB.blk"
    stock_list = load_stock_list(BLOB_FILE_PATH)
    # stock_list = ['000001', '600000'] 
    monitor = GoldenListMonitor(stock_list)
    monitor.run()