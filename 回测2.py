import time
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pytdx.hq import TdxHq_API
from typing import Callable, Dict, List, Tuple
import struct
from mootdx.reader import Reader
from collections import defaultdict, deque
import os
import glob

# 忽略无关警告
warnings.filterwarnings('ignore')

# 通达信板块文件路径
BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\SSYNYS.blk"
# BLOB_FILE_PATH = r"D:\zd_hbzq\T0002\blocknew\TEST.blk"
# 本地通达信数据默认路径
DEFAULT_TDX_PATH = r"D:\zd_hbzq"

class TdxStockBacktest:
    """基于pytdx的股票回测框架（纯日线RPS策略+以损定量）"""
    
    def __init__(self):
        self.api = TdxHq_API()
        self.stock_data = {}  # {'day': 日线数据}
        self.backtest_result = None  # 存储回测结果
        self.trade_records = None  # 交易记录
        self.stop_loss_price = 0.0  # 止损价格
        self.in_position = False    # 是否持仓标记
        self.stop_loss_ratio = 0.02 # 总资金止损百分比（默认2%）
        self.risk_per_trade = 0.0   # 单笔交易可承受的最大亏损金额
        self.all_trades_detail = [] # 存储单股票所有交易明细（用于总笔数汇总）
    
    def connect_tdx(self, ip: str = "152.136.167.10", port: int = 7709) -> bool:
        """连接通达信服务器"""
        try:
            self.api.connect(ip, port)
            print(f"成功连接通达信服务器 {ip}:{port}")
            return True
        except Exception as e:
            print(f"连接失败: {e}")
            return False
        
    def read_tdx_export_txt(self, file_path):
        """读取通达信导出的txt文件(日线)"""
        try:
            df = pd.read_csv(file_path, sep=r'\s+', encoding='gbk', skiprows=1, skipfooter=1, engine='python')
            df['datetime'] = pd.to_datetime(df['日期'].astype(str))
            
            rename_dict = {
                '开盘': '开盘价', '最高': '最高价', '最低': '最低价',
                '收盘': '收盘价', '成交量': '成交量', '成交额': '成交额'
            }
            df = df.rename(columns=rename_dict)
            cols = ['datetime', '开盘价', '最高价', '最低价', '收盘价', '成交量', '成交额']
            df = df[cols]
            
            df.set_index('datetime', inplace=True)
            df.sort_index(inplace=True)
            return df
        except Exception as e:
            print(f"读取 TXT失败: {e}")
            return pd.DataFrame()
            
    def find_txt_file(self,data_dir: str, code: str, period_hint: str) -> str:
        """在指定目录下查找 txt 文件"""
        pattern = os.path.join(data_dir, f"*{code}*.txt")
        files = glob.glob(pattern)
        if not files:
            return None
        if len(files) == 1:
            return files[0]
        for f in files:
            if period_hint in os.path.basename(f):
                return f
        return files[0]
    
    def get_txt_day_data(self, code: str, txt_day_dir: str, start_date=None, end_date=None) -> pd.DataFrame:
        """从 txt 文件读取日线数据"""
        file_path = self.find_txt_file(txt_day_dir, code, "日线")
        if file_path is None:
            print(f"未找到 {code} 的日线 txt 文件")
            return pd.DataFrame()
        df = self.read_tdx_export_txt(file_path)
        if df.empty:
            return df
        if start_date:
            df = df[df.index >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df.index <= pd.to_datetime(end_date)]
        print(f"成功读取 {code} txt日线({start_date or '起点'}至{end_date or '至今'})，共 {len(df)} 条")
        return df

    def get_local_day_data(self, symbol, tdx_path=DEFAULT_TDX_PATH, start_date=None, end_date=None):
        """从本地通达信数据读取日线"""
        reader = Reader.factory(market='std', tdxdir=tdx_path)
        df_day = reader.daily(symbol=symbol)
        
        if df_day is None or df_day.empty:
            print(f"未获取到{symbol}本地日线数据")
            return pd.DataFrame()
        
        df_day = df_day.rename(columns={
            'open': '开盘价', 'high': '最高价', 'low': '最低价',
            'close': '收盘价', 'volume': '成交量', 'amount': '成交额'
        })
        
        df_day.index = pd.to_datetime(df_day.index)
            
        if start_date:
            df_day = df_day[df_day.index >= pd.to_datetime(start_date)]
        if end_date:
            df_day = df_day[df_day.index <= pd.to_datetime(end_date)]
        
        price_cols = ['开盘价', '最高价', '最低价', '收盘价']
        df_day[price_cols] = df_day[price_cols].round(2)
        
        print(f"成功读取 {symbol} 日线({start_date or '起点'}至{end_date or '至今'})，共 {len(df_day)} 条")
        return df_day
        
    def get_stock_k_data(self, code: str, start: int = 0, count: int = 1000, ktype: int = 9) -> pd.DataFrame:
        """获取指定周期的股票K线数据 (联网)"""
        market = 1 if code.startswith('6') else 0
        try:
            data = self.api.get_security_bars(ktype, market, code, start, count)
            df = pd.DataFrame(data)
            if df.empty:
                return pd.DataFrame()
            
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.rename(columns={
                'open': '开盘价', 'high': '最高价', 'low': '最低价', 
                'close': '收盘价', 'vol': '成交量', 'amount': '成交额'
            })
            df = df.set_index('datetime')
            result_df = df[['开盘价', '最高价', '最低价', '收盘价', '成交量', '成交额']]
            
            self.stock_data['day'] = result_df
            print(f"成功获取 {code} 联网数据，共 {len(result_df)} 条")
            return result_df
        except Exception as e:
            print(f"获取联网数据失败: {e}")
            return pd.DataFrame()
    
    def get_multi_period_data(self, code: str, count: int = 800,
                          use_local: bool = False, tdx_path: str = DEFAULT_TDX_PATH,
                          use_txt_files: bool = False,
                          txt_day_dir: str = r"D:\zd_hbzq\daochushujuday",
                          start_date=None, end_date=None) -> Dict[str, pd.DataFrame]:
        """获取日线数据"""
        if use_txt_files:
            day_data = self.get_txt_day_data(code, txt_day_dir, start_date, end_date)
        elif use_local:
            day_data = self.get_local_day_data(code, tdx_path, start_date, end_date)
        else:
            day_data = self.get_stock_k_data(code, count=count, ktype=9)

        self.stock_data['day'] = day_data
        return {'day': day_data}
    
    @staticmethod
    def calculate_rps_matrix(all_stocks_data: Dict[str, pd.DataFrame], n: int = 250) -> pd.DataFrame:
        """计算整个池子的 RPS 矩阵"""
        extrs_dict = {}
        for code, df in all_stocks_data.items():
            if df is None or df.empty or '收盘价' not in df.columns:
                continue
            extrs_dict[code] = df['收盘价'].pct_change(n)

        if not extrs_dict:
            return pd.DataFrame()
        
        extrs_df = pd.DataFrame(extrs_dict)
        rps_df = extrs_df.rank(axis=1, pct=True, ascending=True) * 100
        return rps_df 
    
    def calculate_position_size(self, current_cash: float, entry_price: float, stop_loss_price: float) -> int:
        """以损定量核心计算：根据止损百分比计算可买数量"""
        if entry_price <= stop_loss_price:
            return 0  
        
        self.risk_per_trade = current_cash * self.stop_loss_ratio
        loss_per_share = entry_price - stop_loss_price
        max_shares = self.risk_per_trade / loss_per_share
        position_size = int(max_shares // 100 * 100) 
        max_affordable = current_cash / (entry_price * 1.001)  
        position_size = min(position_size, int(max_affordable // 100 * 100))
        return max(position_size, 0)
    
    @staticmethod
    def calc_max_drawdown(asset_values: np.ndarray) -> float:
        """计算最大回撤"""
        if len(asset_values) == 0:
            return 0.0
        running_max = np.maximum.accumulate(asset_values)
        drawdown = (asset_values - running_max) / running_max
        max_dd = np.min(drawdown) * 100
        return max_dd
    
    def calc_backtest_metrics(self, init_cash: float) -> Dict:
        """计算回测核心指标"""
        if self.backtest_result is None or len(getattr(self, 'trade_pnl', [])) == 0:
            return {}
        
        total_profit = self.backtest_result['总资产'].iloc[-1] - init_cash
        total_return = total_profit / init_cash * 100
        
        time_delta = (self.backtest_result.index[-1] - self.backtest_result.index[0])
        days = time_delta.days
        annual_return = (pow((self.backtest_result['总资产'].iloc[-1] / init_cash), 365/days) - 1) * 100 if days !=0 else 0
        
        max_drawdown = self.calc_max_drawdown(self.backtest_result['总资产'].values)
        
        total_trades = len(self.trade_pnl)
        win_trades = sum(1 for pnl in self.trade_pnl if pnl > 0)
        win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
        
        profits = [pnl for pnl in self.trade_pnl if pnl > 0]
        losses = [abs(pnl) for pnl in self.trade_pnl if pnl < 0]
        avg_profit = np.mean(profits) if profits else 0.0
        avg_loss = np.mean(losses) if losses else 0.0
        profit_loss_ratio = avg_profit / avg_loss if avg_loss != 0 else 0.0
        
        returns = self.backtest_result['总资产'].pct_change().dropna()
        sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std() if returns.std() != 0 else 0.0
        
        metrics = {
            '初始资金': init_cash,
            '最终总资产': self.backtest_result['总资产'].iloc[-1],
            '累计收益': total_profit,
            '累计收益率(%)': total_return,
            '年化收益率(%)': annual_return,
            '最大回撤(%)': max_drawdown,
            '总交易次数': total_trades,
            '盈利交易次数': win_trades,
            '胜率(%)': win_rate,
            '平均盈利': avg_profit,
            '平均亏损': avg_loss,
            '盈亏比': profit_loss_ratio,
            '夏普比率': sharpe_ratio
        }
        return metrics
    
    def daily_rps_strategy(self, day_df: pd.DataFrame, rps_signals: pd.Series = None) -> pd.DataFrame:
        """核心策略：日线RPS共振买入 + 连续跌破MA5卖出"""
        data = day_df.copy()
        data.index.name = 'datetime'
        
        # 1. 计算MA5
        data['ma5'] = data['收盘价'].rolling(window=5).mean()
        
        # 2. 对接RPS买点信号 (在 batch_backtest 中已计算好是否符合“第一次同时>=90”条件)
        if rps_signals is not None:
            data['date_only'] = data.index.date
            rps_df = rps_signals.to_frame(name='rps_buy_flag')
            rps_df['date_only'] = rps_df.index.date
            data = data.reset_index().merge(
                rps_df[['date_only', 'rps_buy_flag']], 
                on='date_only', 
                how='left'
            ).set_index('datetime')
            data['rps_buy_flag'] = data['rps_buy_flag'].fillna(0)
        else:
            data['rps_buy_flag'] = 0

        # --- 3. 核心状态机循环 ---
        data['signal'] = 0
        data['sell_reason'] = ""
        data['active_stop_loss'] = 0.0
        data['buy_position'] = "RPS共振" 
        
        in_pos = False
        current_stop_loss = 0.0 
        
        close_list = data['收盘价'].tolist()
        low_list = data['最低价'].tolist()
        high_list = data['最高价'].tolist()
        open_list = data['开盘价'].tolist()
        ma5_list = data['ma5'].tolist()
        rps_flag_list = data['rps_buy_flag'].tolist()
        
        for i in range(len(data)):
            current_idx_time = data.index[i]
            
            if not in_pos:
                # --- 买入逻辑优化 ---
                # 1. 连续两次RPS >= 90
                cond_rps = rps_flag_list[i] and rps_flag_list[i-1]
                # 2. 连续两个阳线
                cond_yang = (close_list[i] > open_list[i]) and (close_list[i-1] > open_list[i-1])
                # 3. 第二根收盘 > 第一根收盘 且 第二根最高 > 第一根最高
                cond_break = (close_list[i] > close_list[i-1]) and (high_list[i] > high_list[i-1])
                # 4. 涨停不买 (涨幅控制在 9.9% 以内)
                cond_not_limit = (close_list[i] / close_list[i-1] < 1.098)
                
                if cond_rps and cond_yang and cond_break and cond_not_limit:
                    data.loc[current_idx_time, 'signal'] = 1
                    in_pos = True
                    
                    # 设置止损价：当前K线的最低点或前K的最高点取最低值
                    curr_low = low_list[i]
                    prev_high = high_list[i-1] if i > 0 else curr_low
                    current_stop_loss = min(curr_low, prev_high)
                    data.loc[current_idx_time, 'active_stop_loss'] = current_stop_loss
            else:       
                data.loc[current_idx_time, 'active_stop_loss'] = current_stop_loss 
                
                # A. 止损检查 (优先级最高)
                if low_list[i] <= current_stop_loss:
                    data.loc[current_idx_time, 'signal'] = -1
                    data.loc[current_idx_time, 'sell_reason'] = f"触发止损(当前止损价:{current_stop_loss})"
                    in_pos = False
                    continue

                # B. 主动卖出：连续两根日k收盘低于5日均线
                if i > 0 and not np.isnan(ma5_list[i]) and not np.isnan(ma5_list[i-1]):
                    if close_list[i] < ma5_list[i] and close_list[i-1] < ma5_list[i-1]:
                        data.loc[current_idx_time, 'signal'] = -1
                        data.loc[current_idx_time, 'sell_reason'] = "连续两日收盘跌破MA5"
                        in_pos = False

        return data
    
    def run_backtest(self, code: str, init_cash: float = 100000.0,
                 commission: float = 0.0003, stop_loss_ratio: float = 0.01,
                 use_local: bool = False, tdx_path: str = DEFAULT_TDX_PATH,
                 use_txt_files: bool = False,
                 txt_day_dir: str = r"D:\zd_hbzq\daochushujuday",
                 start_date=None, end_date=None, current_rps: pd.Series = None) -> Tuple[pd.DataFrame, Dict, List[Dict]]:

        print(f"\n========== 开始回测股票 {code} ==========")
        print(f"数据源模式：{'本地通达信数据' if use_local else '外部导出/联网获取'}")
   
        self.all_trades_detail = []
        
        if not use_local and not use_txt_files:
            connect_success = self.connect_tdx()
            if not connect_success:
                print("联网模式连接失败，终止回测")
                return pd.DataFrame(), {}, []
        
        # 获取纯日线数据
        multi_data = self.get_multi_period_data(
            code, count=800,
            use_local=use_local, tdx_path=tdx_path,
            use_txt_files=use_txt_files, txt_day_dir=txt_day_dir,
            start_date=start_date, end_date=end_date
        )
        day_data = multi_data['day']
        
        if day_data.empty:
            print(f"股票 {code} 日线数据为空，无法回测")
            return pd.DataFrame(), {}, []
        
        # 运行每日RPS策略生成信号
        data = self.daily_rps_strategy(day_data, current_rps)
        
        self.stop_loss_ratio = stop_loss_ratio
        print(f"\n以损定量配置：单笔交易最大亏损 = 总资金 × {stop_loss_ratio*100}%")
        
        cash = init_cash
        position = 0 
        total_asset = init_cash 
        trade_records = []
        daily_results = []
        self.trade_pnl = []     
        self.stop_loss_price = 0.0
        self.in_position = False
        buy_price = 0
        buy_fee = 0
        
        for datetime, row in data.iterrows():
            close_price = row['收盘价']
            current_total_asset = cash + position * close_price
            current_idx = data.index.get_loc(datetime)

            if row['signal'] == 1 or row['signal'] == -1:
                self.stop_loss_price = row['active_stop_loss']
            
            # ===== 买入执行 =====
            if row['signal'] == 1 and cash > close_price and not self.in_position:   
                buy_num = self.calculate_position_size(
                    current_cash=current_total_asset,
                    entry_price=close_price,
                    stop_loss_price=self.stop_loss_price
                )
                
                if buy_num > 0:
                    self.buy_in_total_asset = cash + position * close_price
                    cost = buy_num * close_price * (1 + commission)
                    fee = buy_num * close_price * commission
                    if cash >= cost:
                        position += buy_num
                        cash -= cost
                        buy_price = close_price
                        buy_fee = fee
                        self.in_position = True
                        self.buy_kline_index = current_idx  
                        self.current_buy_position = row.get('buy_position', '未知')
                        
                        trade_detail = {
                            '股票代码': code,
                            '交易时间': datetime,
                            '交易类型': '买入',
                            '价格': close_price,
                            '数量': buy_num,
                            '费用': fee,
                            '单笔盈亏': 0.0,
                            '是否盈利': None,
                            '设置止损价': self.stop_loss_price,
                            '单笔风险金额': self.risk_per_trade,
                            '风险比例': self.stop_loss_ratio*100,
                            '单k涨幅': (close_price - self.stop_loss_price)/self.stop_loss_price*100 if self.stop_loss_price!=0 else 0
                        }
                        trade_records.append(trade_detail)
                        self.all_trades_detail.append(trade_detail)
                        
                        print(f"【买入开仓（以损定量）】{datetime} - 价格{close_price}，数量{buy_num}")
                        print(f"          - 止损价:{self.stop_loss_price} | 单笔风险金额:{self.risk_per_trade:.2f} | 风险比例:{self.stop_loss_ratio*100}%")
                else:
                    print(f"【买入失败】{datetime} - 以损定量计算可买数量为0（止损价{self.stop_loss_price} >= 买入价{close_price}）")
            
            # ===== 卖出执行 =====
            elif row['signal'] == -1 and position > 0:
                current_kline_idx = data.index.get_loc(datetime) + 1
                sell_reason = row['sell_reason'] if 'sell_reason' in row else "未知原因"
                sell_num = position
                fee = sell_num * close_price * commission
                income = sell_num * close_price * (1 - commission)
                total_buy_cost = sell_num * buy_price + buy_fee
                total_sell_income = income - fee
                pnl = total_sell_income - total_buy_cost
                hold_k_count = current_idx - self.buy_kline_index + 1  
                cash += income
                
                trade_detail = {
                    '股票代码': code,
                    '交易时间': datetime,
                    '起爆点位置': getattr(self, 'current_buy_position', '未知'), 
                    '交易类型': '策略卖出',
                    '价格': close_price,
                    '数量': sell_num,
                    '费用': fee,
                    '单笔盈亏': pnl,
                    '是否盈利': pnl > 0,
                    '卖出原因': sell_reason,
                    '当前K线索引': current_kline_idx,
                    '持仓K线数量': hold_k_count,  
                    '单笔风险金额': self.risk_per_trade,
                    '实际盈亏比例': pnl/self.buy_in_total_asset*100
                }
                trade_records.append(trade_detail)
                self.trade_pnl.append(pnl)
                self.all_trades_detail.append(trade_detail)
                
                position = 0
                buy_price = 0
                buy_fee = 0
                self.stop_loss_price = 0.0
                self.in_position = False
                self.risk_per_trade = 0.0
                
                print(f"【策略卖出】{datetime} - 第{current_kline_idx}根日K线 | 价格{close_price}，数量{sell_num}，盈亏{pnl:.2f}")
                print(f"          - 卖出原因：{sell_reason}")
                print(f"          - 单笔风险金额:{self.risk_per_trade:.2f} | 实际盈亏比例:{pnl/self.buy_in_total_asset*100:.2f}%")
            
            total_asset = cash + position * close_price
            daily_results.append({
                '时间': datetime,
                '收盘价': close_price,
                '持仓数量': position,
                '可用现金': cash,
                '总资产': total_asset,
                '累计收益': total_asset - init_cash,
                '累计收益率': (total_asset - init_cash) / init_cash * 100,
                '止损价格': self.stop_loss_price if self.in_position else 0.0,
                '单笔风险金额': self.risk_per_trade if self.in_position else 0.0
            })
        
        self.backtest_result = pd.DataFrame(daily_results)
        self.backtest_result = self.backtest_result.set_index('时间')
        self.trade_records = pd.DataFrame(trade_records)
        metrics = self.calc_backtest_metrics(init_cash)
        
        print(f"\n===== 股票 {code} 回测核心指标 =====")
        for k, v in metrics.items():
            print(f"{k}: {v:.2f}")
        print(f"\n========== 股票 {code} 回测完成 ==========\n")
        
        return self.backtest_result, metrics, self.all_trades_detail


def parse_tdx_blk_file(file_path: str) -> List[str]:
    stock_list = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines[1:]:
            line = line.strip()
            if line:
                stock_code = line[1:7]          
                stock_list.append(stock_code)            
    except Exception as e:
        print(f"读取blk文件失败: {e}")
    return stock_list

def calculate_total_trades_metrics(all_trades_detail: List[Dict]) -> Dict:
    if not all_trades_detail:
        return {}
    
    pnl_trades = [t for t in all_trades_detail if t.get('单笔盈亏', 0) != 0.0 and '持仓K线数量' in t]
    if not pnl_trades:
        return {
            '总交易笔数（含买入）': len(all_trades_detail), '有效交易笔数（有盈亏）': 0,
            '盈利笔数': 0, '亏损笔数': 0, '总胜率(%)': 0.0, '总盈利金额': 0.0,
            '总亏损金额': 0.0, '总净收益': 0.0, '平均每笔盈利': 0.0, '平均每笔亏损': 0.0,
            '总盈亏比': 0.0, '最大单笔盈利': 0.0, '最大单笔亏损': 0.0
        }

    hold_periods = [t['持仓K线数量'] for t in pnl_trades]
    win_hold_periods = [t['持仓K线数量'] for t in pnl_trades if t['是否盈利']]
    loss_hold_periods = [t['持仓K线数量'] for t in pnl_trades if not t['是否盈利']]

    total_trades_all = len(all_trades_detail)          
    total_pnl_trades = len(pnl_trades)                 
    win_trades = [t for t in pnl_trades if t['是否盈利']] 
    loss_trades = [t for t in pnl_trades if not t['是否盈利']] 
    total_win_count = len(win_trades)
    total_loss_count = len(loss_trades)
    
    avg_hold_k = np.mean(hold_periods) if hold_periods else 0.0
    avg_win_hold_k = np.mean(win_hold_periods) if win_hold_periods else 0.0
    avg_loss_hold_k = np.mean(loss_hold_periods) if loss_hold_periods else 0.0
    max_hold_k = np.max(hold_periods) if hold_periods else 0.0
    
    total_profit = sum(t['单笔盈亏'] for t in win_trades)
    total_loss = abs(sum(t['单笔盈亏'] for t in loss_trades))
    total_net_profit = total_profit - total_loss
    avg_win_per_trade = total_profit / total_win_count if total_win_count > 0 else 0.0
    avg_loss_per_trade = total_loss / total_loss_count if total_loss_count > 0 else 0.0
    total_profit_loss_ratio = avg_win_per_trade / avg_loss_per_trade if avg_loss_per_trade > 0 else 0.0
    
    max_single_win = max([t['单笔盈亏'] for t in win_trades], default=0.0)
    max_single_loss = min([t['单笔盈亏'] for t in loss_trades], default=0.0)
    total_win_rate = (total_win_count / total_pnl_trades) * 100 if total_pnl_trades > 0 else 0.0
    
    return {
        '总交易笔数（含买入）': total_trades_all, '有效交易笔数（卖出/止损）': total_pnl_trades,
        '盈利笔数': total_win_count, '亏损笔数': total_loss_count, '总胜率(%)': total_win_rate,
        '总盈利金额': total_profit, '总亏损金额': total_loss, '总净收益': total_net_profit,
        '平均每笔盈利': avg_win_per_trade, '平均每笔亏损': avg_loss_per_trade,
        '总盈亏比': total_profit_loss_ratio, '最大单笔盈利': max_single_win, '最大单笔亏损': max_single_loss,
        '平均持仓K线数量': round(avg_hold_k, 2), '盈利平均K线数量': round(avg_win_hold_k, 2),
        '亏损平均K线数量': round(avg_loss_hold_k, 2), '最高持仓K线数量': int(max_hold_k)
    }

def batch_backtest(stock_codes: List[str], init_cash: float = 100000.0, 
                   commission: float = 0.0003, stop_loss_ratio: float = 0.01) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    backtest = TdxStockBacktest()
    connect_success = backtest.connect_tdx()
    if not connect_success:
        print("通达信服务器连接失败，无法执行批量回测")
        return pd.DataFrame(), pd.DataFrame(), {}
    
    all_metrics = []
    all_trades_detail = []  

    all_day_data = {}
    print("正在预取日线数据以计算 RPS 强度...")
    for code in stock_codes:
        df = backtest.get_local_day_data(code)
        if not df.empty:
            all_day_data[code] = df
            
    # 步骤 B: 计算多周期 RPS 强度 (替换为 13, 8, 5)
    print("正在计算 RPS13, RPS8, RPS5 强度矩阵...")
    rps5_matrix = TdxStockBacktest.calculate_rps_matrix(all_day_data, n=5)
    rps8_matrix = TdxStockBacktest.calculate_rps_matrix(all_day_data, n=8)
    rps13_matrix = TdxStockBacktest.calculate_rps_matrix(all_day_data, n=13)

    for idx, code in enumerate(stock_codes):
        print(f"\n------------------- 进度 {idx+1}/{len(stock_codes)} -------------------")
        try:
            s5 = rps5_matrix[code] if code in rps5_matrix else pd.Series(0, index=rps5_matrix.index)
            s8 = rps8_matrix[code] if code in rps8_matrix else pd.Series(0, index=rps8_matrix.index)
            s13 = rps13_matrix[code] if code in rps13_matrix else pd.Series(0, index=rps13_matrix.index)
            
            # --- 核心逻辑修改：判断三个条件同时 >= 90，并且是“第一次”达成 ---
            current_cond = (s5 >= 90) & (s8 >= 90) & (s13 >= 90)
            # 前K未达到当前条件
            prev_cond = current_cond.shift(1).fillna(False)
            
            # 只有在当前达标，且前一天未达标时，才置为1(买入信号)
            combined_rps_filter = (current_cond & (~prev_cond)).astype(int)

            _, metrics, trades_detail = backtest.run_backtest(
                code=code,
                init_cash=init_cash,
                commission=commission,
                stop_loss_ratio=stop_loss_ratio,
                use_local=False,           
                use_txt_files=True,        
                txt_day_dir=r"D:\zd_hbzq\daochushujuday",
                start_date='2025-11-01',
                end_date='2026-05-28',
                current_rps=combined_rps_filter
            )
            if metrics:  
                metrics['股票代码'] = code
                all_metrics.append(metrics)
            if trades_detail:  
                all_trades_detail.extend(trades_detail)
            time.sleep(0.001)
        except Exception as e:
            print(f"股票 {code} 回测异常: {e}")
            continue
    
    if not all_metrics:
        print("无有效回测结果")
        return pd.DataFrame(), pd.DataFrame(), {}
    
    stock_summary_df = pd.DataFrame(all_metrics)
    cols = ['股票代码'] + [col for col in stock_summary_df.columns if col != '股票代码']
    stock_summary_df = stock_summary_df[cols]
    
    print("\n" + "="*80)
    print("                          板块回测（单股票维度）汇总")
    print("="*80)
    total_stocks = len(stock_summary_df)
    profitable_stocks = len(stock_summary_df[stock_summary_df['累计收益'] > 0])
    avg_return = stock_summary_df['累计收益率(%)'].mean()
    avg_max_dd = stock_summary_df['最大回撤(%)'].mean()
    avg_win_rate = stock_summary_df['胜率(%)'].mean()
    avg_profit_loss_ratio = stock_summary_df['盈亏比'].mean()
    total_profit_all = stock_summary_df['累计收益'].sum()
    total_return_all = (total_profit_all / (init_cash * total_stocks)) * 100
    
    print(f"1. 参与回测股票总数：{total_stocks} 只")
    print(f"2. 盈利股票数量：{profitable_stocks} 只 | 盈利股票占比：{profitable_stocks/total_stocks*100:.2f}%")
    print(f"3. 单股票平均累计收益率：{avg_return:.2f}%")
    print(f"4. 单股票平均最大回撤：{avg_max_dd:.2f}%")
    print(f"5. 单股票平均胜率：{avg_win_rate:.2f}%")
    print(f"6. 单股票平均盈亏比：{avg_profit_loss_ratio:.2f}")
    print(f"7. 板块总累计收益：{total_profit_all:.2f} 元 (等额资金分配下)")
    print(f"8. 板块总累计收益率：{total_return_all:.2f}% (等额资金分配下)")
    print("="*80)
    
    total_trades_metrics = calculate_total_trades_metrics(all_trades_detail)
    print("\n" + "="*80)
    print("                          板块回测（总交易笔数维度）汇总")
    print("="*80)
    for k, v in total_trades_metrics.items():
        print(f"{k}: {v:.2f}")
    print("="*80)
    
    trades_detail_df = pd.DataFrame(all_trades_detail)
    return stock_summary_df, trades_detail_df, total_trades_metrics

def calculate_pure_compounding(all_trades_detail: List[Dict], init_cash: float = 100000.0) -> Dict:
    if not all_trades_detail:
        return {}
    closed_trades = [t for t in all_trades_detail if t.get('单笔盈亏', 0) != 0.0]
    if not closed_trades:
        return {}

    closed_trades.sort(key=lambda x: pd.to_datetime(x['交易时间']))
    current_capital = init_cash
    capital_curve = [current_capital]  
    
    for trade in closed_trades:
        trade_return_ratio = trade['实际盈亏比例'] / 100.0
        current_capital = current_capital * (1 + trade_return_ratio)
        capital_curve.append(current_capital)

    capital_array = np.array(capital_curve)
    running_max = np.maximum.accumulate(capital_array)
    drawdowns = (capital_array - running_max) / running_max
    max_dd = np.min(drawdowns) * 100

    total_return_pct = (current_capital - init_cash) / init_cash * 100

    return {
        "初始总资金": init_cash,
        "最终总资金": current_capital,
        "参与复利交易笔数": len(closed_trades),
        "理论复利总收益率(%)": total_return_pct,
        "复利资金曲线最大回撤(%)": max_dd
    }

def analyze_loss_periods(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame()
    
    df = trades_df.copy()
    df['日期'] = pd.to_datetime(df['交易时间']).dt.date
    
    daily_stats = df[df['单笔盈亏'] != 0].groupby('日期')['单笔盈亏'].agg(['count', 'sum']).reset_index()
    daily_stats.columns = ['日期', '成交笔数', '当日净损益']

    profit_days = daily_stats[daily_stats['当日净损益'] > 0].copy()
    profit_days = profit_days.sort_values(by='当日净损益', ascending=False).reset_index(drop=True)
    profit_days.columns = ['盈利日期', '盈利单数', '盈利金额']

    loss_days = daily_stats[daily_stats['当日净损益'] < 0].copy()
    loss_days = loss_days.sort_values(by='当日净损益', ascending=True).reset_index(drop=True)
    loss_days.columns = ['亏损日期', '亏损单数', '亏损金额']

    combined_df = pd.concat([profit_days, loss_days], axis=1)

    print(f"\n统计完成：盈利天数 {len(profit_days)}，亏损天数 {len(loss_days)}")
    return combined_df

def analyze_by_buy_date(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame()

    trades_df = trades_df.sort_values('交易时间').reset_index(drop=True)
    buy_queue = defaultdict(deque)  
    buy_date_summary = {}  

    for _, row in trades_df.iterrows():
        code = row['股票代码']
        ttype = row['交易类型']

        if ttype == '买入':
            buy_queue[code].append((row['交易时间'], row['数量']))
        elif '卖出' in ttype:
            if code in buy_queue and buy_queue[code]:
                buy_time, _ = buy_queue[code].popleft()
                pnl = row['单笔盈亏']
                buy_date = pd.to_datetime(buy_time).date()

                if buy_date not in buy_date_summary:
                    buy_date_summary[buy_date] = {'盈利金额': 0.0, '盈利单数': 0, '亏损金额': 0.0, '亏损单数': 0}

                if pnl > 0:
                    buy_date_summary[buy_date]['盈利金额'] += pnl
                    buy_date_summary[buy_date]['盈利单数'] += 1
                else:
                    buy_date_summary[buy_date]['亏损金额'] += pnl  
                    buy_date_summary[buy_date]['亏损单数'] += 1
            else:
                print(f"警告: 卖出记录 {row.to_dict()} 没有对应的买入记录")

    rows = []
    for date, vals in buy_date_summary.items():
        rows.append({
            '日期': date, '盈利单数': vals['盈利单数'], '盈利金额': vals['盈利金额'],
            '亏损单数': vals['亏损单数'], '亏损金额': vals['亏损金额']
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    profit_days = df[df['盈利金额'] > 0][['日期', '盈利单数', '盈利金额']].copy()
    profit_days = profit_days.rename(columns={'日期': '盈利日期'})
    profit_days = profit_days.sort_values('盈利金额', ascending=False).reset_index(drop=True)

    loss_days = df[df['亏损金额'] < 0][['日期', '亏损单数', '亏损金额']].copy()
    loss_days = loss_days.rename(columns={'日期': '亏损日期'})
    loss_days = loss_days.sort_values('亏损金额', ascending=True).reset_index(drop=True)  

    combined = pd.concat([profit_days, loss_days], axis=1)
    return combined

# ------------------- 主执行入口 -------------------
if __name__ == "__main__":
    stock_list = parse_tdx_blk_file(BLOB_FILE_PATH)
    
    if not stock_list:
        print("未提取到股票代码，退出程序")
    else:
        stock_summary_result, trades_detail_result, total_trades_metrics = batch_backtest(
            stock_codes=stock_list,
            init_cash=100000.0,    
            commission=0.0003,     
            stop_loss_ratio=0.02   
        )
        
        trades_list = trades_detail_result.to_dict('records') if not trades_detail_result.empty else []
        compounding_metrics = calculate_pure_compounding(trades_list, init_cash=100000.0)
        
        print("\n" + "="*80)
        print("                    板块回测（无视重叠的极限复利）汇总")
        print("="*80)
        for k, v in compounding_metrics.items():
            if "(%)" in k:
                print(f"{k}: {v:.2f}%")
            elif "资金" in k:
                print(f"{k}: {v:,.2f} 元")
            else:
                print(f"{k}: {v}")
        print("="*80)
        
        loss_dates_df = analyze_loss_periods(trades_detail_result)
        t = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(time.time()))
        buy_date_stats = analyze_by_buy_date(trades_detail_result)
        
        if not stock_summary_result.empty:
            with pd.ExcelWriter(f"板块回测汇总结果_含总笔数{t}.xlsx", engine="openpyxl") as writer:
                stock_summary_result.to_excel(writer, sheet_name="单股票维度汇总", index=False)
                trades_detail_result.to_excel(writer, sheet_name="所有交易明细", index=False)
                
                total_trades_df = pd.DataFrame([total_trades_metrics])
                total_trades_df.to_excel(writer, sheet_name="总交易笔数维度汇总", index=False)
                
                if loss_dates_df is not None and not loss_dates_df.empty:
                    loss_dates_df.to_excel(writer, sheet_name="亏损日期提取", index=False)
                    
                if not buy_date_stats.empty:
                    buy_date_stats.to_excel(writer, sheet_name="按买入日期统计", index=False)
            print(f"\n汇总结果已保存到: 板块回测汇总结果_含总笔数{t}.xlsx")