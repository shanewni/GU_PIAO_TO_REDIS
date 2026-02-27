@staticmethod
    def calculate_dynamic_sell_signals(high_full: List[float], low_full: List[float], 
                                     close_full: List[float], ma60_full: pd.Series,
                                     buy_price: float = None, buy_idx: int = None) -> tuple[List[bool], List[str]]:
        """
        逐K线模拟动态出现，计算动态卖出条件。
        增加逻辑：针对MA60卖出条件，需满足涨幅>10%或持仓>30根K线。
        """
        total_length = len(high_full)
        sell_signals = [False] * total_length
        sell_reasons = [""] * total_length
        ma60_list = ma60_full.tolist()
        
        for window_end in range(1, total_length + 1):
            if window_end < 60:
                continue
                
            current_idx = window_end - 1
            current_close = close_full[current_idx]
            
            # === 1. 均线条件判定 (含门槛校验) ===
            cond_ma60 = False
            ma60_reason = ""
            
            # 基础条件：连续两根收盘价小于MA60
            if current_idx >= 1:
                if current_close < ma60_list[current_idx] and close_full[current_idx-1] < ma60_list[current_idx-1]:
                    # 检查是否处于持仓状态且满足门槛
                    if buy_price is not None and buy_idx is not None:
                        profit_ratio = (current_close - buy_price) / buy_price
                        hold_count = current_idx - buy_idx
                        
                        # 执行门槛检查
                        if profit_ratio >= 0.10 or hold_count >= 30:
                            cond_ma60 = True
                            ma60_reason = f"连续2根低于MA60(达标:收益{profit_ratio:.1%}/持仓{hold_count}线)"
                    else:
                        # 如果没有传入买入信息（初始化阶段），默认不触发均线卖出
                        cond_ma60 = False

            # === 2. 分型条件判定 (不设门槛，作为硬止损) ===
            high_window = high_full[:window_end]
            frac_window = gupiaojichu.identify_turns(window_end, high_window, low_full[:window_end])
            
            top_indices = [i for i, val in enumerate(frac_window) if val == 1.0]
            bottom_indices = [i for i, val in enumerate(frac_window) if val == -1.0]
            
            cond_pattern1 = False # 顶分型后未突破
            cond_pattern2 = False # 跌破底分型
            
            if bottom_indices:
                last_bottom_idx = bottom_indices[-1]
                if low_full[current_idx] < low_full[last_bottom_idx]:
                    cond_pattern2 = True
                    
                valid_tops = [i for i in top_indices if i < last_bottom_idx]
                if valid_tops:
                    prev_top_idx = valid_tops[-1]
                    if (current_idx - last_bottom_idx) >= (last_bottom_idx - prev_top_idx):
                        if max(high_window[last_bottom_idx:]) <= high_window[prev_top_idx]:
                            cond_pattern1 = True
            
            # === 3. 综合信号 ===
            reasons = []
            if cond_ma60: reasons.append(ma60_reason)
            if cond_pattern1: reasons.append("底分型后未突破前高且距离达标")
            if cond_pattern2: reasons.append("跌破最后底分型最低价")
            
            if reasons:
                sell_signals[current_idx] = True
                sell_reasons[current_idx] = "；".join(reasons)
                
        return sell_signals, sell_reasons