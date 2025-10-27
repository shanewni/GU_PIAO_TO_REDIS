from collections import deque
import math

class MergedBar:
    def __init__(self, high, low, orig_idx, is_same_value):
        self.high = high
        self.low = low
        self.orig_idx = orig_idx
        self.is_same_value = is_same_value

def sliding_max(arr, window_size):
    """计算滑动窗口最大值（单调队列实现）"""
    n = len(arr)
    result = [ -math.inf for _ in range(n) ]
    dq = deque()  # 存储索引，队首为当前窗口最大值索引
    
    for i in range(n):
        # 移除窗口外的元素（索引 <= i - window_size）
        while dq and dq[0] <= i - window_size:
            dq.popleft()
        
        # 移除队列中小于当前元素的索引（它们不可能是最大值）
        while dq and arr[dq[-1]] <= arr[i]:
            dq.pop()
        
        dq.append(i)
        
        # 窗口完全形成后开始记录结果
        if i >= window_size - 1:
            result[i] = arr[dq[0]]
    
    return result

def sliding_min(arr, window_size):
    """计算滑动窗口最小值（单调队列实现）"""
    n = len(arr)
    result = [ math.inf for _ in range(n) ]
    dq = deque()  # 存储索引，队首为当前窗口最小值索引
    
    for i in range(n):
        # 移除窗口外的元素
        while dq and dq[0] <= i - window_size:
            dq.popleft()
        
        # 移除队列中大于当前元素的索引
        while dq and arr[dq[-1]] >= arr[i]:
            dq.pop()
        
        dq.append(i)
        
        # 窗口完全形成后开始记录结果
        if i >= window_size - 1:
            result[i] = arr[dq[0]]
    
    return result

def merge_contained_bars(high, low, data_len):
    """处理K线包含关系及连续同值K线合并"""
    if data_len == 0:
        return []
    
    merged = []
    eps = 1e-6  # 浮点数精度阈值
    
    for i in range(data_len):
        curr_high = high[i]
        curr_low = low[i]
        # 判断当前K线是否为"同值K线"（high≈low）
        is_current_same = abs(curr_high - curr_low) < eps
        
        current_bar = MergedBar(
            high=curr_high,
            low=curr_low,
            orig_idx=i,
            is_same_value=is_current_same
        )
        
        if not merged:
            merged.append(current_bar)
            continue
        
        last_bar = merged[-1]
        
        # 处理连续同值K线合并
        if is_current_same and last_bar.is_same_value:
            values_equal = abs(last_bar.high - curr_high) < eps
            if values_equal:
                # 合并：保留最早的orig_idx
                merged_same_bar = MergedBar(
                    high=last_bar.high,
                    low=last_bar.low,
                    orig_idx=last_bar.orig_idx,
                    is_same_value=True
                )
                merged.pop()
                merged.append(merged_same_bar)
                continue
        
        # 同值K线与非同值K线不参与包含合并
        if last_bar.is_same_value or is_current_same:
            merged.append(current_bar)
            continue
        
        # 检查包含关系
        is_contained = (curr_high <= last_bar.high + eps and curr_low >= last_bar.low - eps) or \
                       (last_bar.high <= curr_high + eps and last_bar.low >= curr_low - eps)
        
        if not is_contained:
            merged.append(current_bar)
        else:
            # 存在包含关系，按趋势合并
            if len(merged) >= 2:
                prev_last = merged[-2]
                # 判断前序趋势
                is_up = (last_bar.high > prev_last.high + eps) and (last_bar.low > prev_last.low + eps)
                is_down = (last_bar.high < prev_last.high - eps) and (last_bar.low < prev_last.low - eps)
                
                if is_up:
                    max_high = max(last_bar.high, curr_high)
                    if abs(max_high - last_bar.high) < eps:
                        orig_idx = last_bar.orig_idx
                    else:
                        orig_idx = current_bar.orig_idx
                    new_bar = MergedBar(
                        high=max_high,
                        low=max(last_bar.low, curr_low),
                        orig_idx=orig_idx,
                        is_same_value=False
                    )
                elif is_down:
                    min_low = min(last_bar.low, curr_low)
                    if abs(min_low - last_bar.low) < eps:
                        orig_idx = last_bar.orig_idx
                    else:
                        orig_idx = current_bar.orig_idx
                    new_bar = MergedBar(
                        high=min(last_bar.high, curr_high),
                        low=min_low,
                        orig_idx=orig_idx,
                        is_same_value=False
                    )
                else:
                    # 非明确趋势：取范围
                    new_bar = MergedBar(
                        high=max(last_bar.high, curr_high),
                        low=min(last_bar.low, curr_low),
                        orig_idx=last_bar.orig_idx,
                        is_same_value=False
                    )
            else:
                # 仅1根非同值K线，合并取范围
                max_high = max(last_bar.high, curr_high)
                if abs(max_high - last_bar.high) < eps:
                    orig_idx = last_bar.orig_idx
                else:
                    orig_idx = current_bar.orig_idx
                new_bar = MergedBar(
                    high=max_high,
                    low=min(last_bar.low, curr_low),
                    orig_idx=orig_idx,
                    is_same_value=False
                )
            
            merged.pop()
            merged.append(new_bar)
    
    return merged

def identify_turns(data_len, high, low):
    """识别K线转向点（顶底分型）主函数"""
    # 初始化输出为0
    pf_out = [0.0 for _ in range(data_len)]
    
    if data_len < 6:
        return pf_out
    
    # 处理K线包含关系及合并
    merged_bars = merge_contained_bars(high, low, data_len)
    merged_len = len(merged_bars)
    window_size = 4
    
    if merged_len <= window_size * 2:
        return pf_out
    
    # 提取合并后K线的high和low数组
    merged_high = [bar.high for bar in merged_bars]
    merged_low = [bar.low for bar in merged_bars]
    
    # 滑动窗口极值预计算
    # 1. 前序窗口（i - window_size 到 i - 1）的max_high和min_low
    pre_max_high = sliding_max(merged_high, window_size)
    pre_min_low = sliding_min(merged_low, window_size)
    
    # 2. 后序窗口（i + 1 到 i + window_size）的max_high和min_low
    # 反转数组计算滑动窗口，再反转结果
    reversed_high = merged_high[::-1]
    reversed_low = merged_low[::-1]
    reversed_post_max_high = sliding_max(reversed_high, window_size)
    reversed_post_min_low = sliding_min(reversed_low, window_size)
    # 反转回原顺序
    post_max_high = reversed_post_max_high[::-1]
    post_min_low = reversed_post_min_low[::-1]
    
    # 识别转向点
    turns = []
    
    # 处理中间部分K线（前后各window_size根）
    for i in range(window_size, merged_len - window_size):
        current = merged_bars[i]
        current_high = current.high
        current_low = current.low
        
        # 前序窗口最大值（i - window_size 到 i - 1）
        max_prev_high = pre_max_high[i - 1]
        # 后序窗口最大值（i + 1 到 i + window_size）
        max_next_high = post_max_high[i + 1]
        max_prev_next_high = max(max_prev_high, max_next_high)
        
        # 前序窗口最小值（i - window_size 到 i - 1）
        min_prev_low = pre_min_low[i - 1]
        # 后序窗口最小值（i + 1 到 i + window_size）
        min_next_low = post_min_low[i + 1]
        min_prev_next_low = min(min_prev_low, min_next_low)
        
        if current_high >= max_prev_next_high - 1e-6:
            turns.append( (i, 1) )
        elif current_low <= min_prev_next_low + 1e-6:
            turns.append( (i, -1) )
    
    # 处理最后window_size根K线
    start = max(0, merged_len - window_size)
    for i in range(start, merged_len):
        current = merged_bars[i]
        current_high = current.high
        current_low = current.low
        
        start_prev = max(0, i - window_size)
        # 计算前序窗口极值
        if start_prev == 0 and i - 1 >= window_size - 1:
            max_prev_high = pre_max_high[i - 1]
            min_prev_low = pre_min_low[i - 1]
        else:
            # 不足window_size时直接计算
            max_prev_high = max(merged_high[start_prev:i]) if start_prev < i else -math.inf
            min_prev_low = min(merged_low[start_prev:i]) if start_prev < i else math.inf
        
        if current_high >= max_prev_high - 1e-6:
            turns.append( (i, 1) )
        elif current_low <= min_prev_low + 1e-6:
            turns.append( (i, -1) )
    
    # 合并连续同类型转向点
    new_turns = []
    i = 0
    while i < len(turns):
        current_idx, current_type = turns[i]
        current_val = merged_bars[current_idx].high if current_type == 1 else merged_bars[current_idx].low
        
        j = i + 1
        while j < len(turns) and turns[j][1] == current_type:
            val = merged_bars[turns[j][0]].high if current_type == 1 else merged_bars[turns[j][0]].low
            if (current_type == 1 and val > current_val + 1e-6) or (current_type == -1 and val < current_val - 1e-6):
                current_idx = turns[j][0]
                current_val = val
            j += 1
        
        new_turns.append( (current_idx, current_type) )
        i = j
    
    # 顶底分型高低关系验证
    validated_turns = []
    for i in range(len(new_turns)):
        if i == 0:
            validated_turns.append(new_turns[i])
            continue
        
        if not validated_turns:
            validated_turns.append(new_turns[i])
            continue
        
        prev_idx, prev_type = validated_turns[-1]
        curr_idx, curr_type = new_turns[i]
        
        if prev_type == curr_type:
            continue
        
        # 获取分型关键值
        prev_val = merged_bars[prev_idx].high if prev_type == 1 else merged_bars[prev_idx].low
        curr_val = merged_bars[curr_idx].high if curr_type == 1 else merged_bars[curr_idx].low
        
        # 验证高低关系
        is_valid = False
        if (prev_type, curr_type) == (-1, 1):
            is_valid = curr_val > prev_val + 1e-6
        elif (prev_type, curr_type) == (1, -1):
            is_valid = curr_val < prev_val - 1e-6
        
        if is_valid:
            validated_turns.append( (curr_idx, curr_type) )
        else:
            validated_turns.pop()
    
    new_turns = validated_turns
    if len(new_turns) <= window_size:
        return pf_out
    
    # 验证交替性并输出
    confirmed_turns = []
    for i in range(len(new_turns) - 1):
        idx1, t1 = new_turns[i]
        idx2, t2 = new_turns[i + 1]
        if (t1 == 1 and t2 == -1) or (t1 == -1 and t2 == 1):
            confirmed_turns.append( (idx1, t1) )
    if new_turns:
        confirmed_turns.append(new_turns[-1])
    
    # 映射到原始索引输出
    for merged_idx, turn_type in confirmed_turns:
        orig_idx = merged_bars[merged_idx].orig_idx
        if orig_idx < data_len:
            pf_out[orig_idx] = float(turn_type)
    
    return pf_out