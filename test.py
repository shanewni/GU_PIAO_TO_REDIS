from pytdx.hq import TdxHq_API
import pandas as pd

# pytdx get_security_bars 函数 category 参数说明：
TIME_PERIODS = {
    0: "5分钟K线",
    1: "15分钟K线", 
    2: "30分钟K线",
    3: "1小时K线",
    4: "日K线",
    5: "周K线",
    6: "月K线",
    7: "1分钟K线",
    8: "1分钟K线",  # 与7相同
    9: "日K线",     # 与4相同
    10: "季K线",
    11: "年K线"
}

# 创建API对象
api = TdxHq_API()

# 选择服务器并连接，这里使用了一个公开的服务器IP和端口
# 如果这个服务器不稳定，可以尝试寻找其他可用的服务器IP
server_ip = '152.136.167.10'
server_port = 7709

try:
    if api.connect(server_ip, server_port):
        print("成功连接到行情服务器")
        
        # 获取股票代码为600000的5分钟K线数据
        # 参数说明: (市场代码, 股票代码, 起始索引, 需要获取的数量)
        # 市场代码：0 - 深圳，1 - 上海
        stock_code = '600000'
        market = 1  # 上海市场
        data = api.get_security_bars(0, market, stock_code, 0, 10)
        
        # 断开连接
        api.disconnect()
        
        if data:
            # 将数据转换为DataFrame
            # 注意：pytdx返回的数据字段顺序可能与以下定义一致，请根据实际情况调整
            df = pd.DataFrame(data, columns=['datetime', 'open', 'close', 'high', 'low', 'volume', 'amount', 'market', 'code'])
            # 处理时间戳
            df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M')
            print("成功获取5分钟K线数据：")
            print(df.head())
        else:
            print("未获取到数据。")
    else:
        print("连接行情服务器失败。")
except Exception as e:
    print(f"操作过程中发生错误: {e}")