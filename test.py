from pytdx.exhq import TdxExHq_API

api = TdxExHq_API()
# 这里的 IP 可以从通达信软件里的“通讯设置”里找，比如 119.147.212.81
if api.connect('152.136.167.10', 7727):
    # 获取扩展行情 (市场ID, 品种代码)
    # 市场ID：1代表上期所, 2代表中金所, 3代表大商所, 4代表郑商所
    data = api.get_instrument_bars(8, 1, 'AL2605', 0, 100) 
    df = api.to_df(data)
    api.disconnect()
    print(df)