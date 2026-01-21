import pandas as pd

# 1. 指定你的通达信软件安装目录（请修改为你的实际路径）
tdx_install_path = r'D:/new_tdx_x/new_tdx'  # 例如：C:\new_tdx, D:\TDX

# 2. 构建具体的 .cfg 文件路径
hq_cache_path = f'{tdx_install_path}\\T0002\\hq_cache'
tdxhy_file = f'{hq_cache_path}\\tdxhy.cfg'  # 股票-行业映射文件
tdxzs_file = f'{hq_cache_path}\\tdxzs3.cfg'  # 指数列表文件

# 3. 读取 tdxhy.cfg (通常为GBK编码)
try:
    # 该文件通常以 '|' 或制表符分隔，没有表头
    df_hy = pd.read_csv(tdxhy_file, sep='|', encoding='gbk', dtype=str, header=None, engine='python')
    stock_to_industry_map = pd.Series(df_hy[5].values, index=df_hy[1]).to_dict()
        # 打印字典长度和前几项验证
    print(f"成功构建映射，共 {len(stock_to_industry_map)} 条记录。")
    print("示例（前5项）:", list(stock_to_industry_map.items())[:5])
except FileNotFoundError:
    print(f"错误：未在 '{tdxhy_file}' 找到文件，请检查路径。")
except Exception as e:
    print(f"读取文件时出错: {e}")

# 4. 读取 tdxzs.cfg
try:
    df_zs = pd.read_csv(tdxzs_file, sep='|', header=None,  dtype=str,encoding='gbk', engine='python')
    mask = df_zs[5].str.startswith('X')  # 这是关键过滤条件
    filtered_series = pd.Series(df_zs.loc[mask, 5].values, index=df_zs.loc[mask, 1])
    stock_to_zs_map = filtered_series.to_dict()
        # 打印字典长度和前几项验证
    print(f"成功构建映射，共 {len(stock_to_zs_map)} 条记录。")
    print("示例（前5项）:", list(stock_to_zs_map.items())[:5])
    result =  stock_to_zs_map.get('881449')  # 示例查询
    print(f"股票 {881449} 的行业代码是: {result}")
except Exception as e:
    print(f"读取 tdxzs.cfg 时出错: {e}")