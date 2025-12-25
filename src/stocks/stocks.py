import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import requests
import concurrent.futures

def get_stock_concepts_eastmoney(symbol):
    """
    Fetch concepts for a single stock from EastMoney.
    """
    try:
        # Determine prefix
        if str(symbol).startswith(('6', '9')): prefix = "SH"
        elif str(symbol).startswith(('0', '3')): prefix = "SZ"
        elif str(symbol).startswith(('8', '4')): prefix = "BJ"
        else: prefix = "SZ"
        
        url = "http://emweb.securities.eastmoney.com/PC_HSF10/CoreConception/PageAjax"
        params = {"code": f"{prefix}{symbol}"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        r = requests.get(url, params=params, headers=headers, timeout=2)
        data = r.json()
        
        if 'ssbk' in data and data['ssbk']:
            concepts = [item['BOARD_NAME'] for item in data['ssbk']]
            return ";".join(concepts)
    except Exception:
        return ""
    return ""

def get_limit_up_model(date: str = None):
    # 1. 获取数据：自动寻找最近一个有数据的交易日
    df = pd.DataFrame()
    used_date = None

    if date:
        print(f"正在抓取东财 {date} 涨停板池数据...")
        try:
            df = ak.stock_zt_pool_em(date=date)
            used_date = date
        except Exception as e:
            print(f"抓取指定日期数据失败: {e}")
    else:
        # 自动回溯查找最近10天的数据
        curr_time = datetime.now()
        print("正在寻找最近一个交易日的涨停数据...")
        
        for i in range(10):
            check_date = curr_time - timedelta(days=i)
            date_str = check_date.strftime("%Y%m%d")
            
            try:
                temp_df = ak.stock_zt_pool_em(date=date_str)
                if not temp_df.empty:
                    df = temp_df
                    used_date = date_str
                    print(f"成功获取 {date_str} 的数据")
                    break
            except Exception:
                continue
    
    if df.empty:
        print("未获取到任何近期涨停板数据。")
        return pd.DataFrame()

    # 2. 数据处理与格式化
    # 检查必要列是否存在
    required_columns = ['炸板次数', '换手率', '最后封板时间', '代码', '名称', '所属行业', '连板数', '最新价']
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"数据源缺失必要列: {missing_cols}")
        return pd.DataFrame()

    # 复制一份数据进行处理
    result = df.copy()
    
    # --- 新增：并发获取个股概念 ---
    print("正在抓取个股概念数据 (多线程)...")
    result['所属概念'] = "" # Initialize
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_index = {
            executor.submit(get_stock_concepts_eastmoney, row['代码']): index 
            for index, row in result.iterrows()
        }
        
        for future in concurrent.futures.as_completed(future_to_index):
            index = future_to_index[future]
            try:
                concepts = future.result()
                result.at[index, '所属概念'] = concepts
            except Exception:
                pass
    # ---------------------------

    # 格式化最后封板时间为 HH:MM:SS
    def format_time(t_str):
        s = str(t_str).replace(':', '').zfill(6)
        return f"{s[:2]}:{s[2:4]}:{s[4:]}"
    
    result['最后封板时间'] = result['最后封板时间'].apply(format_time)
    
    # 转换数值列
    result['换手率'] = pd.to_numeric(result['换手率'], errors='coerce')
    result['最新价'] = pd.to_numeric(result['最新价'], errors='coerce')

    # 3. 结果排序：优先按连板数降序，其次按最后封板时间升序
    # 注意：最后封板时间是字符串，升序即时间越早越前
    result = result.sort_values(by=['连板数', '最后封板时间'], ascending=[False, True])
    
    # 添加日期列以便前端展示
    if used_date:
        result['日期'] = used_date

    # 确保返回列包含日期和概念
    final_cols = ['日期'] + required_columns + ['所属概念']
    return result[final_cols]

# 运行模型
if __name__ == "__main__":
    # 可以尝试获取当天的，如果为空则尝试获取前一个交易日（简单起见这里先只试今天）
    candidates = get_limit_up_model()
    
    if candidates.empty:
        # 如果实时数据为空，尝试获取最近一个交易日（可选）
        print("尝试获取最近一个交易日的数据...")
        # 这里仅作演示，实际应用中可以根据需要调整
        # candidates = get_limit_up_model(date="20251223") 
    
    if not candidates.empty:
        print(f"\n筛选出 {len(candidates)} 只符合“开板回封+高换手”的模型股：")
        print(candidates)
    else:
        print("\n今日暂无符合“开板回封+高换手”模型的个股。")