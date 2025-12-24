import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

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
            
            # 只有在尝试过去日期时才打印详细日志，避免刷屏
            # print(f"尝试获取 {date_str} 数据...") 
            
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

    # 2. 核心筛选逻辑设定
    # 检查必要列是否存在
    required_columns = ['炸板次数', '换手率', '最后封板时间', '代码', '名称', '所属行业', '连板数', '最新价']
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"数据源缺失必要列: {missing_cols}")
        return pd.DataFrame()

    # 逻辑 A：筛选有过“分歧”的个股（炸板次数 > 0）
    divergence_df = df[df['炸板次数'] > 0].copy()
    
    if divergence_df.empty:
        return pd.DataFrame()

    # 逻辑 C：筛选“强承接”（最后封板时间在 14:30 之前，避免尾盘勉强回封）
    # 注意：东财封板时间格式通常为 '093500' (HHMMSS)
    # 确保去除可能存在的冒号并进行比较
    divergence_df['最后封板时间_clean'] = divergence_df['最后封板时间'].astype(str).str.replace(':', '')
    strong_reseal = divergence_df[divergence_df['最后封板时间_clean'] <= '143000'].copy()
    
    if strong_reseal.empty:
        return pd.DataFrame()

    # 逻辑 B：筛选“高换手”（例如换手率介于 10% - 30% 之间）
    # 换手过低没分歧，换手过高（如>35%）可能是主力彻底出逃，需谨慎
    strong_reseal['换手率'] = pd.to_numeric(strong_reseal['换手率'], errors='coerce')
    result_df = strong_reseal[
        (strong_reseal['换手率'] >= 10.0) & (strong_reseal['换手率'] <= 30.0)
    ]
    
    if result_df.empty:
        return pd.DataFrame()

    # 3. 结果排序：按换手率递减排序
    result = result_df.sort_values(by='换手率', ascending=False).copy()
    
    # 格式化最后封板时间为 HH:MM:SS
    def format_time(t_str):
        s = str(t_str).replace(':', '').zfill(6)
        return f"{s[:2]}:{s[2:4]}:{s[4:]}"
    
    result['最后封板时间'] = result['最后封板时间'].apply(format_time)
    
    # 添加日期列以便前端展示
    if used_date:
        result['日期'] = used_date

    # 确保返回列包含日期
    final_cols = ['日期'] + required_columns
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