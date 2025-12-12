import pandas as pd
import numpy as np

def calculate_max_drawdown(nav_series: pd.Series) -> float:
    """
    Calculate Maximum Drawdown from a series of NAVs.
    """
    if nav_series.empty:
        return 0.0
    
    # Calculate cumulative max
    roll_max = nav_series.cummax()
    daily_drawdown = nav_series / roll_max - 1.0
    max_drawdown = daily_drawdown.min()
    return max_drawdown

def calculate_annualized_return(nav_series: pd.Series, days: int) -> float:
    """
    Calculate Annualized Return.
    """
    if nav_series.empty or days <= 0:
        return 0.0
    
    total_return = nav_series.iloc[-1] / nav_series.iloc[0] - 1.0
    annualized = (1 + total_return) ** (365.0 / days) - 1.0
    return annualized

def analyze_position_changes(holdings_prev: pd.DataFrame, holdings_curr: pd.DataFrame) -> pd.DataFrame:
    """
    Compare two quarters of holdings to identify changes.
    holdings DataFrame expected columns: ['股票代码', '股票名称', '持仓市值']
    """
    if holdings_prev.empty and holdings_curr.empty:
        return pd.DataFrame()
    
    # Rename for merging
    prev = holdings_prev[['股票代码', '股票名称', '持仓市值']].rename(columns={'持仓市值': 'mv_prev'})
    curr = holdings_curr[['股票代码', '股票名称', '持仓市值']].rename(columns={'持仓市值': 'mv_curr'})
    
    # Merge
    merged = pd.merge(prev, curr, on=['股票代码', '股票名称'], how='outer').fillna(0)
    
    merged['change_type'] = 'UNCHANGED'
    merged['diff'] = merged['mv_curr'] - merged['mv_prev']
    
    # Identify types
    # NEW: 0 prev, >0 curr
    merged.loc[(merged['mv_prev'] == 0) & (merged['mv_curr'] > 0), 'change_type'] = 'NEW'
    # DELETE: >0 prev, 0 curr
    merged.loc[(merged['mv_prev'] > 0) & (merged['mv_curr'] == 0), 'change_type'] = 'DELETE'
    # INCREASE: >prev
    merged.loc[(merged['mv_prev'] > 0) & (merged['mv_curr'] > merged['mv_prev']), 'change_type'] = 'INCREASE'
    # DECREASE: <prev
    merged.loc[(merged['mv_prev'] > 0) & (merged['mv_curr'] < merged['mv_prev']) & (merged['mv_curr'] > 0), 'change_type'] = 'DECREASE'
    
    return merged

def estimate_adjustment_success(change_df: pd.DataFrame, stock_price_data: dict) -> float:
    """
    Estimate success rate.
    change_df: output of analyze_position_changes
    stock_price_data: dict mapping 'stock_code' -> 'return_next_quarter' (float)
    
    Logic:
    - If NEW or INCREASE, success if stock return > 0 (or benchmark)
    - If DELETE or DECREASE, success if stock return < 0 (avoided loss) or < fund_return
    
    Returns a success score (0.0 to 1.0)
    """
    if change_df.empty:
        return 0.0
    
    success_count = 0
    total_count = 0
    
    for _, row in change_df.iterrows():
        code = row['股票代码']
        c_type = row['change_type']
        
        # Determine stock return for the subsequent period
        # Ideally we fetch this. For now, we use a passed dictionary or mock it
        s_ret = stock_price_data.get(code, 0.0)
        
        if c_type in ['NEW', 'INCREASE']:
            total_count += 1
            if s_ret > 0:
                success_count += 1
        elif c_type in ['DELETE', 'DECREASE']:
            total_count += 1
            if s_ret < 0: # Avoided loss
                success_count += 1
                
    if total_count == 0:
        return 0.0
        
    return success_count / total_count
