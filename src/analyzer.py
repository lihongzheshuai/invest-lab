import pandas as pd
import numpy as np

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
