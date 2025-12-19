import pandas as pd
import numpy as np
import os
import glob

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

def search_funds_by_stocks(stock_inputs: list[str], holdings_dir: str, year: int, filter_fund_codes: list[str] = None) -> pd.DataFrame:
    """
    Search for funds that hold the given stocks in their top 10 holdings.
    
    Args:
        stock_inputs: List of stock names or codes.
        holdings_dir: Directory containing holding CSV files.
        year: Year to filter files (e.g., 2024).
        filter_fund_codes: Optional list of fund codes to include in the search. 
                           If None, searches all files.
        
    Returns:
        DataFrame with columns: ['fund_code', 'match_count', 'match_degree', 'matched_stocks']
    """
    results = []
    
    # Normalize inputs: Remove empty strings and whitespace
    inputs = [s.strip() for s in stock_inputs if s.strip()]
    if not inputs:
        return pd.DataFrame()
    
    # Create a set for faster lookup if filter is provided
    allowed_codes = set(filter_fund_codes) if filter_fund_codes is not None else None
    
    # Pattern to match files for the specific year
    pattern = os.path.join(holdings_dir, f"*_{year}.csv")
    files = glob.glob(pattern)
    
    for file_path in files:
        fund_code = os.path.basename(file_path).split('_')[0]
        
        # Apply filter if permitted
        if allowed_codes is not None and fund_code not in allowed_codes:
            continue
            
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                continue
                
            # Ensure columns exist
            if '股票代码' not in df.columns or '股票名称' not in df.columns:
                continue
            
            # Convert codes to string
            df['股票代码'] = df['股票代码'].astype(str)
            
            matches = set()
            
            # Check for matches
            for item in inputs:
                # Check if item matches any stock code
                matched_codes = df[df['股票代码'] == item]['股票名称'].tolist()
                if matched_codes:
                    matches.update(matched_codes)
                
                # Check if item matches any stock name
                matched_names = df[df['股票名称'] == item]['股票名称'].tolist()
                if matched_names:
                    matches.update(matched_names)
            
            if matches:
                match_count = len(matches)
                match_degree = match_count / len(inputs)
                
                results.append({
                    'fund_code': fund_code,
                    'match_count': match_count,
                    'match_degree': match_degree,
                    'matched_stocks': ", ".join(matches)
                })
                
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
            
    if not results:
        return pd.DataFrame()
        
    results_df = pd.DataFrame(results)
    # Sort by match count (desc), then match degree (desc)
    results_df = results_df.sort_values(by=['match_count', 'match_degree'], ascending=False)
    
    return results_df
