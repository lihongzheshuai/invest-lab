import pandas as pd
import numpy as np
import os
import glob
import asyncio
import json
import hashlib
import time

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
HOLDINGS_DIR = os.path.join(DATA_DIR, 'holdings')
REVERSE_INDEX_FILE = os.path.join(DATA_DIR, 'reverse_index.json')

# --- Reverse Index Cache Logic ---

def load_reverse_index():
    """
    Load the reverse index cache.
    Structure: {
        'timestamp': 123456789,
        'scanned_funds': ['000001', ...],
        'index': { 'StockName/Code': ['000001', ...] },
        'fund_quarters': { '000001': '2024Q3', ... },
        'fund_stocks': { '000001': ['StockA', 'CodeA', ...] } # Forward index for cleanup
    }
    """
    if not os.path.exists(REVERSE_INDEX_FILE):
        return {'timestamp': 0, 'scanned_funds': [], 'index': {}, 'fund_quarters': {}, 'fund_stocks': {}}
        
    try:
        with open(REVERSE_INDEX_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Ensure schema compatibility
        if 'fund_stocks' not in data:
            data['fund_stocks'] = {}
            
        # Validity Check: Holdings Dir MTime
        holdings_mtime = os.path.getmtime(HOLDINGS_DIR) if os.path.exists(HOLDINGS_DIR) else 0
        cache_ts = data.get('timestamp', 0)
        
        # Invalidate only if directory is SIGNIFICANTLY newer than cache (2s buffer)
        if holdings_mtime > cache_ts + 2.0:
            return {'timestamp': 0, 'scanned_funds': [], 'index': {}, 'fund_quarters': {}, 'fund_stocks': {}}
            
        return data
    except Exception:
        return {'timestamp': 0, 'scanned_funds': [], 'index': {}, 'fund_quarters': {}, 'fund_stocks': {}}

def save_reverse_index(index_data):
    """Save the reverse index to disk."""
    try:
        index_data['timestamp'] = time.time()
        with open(REVERSE_INDEX_FILE, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False)
    except Exception as e:
        print(f"Failed to save reverse index: {e}")

# --- Core Analysis Logic ---

def analyze_position_changes(holdings_prev: pd.DataFrame, holdings_curr: pd.DataFrame) -> pd.DataFrame:
    """Compare two quarters of holdings."""
    if holdings_prev.empty and holdings_curr.empty:
        return pd.DataFrame()
    
    prev = holdings_prev[['股票代码', '股票名称', '持仓市值']].rename(columns={'持仓市值': 'mv_prev'})
    curr = holdings_curr[['股票代码', '股票名称', '持仓市值']].rename(columns={'持仓市值': 'mv_curr'})
    
    merged = pd.merge(prev, curr, on=['股票代码', '股票名称'], how='outer').fillna(0)
    
    merged['change_type'] = 'UNCHANGED'
    merged['diff'] = merged['mv_curr'] - merged['mv_prev']
    
    merged.loc[(merged['mv_prev'] == 0) & (merged['mv_curr'] > 0), 'change_type'] = 'NEW'
    merged.loc[(merged['mv_prev'] > 0) & (merged['mv_curr'] == 0), 'change_type'] = 'DELETE'
    merged.loc[(merged['mv_prev'] > 0) & (merged['mv_curr'] > merged['mv_prev']), 'change_type'] = 'INCREASE'
    merged.loc[(merged['mv_prev'] > 0) & (merged['mv_curr'] < merged['mv_prev']) & (merged['mv_curr'] > 0), 'change_type'] = 'DECREASE'
    
    return merged

async def process_single_fund(fund_code, year, holdings_dir, sem, progress_callback=None):
    """
    Async worker: Check Cache -> Fetch -> Extract ALL Stocks
    Returns: (fund_code, [stock_list], latest_quarter) or None
    """
    from src.scraper import fetch_fund_holdings
    
    async with sem:
        df = pd.DataFrame()
        file_path = os.path.join(holdings_dir, f"{fund_code}_{year}.csv")
        
        # 1. Read
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig', dtype={'股票代码': str})
            except UnicodeDecodeError:
                try: df = pd.read_csv(file_path, encoding='gb18030', dtype={'股票代码': str})
                except: pass
            except: pass
        
        # 2. Fetch if missing
        if df.empty:
            loop = asyncio.get_event_loop()
            try:
                df = await loop.run_in_executor(None, fetch_fund_holdings, fund_code, year)
            except: pass

        if progress_callback:
            progress_callback()

        # 3. Extract Stocks
        if df is None or df.empty:
            # Mark as scanned but empty (e.g. Bond fund or data missing)
            return (fund_code, [], "Unknown")
            
        if '股票代码' not in df.columns or '股票名称' not in df.columns:
            return (fund_code, [], "Unknown")
        
        latest_quarter = "Unknown"
        if '季度' in df.columns and not df['季度'].empty:
            latest_quarter = df['季度'].max()
            df = df[df['季度'] == latest_quarter] # Filter for latest
            
        df['股票代码'] = df['股票代码'].astype(str)
        
        # Return all stocks found in this fund
        stocks = []
        for _, row in df.iterrows():
            stocks.append({'code': str(row['股票代码']), 'name': str(row['股票名称'])})
            
        return (fund_code, stocks, latest_quarter)

async def search_funds_by_stocks_async(stock_inputs: list[str], holdings_dir: str, year: int, filter_fund_codes: list[str], progress_callback=None) -> pd.DataFrame:
    """
    Async Search with Reverse Index Caching.
    1. Loads Index.
    2. Identifies funds in `filter_fund_codes` that are NOT yet scanned/indexed.
    3. Scans those funds (fetch/read) and updates the Global Index.
    4. Queries the Index for `stock_inputs`.
    5. Returns matches found within `filter_fund_codes`.
    """
    inputs = [s.strip() for s in stock_inputs if s.strip()]
    if not inputs or not filter_fund_codes:
        return pd.DataFrame()
        
    # Load Index
    cache_data = load_reverse_index()
    scanned_set = set(cache_data['scanned_funds'])
    
    # Identify Unscanned Funds
    unscanned_codes = [c for c in filter_fund_codes if c not in scanned_set]
    
    # If we have unscanned funds, we must scan them
    if unscanned_codes:
        sem = asyncio.Semaphore(20)
        tasks = [process_single_fund(code, year, holdings_dir, sem, progress_callback) for code in unscanned_codes]
        
        results = await asyncio.gather(*tasks)
        
        # Update Index
        updated = False
        index = cache_data.get('index', {})
        fund_stocks = cache_data.get('fund_stocks', {})
        
        for res in results:
            if res:
                f_code, stocks, quarter = res
                
                # 1. Clean up OLD stocks for this fund if it was indexed before
                # (Though usually scanned_funds check prevents re-scanning, 
                # but if we FORCE re-scan or invalidation happens partially, this is safe)
                if f_code in fund_stocks:
                    old_keys = fund_stocks[f_code]
                    for key in old_keys:
                        if key in index and f_code in index[key]:
                            index[key].remove(f_code)
                            # Cleanup empty lists to keep JSON small? Optional.
                            if not index[key]: del index[key]
                
                # 2. Add NEW stocks
                new_keys = []
                for stock in stocks:
                    s_code = stock['code']
                    s_name = stock['name']
                    
                    # Index by Code
                    if s_code not in index: index[s_code] = []
                    if f_code not in index[s_code]: index[s_code].append(f_code)
                    new_keys.append(s_code)
                    
                    # Index by Name
                    if s_name not in index: index[s_name] = []
                    if f_code not in index[s_name]: index[s_name].append(f_code)
                    new_keys.append(s_name)
                
                # 3. Update Metadata
                cache_data['scanned_funds'].append(f_code)
                cache_data['fund_quarters'][f_code] = quarter
                fund_stocks[f_code] = new_keys
                
                updated = True
        
        if updated:
            # Deduplicate scanned_funds list
            cache_data['scanned_funds'] = list(set(cache_data['scanned_funds']))
            # Ensure index keys exist
            cache_data['index'] = index
            cache_data['fund_stocks'] = fund_stocks
            save_reverse_index(cache_data)
    
    # Query Index
    final_results = []
    scope_set = set(filter_fund_codes)
    index = cache_data.get('index', {})
    quarters = cache_data.get('fund_quarters', {})
    
    fund_hits = {} # {fund_code: {matched_stocks_set}}
    
    for inp in inputs:
        if inp in index:
            hit_funds = index[inp]
            for f_code in hit_funds:
                if f_code in scope_set:
                    if f_code not in fund_hits: fund_hits[f_code] = set()
                    fund_hits[f_code].add(inp)
    
    # Build Result Rows
    for f_code, matches in fund_hits.items():
        if matches:
            final_results.append({
                'fund_code': f_code,
                'match_count': len(matches),
                'match_degree': len(matches) / len(inputs),
                'matched_stocks': ", ".join(matches),
                'quarter': quarters.get(f_code, 'Unknown')
            })
            
    if not final_results:
        return pd.DataFrame()
        
    results_df = pd.DataFrame(final_results)
    results_df = results_df.sort_values(by=['match_count', 'match_degree'], ascending=False)
    return results_df

def check_cache_coverage(fund_codes):
    """
    Check if the provided fund codes have already been scanned/indexed.
    Returns True if all codes are in the 'scanned_funds' set of the valid cache.
    """
    cache_data = load_reverse_index()
    if not cache_data or not cache_data['scanned_funds']:
        return False
        
    scanned_set = set(cache_data['scanned_funds'])
    # Check if all target funds are in scanned set
    # Using set comparison for speed
    target_set = set(fund_codes)
    return target_set.issubset(scanned_set)

def query_reverse_index_direct(stock_inputs, filter_fund_codes):
    """
    Directly query the reverse index for stocks within the given fund scope.
    Assumes cache is valid and covers the funds (checked via check_cache_coverage).
    """
    inputs = [s.strip() for s in stock_inputs if s.strip()]
    if not inputs:
        return pd.DataFrame()
        
    cache_data = load_reverse_index()
    index = cache_data.get('index', {})
    quarters = cache_data.get('fund_quarters', {})
    scope_set = set(filter_fund_codes)
    
    fund_hits = {} # {fund_code: {matched_stocks_set}}
    
    for inp in inputs:
        if inp in index:
            hit_funds = index[inp]
            for f_code in hit_funds:
                if f_code in scope_set:
                    if f_code not in fund_hits: fund_hits[f_code] = set()
                    fund_hits[f_code].add(inp)
    
    final_results = []
    for f_code, matches in fund_hits.items():
        if matches:
            final_results.append({
                'fund_code': f_code,
                'match_count': len(matches),
                'match_degree': len(matches) / len(inputs),
                'matched_stocks': ", ".join(matches),
                'quarter': quarters.get(f_code, 'Unknown')
            })
            
    if not final_results:
        return pd.DataFrame()
        
    results_df = pd.DataFrame(final_results)
    results_df = results_df.sort_values(by=['match_count', 'match_degree'], ascending=False)
    return results_df

def search_funds_by_stocks(stock_inputs: list[str], holdings_dir: str, year: int, filter_fund_codes: list[str] = None) -> pd.DataFrame:
    """Sync wrapper."""
    if filter_fund_codes:
        return asyncio.run(search_funds_by_stocks_async(stock_inputs, holdings_dir, year, filter_fund_codes))
    return pd.DataFrame()