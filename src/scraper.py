import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

from src.data_manager import load_fund_nav_from_cache, save_fund_nav_to_cache, \
                                 load_fund_holdings_from_cache, save_fund_holdings_to_cache, \
                                 update_fund_status, get_nav_last_date
from src.source_manager import get_active_source, update_source_status

def fetch_fund_info(fund_code: str) -> pd.DataFrame:
    """
    Fetch basic fund information.
    """
    try:
        return pd.DataFrame([{"基金代码": fund_code, "Message": "Basic info API unstable"}])
    except Exception as e:
        print(f"Error fetching info for {fund_code}: {e}")
        return pd.DataFrame()

def _fetch_holdings_akshare(fund_code: str, year: int) -> pd.DataFrame:
    """Internal helper to fetch holdings using Akshare."""
    return ak.fund_portfolio_hold_em(symbol=fund_code, date=str(year))

def fetch_fund_holdings(fund_code: str, year: int = 2024) -> pd.DataFrame:
    """
    Fetch fund holdings, prioritizing cached data, then using managed data sources.
    """
    # 1. Try Cache
    cached_df = load_fund_holdings_from_cache(fund_code, year)
    if not cached_df.empty:
        return cached_df

    # 2. Get Active Source
    source = get_active_source('holdings')
    if not source:
        print("No active data source found for 'holdings'.")
        return pd.DataFrame()

    print(f"Fetching holdings for {fund_code} using source: {source['name']}...")
    
    # 3. Fetch from Source
    try:
        df = pd.DataFrame()
        if source['handler'] == 'akshare_holdings':
            df = _fetch_holdings_akshare(fund_code, year)
        
        # 4. Process Result
        if not df.empty:
            save_fund_holdings_to_cache(fund_code, year, df)
            update_source_status(source['id'], True) # Source is working
            update_fund_status(fund_code, True)      # Fund code is valid
            return df
        else:
            # Empty return might mean invalid fund code, not necessarily broken source.
            # But continuous empty returns might imply source issues. 
            # For now, we assume source is okay (it connected), but fund is invalid/empty.
            update_fund_status(fund_code, False)
            return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching holdings from {source['name']}: {e}")
        # Exception usually means connection error or API change -> Source issue
        update_source_status(source['id'], False)
        return pd.DataFrame()

def _fetch_nav_akshare(fund_code: str) -> pd.DataFrame:
    """Internal helper to fetch NAV using Akshare."""
    return ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")

def fetch_fund_nav(fund_code: str, start_date: str = "20200101", end_date: str = None) -> pd.DataFrame:
    """
    Fetch historical NAV, prioritizing cached data if fresh enough.
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
        
    # Check freshness
    last_cached_date_str = get_nav_last_date(fund_code)
    is_fresh = False
    
    if last_cached_date_str:
        last_cached = pd.to_datetime(last_cached_date_str)
        target_end = pd.to_datetime(end_date)
        
        # If cache covers up to the target end date (or very close, e.g. within 3 days for weekends)
        # We consider it fresh.
        # Trading data stops on Friday. If today is Sunday, last data is Friday (2 days ago).
        days_diff = (target_end - last_cached).days
        if days_diff <= 3: 
            is_fresh = True
    
    # 1. Try Cache if fresh
    if is_fresh:
        cached_df = load_fund_nav_from_cache(fund_code)
        if not cached_df.empty:
            cached_df['净值日期'] = pd.to_datetime(cached_df['净值日期'])
            mask = (cached_df['净值日期'] >= pd.to_datetime(start_date)) & (cached_df['净值日期'] <= pd.to_datetime(end_date))
            return cached_df.loc[mask]

    # 2. Get Active Source (if not fresh or cache missing)
    source = get_active_source('nav')
    if not source:
        print("No active data source found for 'nav'.")
        # Fallback to cache even if stale if no source available
        cached_df = load_fund_nav_from_cache(fund_code)
        if not cached_df.empty:
             cached_df['净值日期'] = pd.to_datetime(cached_df['净值日期'])
             return cached_df
        return pd.DataFrame()

    print(f"Fetching NAV for {fund_code} using source: {source['name']} (Freshness check: {'Passed' if is_fresh else 'Failed/Missing'})...")

    # 3. Fetch from Source
    try:
        df = pd.DataFrame()
        if source['handler'] == 'akshare_nav':
            df = _fetch_nav_akshare(fund_code)
        
        # 4. Process Result
        if not df.empty and '净值日期' in df.columns:
            df['净值日期'] = pd.to_datetime(df['净值日期'])
            
            # Save raw data to cache
            save_fund_nav_to_cache(fund_code, df)
            
            # Update Statuses
            update_source_status(source['id'], True)
            update_fund_status(fund_code, True)
            
            # Filter return
            mask = (df['净值日期'] >= pd.to_datetime(start_date)) & (df['净值日期'] <= pd.to_datetime(end_date))
            return df.loc[mask]
        
        update_fund_status(fund_code, False)
        return df

    except Exception as e:
        print(f"Error fetching NAV from {source['name']}: {e}")
        update_source_status(source['id'], False)
        # Fallback to cache if fetch fails
        cached_df = load_fund_nav_from_cache(fund_code)
        if not cached_df.empty:
            cached_df['净值日期'] = pd.to_datetime(cached_df['净值日期'])
            return cached_df
        return pd.DataFrame()

def batch_fetch_holdings(fund_codes: list[str], year: int, progress_callback=None):
    """
    Batch fetch holdings for a list of funds.
    
    Args:
        fund_codes: List of fund codes.
        year: Year to fetch.
        progress_callback: Optional function(current, total, message) to report progress.
    """
    total = len(fund_codes)
    success_count = 0
    
    for i, code in enumerate(fund_codes):
        if progress_callback:
            progress_callback(i, total, f"Fetching {code}...")
            
        try:
            # We use fetch_fund_holdings which handles caching and sources
            df = fetch_fund_holdings(code, year)
            if not df.empty:
                success_count += 1
        except Exception as e:
            print(f"Error processing {code}: {e}")
            
    if progress_callback:
        progress_callback(total, total, f"Completed. Success: {success_count}/{total}")

def fetch_fund_estimation_batch(fund_codes: list[str] = None) -> pd.DataFrame:
    """
    Fetch real-time fund valuation estimation.
    """
    try:
        print("Fetching real-time fund estimation from Akshare...")
        df = ak.fund_value_estimation_em(symbol='全部')
        
        if df.empty:
            return pd.DataFrame()

        # Identify columns dynamically
        code_col = next((c for c in df.columns if '基金代码' in c), None)
        
        # Columns like '2025-12-25-估算数据-估算值'
        val_col = next((c for c in df.columns if '估算' in c and '估算值' in c), None)
        rate_col = next((c for c in df.columns if '估算' in c and '增长率' in c), None)
        
        if not (code_col and val_col and rate_col):
            print("Could not identify estimation columns.")
            return pd.DataFrame()
            
        # Rename
        rename_map = {
            code_col: '基金代码',
            val_col: '估算净值',
            rate_col: '估算涨幅'
        }
        df = df.rename(columns=rename_map)
        
        # Extract Valuation Time (Date) from the column name
        # Example val_col: '2025-12-25-估算数据-估算值'
        est_time = "Unknown"
        if val_col:
            try:
                # Extract the part before '-估算'
                est_time = val_col.split('-估算')[0]
            except:
                est_time = "Unknown"
        
        df['估算时间'] = est_time
        
        # Ensure Code is string
        df['基金代码'] = df['基金代码'].astype(str)
        
        # Filter
        if fund_codes:
            # Ensure input codes are strings
            target_codes = [str(c) for c in fund_codes]
            df = df[df['基金代码'].isin(target_codes)]
            
        return df[['基金代码', '估算净值', '估算涨幅', '估算时间']]
        
    except Exception as e:
        print(f"Error fetching estimation: {e}")
        return pd.DataFrame()
