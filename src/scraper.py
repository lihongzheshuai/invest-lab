import akshare as ak
import pandas as pd
from datetime import datetime

from src.data_manager import load_fund_nav_from_cache, save_fund_nav_to_cache, \
                                 load_fund_holdings_from_cache, save_fund_holdings_to_cache, \
                                 update_fund_status
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
    Fetch historical NAV, prioritizing cached data, then using managed data sources.
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    
    # 1. Try Cache
    cached_df = load_fund_nav_from_cache(fund_code)
    if not cached_df.empty:
        cached_df['净值日期'] = pd.to_datetime(cached_df['净值日期'])
        mask = (cached_df['净值日期'] >= pd.to_datetime(start_date)) & (cached_df['净值日期'] <= pd.to_datetime(end_date))
        return cached_df.loc[mask]

    # 2. Get Active Source
    source = get_active_source('nav')
    if not source:
        print("No active data source found for 'nav'.")
        return pd.DataFrame()

    print(f"Fetching NAV for {fund_code} using source: {source['name']}...")

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
        return pd.DataFrame()
