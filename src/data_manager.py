import os
import pandas as pd
import akshare as ak
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
NAV_DIR = os.path.join(DATA_DIR, 'nav')
HOLDINGS_DIR = os.path.join(DATA_DIR, 'holdings')
FUNDS_LIST_PATH = os.path.join(DATA_DIR, 'funds.csv')

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created data directory at {DATA_DIR}")

def ensure_data_dir_structure():
    """Ensures the main data directory and its subdirectories exist."""
    ensure_data_dir()
    if not os.path.exists(NAV_DIR):
        os.makedirs(NAV_DIR)
        print(f"Created NAV data directory at {NAV_DIR}")
    if not os.path.exists(HOLDINGS_DIR):
        os.makedirs(HOLDINGS_DIR)
        print(f"Created holdings data directory at {HOLDINGS_DIR}")

def fetch_and_save_fund_list():
    """
    Fetches the list of all funds from Akshare and updates data/funds.csv.
    Preserves 'status' and 'last_updated' columns if the file already exists.
    """
    ensure_data_dir_structure()
    
    print("Fetching fund list from Akshare...")
    try:
        # Fetch new list
        new_df = ak.fund_name_em()
        if new_df.empty:
            print("Fetched empty fund list from Akshare.")
            return pd.DataFrame()
        
        # Ensure '基金代码' is string
        new_df['基金代码'] = new_df['基金代码'].astype(str)

        # Check for existing file to preserve status
        if os.path.exists(FUNDS_LIST_PATH):
            print(f"Updating existing fund list at {FUNDS_LIST_PATH}...")
            existing_df = pd.read_csv(FUNDS_LIST_PATH, dtype={'基金代码': str})
            
            # Columns to preserve
            if 'status' in existing_df.columns:
                # Merge logic: Left join new list with existing status
                # We want the latest basic info (new_df) but keep old status
                status_df = existing_df[['基金代码', 'status', 'last_updated']].dropna(subset=['status'])
                merged_df = pd.merge(new_df, status_df, on='基金代码', how='left')
            else:
                merged_df = new_df
                merged_df['status'] = 'unknown'
                merged_df['last_updated'] = None
        else:
            print("Creating new fund list...")
            merged_df = new_df
            merged_df['status'] = 'unknown'
            merged_df['last_updated'] = None

        # Fill NaN status for new funds
        merged_df['status'] = merged_df['status'].fillna('unknown')
        
        # Ensure last_updated is object type to avoid FutureWarning
        if 'last_updated' not in merged_df.columns:
             merged_df['last_updated'] = None
        merged_df['last_updated'] = merged_df['last_updated'].astype('object')

        # Save
        merged_df.to_csv(FUNDS_LIST_PATH, index=False, encoding='utf-8-sig')
        print(f"Saved {len(merged_df)} funds to {FUNDS_LIST_PATH}")
        return merged_df

    except Exception as e:
        print(f"Error fetching/saving fund list: {e}")
        return pd.DataFrame()

def update_fund_status(fund_code: str, is_valid: bool):
    """
    Updates the status of a specific fund in data/funds.csv.
    """
    if not os.path.exists(FUNDS_LIST_PATH):
        print("Fund list file not found. Please run fetch_and_save_fund_list() first.")
        return

    try:
        df = pd.read_csv(FUNDS_LIST_PATH, dtype={'基金代码': str})
        
        if fund_code not in df['基金代码'].values:
            print(f"Fund code {fund_code} not found in the list.")
            return

        status = 'valid' if is_valid else 'invalid'
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Update row
        df.loc[df['基金代码'] == fund_code, 'status'] = status
        df.loc[df['基金代码'] == fund_code, 'last_updated'] = current_time

        df.to_csv(FUNDS_LIST_PATH, index=False, encoding='utf-8-sig')
        print(f"Updated status for {fund_code}: {status}")

    except Exception as e:
        print(f"Error updating fund status: {e}")

def save_fund_nav_to_cache(fund_code: str, nav_df: pd.DataFrame):
    """Saves fund NAV data to cache."""
    ensure_data_dir_structure()
    file_path = os.path.join(NAV_DIR, f'{fund_code}.csv')
    nav_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"Saved NAV for {fund_code} to cache: {file_path}")

def load_fund_nav_from_cache(fund_code: str) -> pd.DataFrame:
    """Loads fund NAV data from cache."""
    file_path = os.path.join(NAV_DIR, f'{fund_code}.csv')
    if os.path.exists(file_path):
        print(f"Loading NAV for {fund_code} from cache: {file_path}")
        df = pd.read_csv(file_path)
        return df
    return pd.DataFrame()

def save_fund_holdings_to_cache(fund_code: str, year: int, holdings_df: pd.DataFrame):
    """Saves fund holdings data to cache."""
    ensure_data_dir_structure()
    file_path = os.path.join(HOLDINGS_DIR, f'{fund_code}_{year}.csv')
    holdings_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"Saved holdings for {fund_code} in {year} to cache: {file_path}")

def load_fund_holdings_from_cache(fund_code: str, year: int) -> pd.DataFrame:
    """Loads fund holdings data from cache."""
    file_path = os.path.join(HOLDINGS_DIR, f'{fund_code}_{year}.csv')
    if os.path.exists(file_path):
        print(f"Loading holdings for {fund_code} in {year} from cache: {file_path}")
        df = pd.read_csv(file_path)
        return df
    return pd.DataFrame()

def get_nav_last_date(fund_code: str) -> str:
    """
    Returns the latest date (YYYY-MM-DD) found in the cached NAV file.
    Returns None if file doesn't exist or is empty.
    """
    file_path = os.path.join(NAV_DIR, f'{fund_code}.csv')
    if not os.path.exists(file_path):
        return None
    
    try:
        # Read only the '净值日期' column to save memory/time? 
        # But pandas reads full file mostly.
        # Use header=0 and just read the relevant column if possible, but reading full is safe.
        df = pd.read_csv(file_path)
        if df.empty or '净值日期' not in df.columns:
            return None
            
        # Assuming sorted, but max is safer
        # Ensure datetime conversion
        dates = pd.to_datetime(df['净值日期'], errors='coerce')
        last_date = dates.max()
        
        if pd.isna(last_date):
            return None
            
        return last_date.strftime("%Y-%m-%d")
        
    except Exception as e:
        print(f"Error checking NAV last date for {fund_code}: {e}")
        return None

if __name__ == "__main__":
    ensure_data_dir_structure()
    fetch_and_save_fund_list()
    # Test update
    # update_fund_status("000001", True)
