import os
import pandas as pd
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
SOURCES_FILE = os.path.join(DATA_DIR, 'data_sources.csv')

# Initial configuration of our known data sources
INITIAL_SOURCES = [
    {
        "id": "akshare_eastmoney_nav",
        "name": "EastMoney NAV (Open Fund)",
        "chinese_name": "东方财富-开放式基金-历史净值",
        "url": "http://fund.eastmoney.com",
        "handler": "akshare_nav",
        "type": "nav",
        "status": "valid",
        "priority": 1,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    },
    {
        "id": "akshare_eastmoney_holdings",
        "name": "EastMoney Holdings",
        "chinese_name": "东方财富-开放式基金-持仓明细",
        "url": "http://fund.eastmoney.com",
        "handler": "akshare_holdings",
        "type": "holdings",
        "status": "valid",
        "priority": 1,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    },
    {
        "id": "akshare_eastmoney_etf_nav",
        "name": "EastMoney ETF NAV",
        "chinese_name": "东方财富-ETF基金-历史净值",
        "url": "http://fund.eastmoney.com",
        "handler": "akshare_etf_nav",
        "type": "nav",
        "status": "valid",
        "priority": 2,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    },
    {
        "id": "akshare_eastmoney_money_nav",
        "name": "EastMoney Money Fund NAV",
        "chinese_name": "东方财富-理财型基金-历史净值",
        "url": "http://fund.eastmoney.com",
        "handler": "akshare_money_nav",
        "type": "nav",
        "status": "valid",
        "priority": 2,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
]

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def init_sources_list():
    """Initializes the data sources CSV if it doesn't exist."""
    ensure_data_dir()
    # Always overwrite/create with initial sources to ensure schema update
    # In a real prod env, we might want to merge, but here we enforce the new schema
    df = pd.DataFrame(INITIAL_SOURCES)
    # Ensure correct column order
    cols = ['id', 'name', 'chinese_name', 'url', 'handler', 'type', 'status', 'priority', 'last_updated']
    df = df[cols]
    df.to_csv(SOURCES_FILE, index=False, encoding='utf-8-sig')
    print(f"Initialized data sources list at {SOURCES_FILE}")

def load_sources():
    """Load data sources configuration."""
    if not os.path.exists(SOURCES_FILE):
        init_sources_list()
        
    try:
        return pd.read_csv(SOURCES_FILE, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error loading data sources: {e}")
        return pd.DataFrame()

def save_sources(df):
    """Save data sources configuration."""
    try:
        df.to_csv(SOURCES_FILE, index=False, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error saving data sources: {e}")

def get_active_source(data_type: str):
    """
    Get the currently active source for a specific data type ('nav' or 'holdings').
    Returns a dictionary or None.
    Prioritizes sources with 'valid' status and lower priority number (1 is highest).
    """
    if not os.path.exists(SOURCES_FILE):
        init_sources_list()
        
    try:
        df = pd.read_csv(SOURCES_FILE, encoding='utf-8-sig')
            
        # Filter by type and status='valid'
        # Check if columns exist to be safe
        if 'status' not in df.columns or 'type' not in df.columns:
            return None
            
        active = df[(df['type'] == data_type) & (df['status'] == 'valid')]
        
        if not active.empty:
            # Sort by priority (if exists)
            if 'priority' in df.columns:
                active = active.sort_values(by='priority', ascending=True)
            return active.iloc[0].to_dict()
            
    except Exception as e:
        print(f"Error getting active source: {e}")
        
    return None

def update_source_status(source_id: str, is_success: bool):
    """
    Updates the status of a data source based on fetch result.
    """
    if not os.path.exists(SOURCES_FILE):
        return

    try:
        # Since I can't easily import _read_csv_robust from data_manager (circular?), use robust logic inline
        # Actually, let's just use pd.read_csv with try/except
        df = pd.read_csv(SOURCES_FILE, encoding='utf-8-sig')

        if source_id in df['id'].values:
            idx = df[df['id'] == source_id].index[0]
            
            if is_success:
                df.at[idx, 'status'] = 'valid'
            
            # If failed, we DO NOT mark invalid to prevent permanent lockout on transient errors.
            # We just log it.
            
            df.at[idx, 'last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            df.to_csv(SOURCES_FILE, index=False, encoding='utf-8-sig')
            if not is_success:
                print(f"Warning: Fetch failed for {source_id}, but status kept valid for retries.")
    except Exception as e:
        print(f"Error updating source status: {e}")

if __name__ == "__main__":
    init_sources_list()
    print("Active NAV source:", get_active_source('nav'))
