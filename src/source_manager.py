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

def get_active_source(task_type: str) -> dict:
    """
    Returns the best available data source for a given task type (e.g., 'nav', 'holdings').
    Returns None if no valid source is found.
    """
    if not os.path.exists(SOURCES_FILE):
        init_sources_list()
    
    try:
        df = pd.read_csv(SOURCES_FILE)
        # Filter by type and valid status, sort by priority (1 is highest)
        candidates = df[
            (df['type'] == task_type) & 
            (df['status'] == 'valid')
        ].sort_values('priority')
        
        if not candidates.empty:
            return candidates.iloc[0].to_dict()
        return None
    except Exception as e:
        print(f"Error reading data sources: {e}")
        return None

def update_source_status(source_id: str, is_success: bool):
    """
    Updates the status of a data source based on fetch result.
    """
    if not os.path.exists(SOURCES_FILE):
        return

    try:
        df = pd.read_csv(SOURCES_FILE)
        if source_id in df['id'].values:
            idx = df[df['id'] == source_id].index[0]
            
            # Logic: If success, mark valid. 
            # If fail, we could mark invalid immediately, or implement a counter.
            # For this requirement, we update status based on validity.
            new_status = 'valid' if is_success else 'invalid'
            
            df.at[idx, 'status'] = new_status
            df.at[idx, 'last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            df.to_csv(SOURCES_FILE, index=False, encoding='utf-8-sig')
            if not is_success:
                print(f"Warning: Data source {source_id} marked as invalid.")
    except Exception as e:
        print(f"Error updating source status: {e}")

if __name__ == "__main__":
    init_sources_list()
    print("Active NAV source:", get_active_source('nav'))
