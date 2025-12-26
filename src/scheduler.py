import pandas as pd
import akshare as ak
import os
from datetime import datetime
from src.data_manager import FUNDS_LIST_PATH, load_fund_holdings_from_cache
from src.scraper import batch_fetch_holdings

def get_latest_online_quarter(sample_funds=['000001', '110011', '005827']):
    """
    Checks a few major funds to see what the latest available quarter is online.
    Returns (year, quarter) tuple, e.g., (2025, 1).
    """
    current_year = datetime.now().year
    
    # Check current year and previous year (in case of Jan 1st)
    years_to_check = [current_year]
    if datetime.now().month <= 4:
        years_to_check.append(current_year - 1)
        
    for year in years_to_check:
        for code in sample_funds:
            try:
                # Fetch summary for the year
                df = ak.fund_portfolio_hold_em(symbol=code, date=str(year))
                if not df.empty and '季度' in df.columns:
                    # Value ex: "2025年1季度股票投资明细"
                    # We look for the latest quarter string
                    # Sort strings? "2025年1季度" < "2025年2季度"
                    quarters = sorted(df['季度'].unique().tolist(), reverse=True)
                    if quarters:
                        latest_q_str = quarters[0]
                        # Parse "YYYY年N季度"
                        try:
                            y_str = latest_q_str[:4]
                            q_str = latest_q_str.split('年')[1].split('季度')[0]
                            return int(y_str), int(q_str)
                        except:
                            continue
            except Exception as e:
                # print(f"Check failed for {code}: {e}")
                continue
    
    # Fallback default
    return current_year, 1

def check_update_needed():
    """
    Determines if a global cache update is required by comparing online availability with local cache.
    Returns: (bool, year) - (True if update needed, Target Year)
    """
    print("Checking for new quarterly data...")
    
    # 1. Get Latest Online
    online_y, online_q = get_latest_online_quarter()
    print(f"Latest online data detected: {online_y} Q{online_q}")
    
    # 2. Check Local Cache of sample funds
    # We check if we have this quarter for the sample funds.
    # If ANY sample fund is missing this quarter in cache, we assume update is needed.
    sample_funds = ['000001', '110011'] 
    
    needs_update = False
    
    for code in sample_funds:
        cached = load_fund_holdings_from_cache(code, online_y)
        
        if cached.empty:
            print(f"Cache missing for sample fund {code} in {online_y}. Update needed.")
            needs_update = True
            break
        
        # Check if '季度' column contains the target quarter
        # We constructed target_str matching the online format
        target_str = f"{online_y}年{online_q}季度"
        
        if '季度' not in cached.columns:
            needs_update = True
            break
            
        has_quarter = cached['季度'].astype(str).str.contains(target_str).any()
        
        if not has_quarter:
             print(f"Cache for {code} exists but is missing {target_str}. Update needed.")
             needs_update = True
             break
    
    return needs_update, online_y

def run_smart_update():
    """
    Executes the update if needed.
    """
    needed, year = check_update_needed()
    if needed:
        print(f"New data detected. Starting batch update for year {year}...")
        
        # Load fund list
        if os.path.exists(FUNDS_LIST_PATH):
            try:
                funds_df = pd.read_csv(FUNDS_LIST_PATH, dtype={'基金代码': str})
                all_codes = funds_df['基金代码'].tolist()
                
                print(f"Updating holdings for {len(all_codes)} funds...")
                
                # Helper for progress
                def progress(current, total, msg):
                    if current % 10 == 0:
                        print(f"[{current}/{total}] {msg}")
                
                # Batch Fetch
                batch_fetch_holdings(all_codes, year, progress_callback=progress)
                print("Global cache update complete.")
                
            except Exception as e:
                print(f"Error loading fund list: {e}")
        else:
            print("Fund list not found. Please initialize app first.")
    else:
        print("Cache is up-to-date (Latest online quarter matches local). No update needed.")

if __name__ == "__main__":
    run_smart_update()
