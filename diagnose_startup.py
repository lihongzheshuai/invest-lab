import pandas as pd
import os
import sys

print("--- Diagnostics Start ---")

def check_csv(path):
    print(f"Checking {path}...")
    if not os.path.exists(path):
        print("  File not found.")
        return
    
    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
        print(f"  Success (utf-8-sig). Rows: {len(df)}")
    except Exception as e:
        print(f"  FAIL (utf-8-sig): {e}")
        # Try fallback to see if it IS gbk
        try:
            df = pd.read_csv(path, encoding='gb18030')
            print("  But works with gb18030. File encoding mismatch!")
        except:
            print("  Fails with gb18030 too.")

check_csv('data/data_sources.csv')
check_csv('data/funds.csv')

print("\n--- Import Check ---")
try:
    from src.data_manager import load_fund_holdings_from_cache
    print("src.data_manager imported.")
except Exception as e:
    print(f"src.data_manager FAIL: {e}")

try:
    from src.source_manager import get_active_source
    print("src.source_manager imported.")
    # Test get_active_source
    src = get_active_source('nav')
    print(f"Active NAV source: {src}")
except Exception as e:
    print(f"src.source_manager FAIL: {e}")

try:
    from src.analyzer import search_funds_by_stocks_async
    print("src.analyzer imported.")
except Exception as e:
    print(f"src.analyzer FAIL: {e}")

print("--- Diagnostics End ---")
