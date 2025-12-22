import pandas as pd
import json
import os
import time

DATA_DIR = 'data'
HOLDINGS_DIR = 'data/holdings'
REVERSE_INDEX = 'data/reverse_index.json'

print("--- Diagnostics ---")

# 1. Check Funds.csv types
if os.path.exists('data/funds.csv'):
    df = pd.read_csv('data/funds.csv', dtype={'基金代码': str})
    print(f"Funds sample code: '{df['基金代码'].iloc[0]}' (Type: {type(df['基金代码'].iloc[0])})")
else:
    print("Funds.csv missing")

# 2. Check Reverse Index
if os.path.exists(REVERSE_INDEX):
    with open(REVERSE_INDEX, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"Index Scanned Funds: {len(data['scanned_funds'])}")
    if data['scanned_funds']:
        print(f"Sample scanned: '{data['scanned_funds'][0]}'")
    
    idx_ts = data.get('timestamp', 0)
    print(f"Index Timestamp: {idx_ts}")
    
    if os.path.exists(HOLDINGS_DIR):
        dir_mtime = os.path.getmtime(HOLDINGS_DIR)
        print(f"Holdings Dir MTime: {dir_mtime}")
        print(f"Is Valid? {dir_mtime <= idx_ts}")
else:
    print("Reverse Index missing")

# 3. Check Holding File Types
files = os.listdir(HOLDINGS_DIR)
if files:
    fpath = os.path.join(HOLDINGS_DIR, files[0])
    # Read WITHOUT dtype to see default behavior
    df_raw = pd.read_csv(fpath)
    print(f"Reading {files[0]} raw:")
    if '股票代码' in df_raw.columns:
        print(f"Stock Code Raw: {df_raw['股票代码'].iloc[0]} (Type: {type(df_raw['股票代码'].iloc[0])})")
    
    # Read WITH dtype
    df_str = pd.read_csv(fpath, dtype={'股票代码': str})
    print(f"Reading {files[0]} with dtype str:")
    print(f"Stock Code Str: '{df_str['股票代码'].iloc[0]}' (Type: {type(df_str['股票代码'].iloc[0])})")
