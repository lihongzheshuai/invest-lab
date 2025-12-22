import pandas as pd
import asyncio
import os
from src.analyzer import process_single_fund, search_funds_by_stocks_async

# Mock data
HOLDINGS_DIR = 'data/holdings'
YEAR = 2024
INPUTS = ['300750', '宁德时代'] # Test with CATL

# 1. Test process_single_fund for a known fund
async def test_single():
    print("Testing single fund (000001)...")
    # Ensure 000001_2024.csv exists or use one that exists
    # I'll check a few files first
    files = os.listdir(HOLDINGS_DIR)
    print(f"Found {len(files)} holding files.")
    if not files:
        print("No holding files to test.")
        return

    # Pick a real fund code from files
    code = files[0].split('_')[0]
    print(f"Testing code: {code}")
    
    sem = asyncio.Semaphore(1)
    res = await process_single_fund(code, YEAR, INPUTS, HOLDINGS_DIR, sem)
    print(f"Result for {code}: {res}")

# 2. Test async search with a small list
async def test_search():
    print("\nTesting search_funds_by_stocks_async...")
    # Get top 5 codes
    files = [f for f in os.listdir(HOLDINGS_DIR) if f.endswith(f'_{YEAR}.csv')]
    codes = [f.split('_')[0] for f in files[:10]]
    print(f"Testing codes: {codes}")
    
    df = await search_funds_by_stocks_async(INPUTS, HOLDINGS_DIR, YEAR, codes)
    print("Search Results:")
    print(df)
    
    if not df.empty:
        print("\nDuplicates check:")
        print(df['fund_code'].value_counts())

if __name__ == "__main__":
    from src.utils import run_async_loop
    run_async_loop(test_single())
    run_async_loop(test_search())
