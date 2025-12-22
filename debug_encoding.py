import pandas as pd
import os

FUNDS_LIST_PATH = 'data/funds.csv'

print(f"File size: {os.path.getsize(FUNDS_LIST_PATH)} bytes")

print("--- Attempt 1: utf-8-sig ---")
try:
    df = pd.read_csv(FUNDS_LIST_PATH, encoding='utf-8-sig')
    print("Success with utf-8-sig")
except Exception as e:
    print(f"Failed: {e}")

print("\n--- Attempt 2: gb18030 ---")
try:
    df = pd.read_csv(FUNDS_LIST_PATH, encoding='gb18030')
    print("Success with gb18030")
except Exception as e:
    print(f"Failed: {e}")

print("\n--- Attempt 3: ISO-8859-1 (fallback) ---")
try:
    df = pd.read_csv(FUNDS_LIST_PATH, encoding='ISO-8859-1')
    print("Success with ISO-8859-1")
    print(df.head())
except Exception as e:
    print(f"Failed: {e}")
