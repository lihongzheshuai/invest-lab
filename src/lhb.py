import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from src.stocks.stocks import enrich_with_concepts

def get_daily_lhb(date_str: str = None) -> pd.DataFrame:
    """
    Fetch daily Dragon and Tiger List (LHB) data.
    Enrich with Industry and Concept.
    
    Args:
        date_str: "YYYYMMDD". If None, defaults to today.
    """
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
        
    print(f"Fetching LHB data for {date_str}...")
    try:
        # Use EM source for detail daily
        # Updated to use stock_lhb_detail_em with start/end date
        df = ak.stock_lhb_detail_em(start_date=date_str, end_date=date_str)
        if df.empty:
            print("No LHB data found.")
            return pd.DataFrame()
            
        # Standardize columns
        # EM columns usually match: ['序号', '代码', '名称', '解读', '收盘价', '涨跌幅', '龙虎榜净买额', '龙虎榜买入额', '龙虎榜卖出额', '龙虎榜成交额', '市场总成交额', '净买额占总成交比', '成交额占总成交比', '换手率', '上榜原因', '上榜日']
        
        if '代码' not in df.columns:
            print(f"Warning: '代码' column not found in LHB data. Available columns: {df.columns.tolist()}")
            # Attempt to identify code column if possible, or skip enrichment
            return df

        # Enrich
        df = enrich_with_concepts(df)
        
        # Add date column if not present or inconsistent
        df['日期'] = date_str

        # Convert monetary columns to Ten Thousand (Wan) and round to 2 decimals
        money_cols = ['龙虎榜净买额', '龙虎榜买入额', '龙虎榜卖出额', '龙虎榜成交额', '市场总成交额']
        for col in money_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = (df[col] / 10000).round(2)
        
        return df
    except Exception as e:
        print(f"Error fetching LHB data: {e}")
        return pd.DataFrame()

def get_lhb_hot_money(date_str: str = None) -> pd.DataFrame:
    """
    Fetch active business departments (Hot Money) on LHB.
    Returns DataFrame with columns: ['营业部名称', '上榜次数', '累积买入额', '买入相关个股', '累积卖出额', '卖出相关个股', '净买入额']
    """
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
        
    print(f"Fetching Hot Money data for {date_str}...")
    try:
        # Use EM source as Sina might be unavailable
        df = ak.stock_lhb_hyyyb_em(start_date=date_str, end_date=date_str)
        
        if df.empty:
            return pd.DataFrame()

        # Rename columns to match expected schema
        rename_map = {
            '买入总金额': '累积买入额',
            '卖出总金额': '累积卖出额',
            '总买卖净额': '净买入额',
            '买入股票': '买入相关个股'
        }
        df = df.rename(columns=rename_map)

        # Ensure '上榜次数' exists (EM doesn't return it for daily range, maybe '买入个股数' + '卖出个股数'?)
        # For simplicity, we can ignore or set to 1 if not present, or use '买入个股数'
        if '上榜次数' not in df.columns:
            if '买入个股数' in df.columns:
                 df['上榜次数'] = df['买入个股数'] # Proxy
            else:
                 df['上榜次数'] = 1
            
        # Clean numeric columns
        numeric_cols = ['上榜次数', '累积买入额', '累积卖出额', '净买入额']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert monetary columns to Ten Thousand (Wan) and round to 2 decimals
        money_cols = ['累积买入额', '累积卖出额', '净买入额']
        for col in money_cols:
            if col in df.columns:
                df[col] = (df[col] / 10000).round(2)
                
        return df
    except Exception as e:
        print(f"Error fetching Hot Money data: {e}")
        return pd.DataFrame()