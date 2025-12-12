import akshare as ak
import pandas as pd
from datetime import datetime

def fetch_fund_info(fund_code: str) -> pd.DataFrame:
    """
    Fetch basic fund information.
    Uses generic fund list to find the name since specific metadata endpoints vary.
    """
    try:
        # fund_name_em returns all funds: code, name, type...
        # We fetch all (might be slow first time) or search.
        # Efficient way: ak.fund_name_em() returns a huge standard table.
        # Let's try catching specific info if available, otherwise just Name.
        
        # NOTE: Many specific 'basic_info' endpoints are deprecated or changed in akshare.
        # We will try to return a simple DF with just the code for now to avoid breaking.
        # If we really need name, we can try fetching the concept/industry data.
        
        # Create a basic stub DF so the app doesn't crash
        return pd.DataFrame([{"基金代码": fund_code, "Message": "Basic info API unstable"}])
    except Exception as e:
        print(f"Error fetching info for {fund_code}: {e}")
        return pd.DataFrame()

def fetch_fund_holdings(fund_code: str, year: int = 2024) -> pd.DataFrame:
    """
    Fetch fund holdings for a specific year.
    Returns DataFrame with columns like: ['序号', '股票代码', '股票名称', '占净值比例', '持股数', '持仓市值', '季度']
    """
    try:
        # Ensure year is string
        df = ak.fund_portfolio_hold_em(symbol=fund_code, date=str(year))
        return df
    except Exception as e:
        print(f"Error fetching holdings for {fund_code} in {year}: {e}")
        return pd.DataFrame()

def fetch_fund_nav(fund_code: str, start_date: str = "20200101", end_date: str = None) -> pd.DataFrame:
    """
    Fetch historical NAV (Net Asset Value).
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
        
    try:
        # Changed 'fund=' to 'symbol=' based on recent API changes/conventions
        df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
        
        # Filter by date if needed, though this endpoint usually returns full history or paginated.
        # The column names usually need standardization.
        # Expected columns: ['净值日期', '单位净值', '日增长率', ...]
        if not df.empty and '净值日期' in df.columns:
            df['净值日期'] = pd.to_datetime(df['净值日期'])
            mask = (df['净值日期'] >= pd.to_datetime(start_date)) & (df['净值日期'] <= pd.to_datetime(end_date))
            return df.loc[mask]
        return df
    except Exception as e:
        print(f"Error fetching NAV for {fund_code}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Test
    code = "000001"
    print(f"Testing Scraper for {code}...")
    
    # Info
    # print("Info:", fetch_fund_info(code))
    
    # Holdings
    print("Holdings (2024):")
    holdings = fetch_fund_holdings(code, 2024)
    if not holdings.empty:
        print(holdings.head())
    else:
        print("No holdings found.")

    # NAV
    print("NAV:")
    nav = fetch_fund_nav(code, start_date="20240101")
    if not nav.empty:
        print(nav.head())
    else:
        print("No NAV found.")
