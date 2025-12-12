import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.scraper import fetch_fund_info, fetch_fund_holdings, fetch_fund_nav
from src.analyzer import calculate_max_drawdown, calculate_annualized_return, analyze_position_changes, estimate_adjustment_success

st.set_page_config(page_title="Fund Investment Lab", layout="wide")

st.title("📈 Fund Investment Lab (A-Share)")

# Sidebar
st.sidebar.header("Configuration")
fund_code = st.sidebar.text_input("Fund Code", value="000248") # Example: China AMC Consumption
year = st.sidebar.number_input("Analysis Year", min_value=2020, max_value=2025, value=2024)

if st.sidebar.button("Analyze"):
    with st.spinner("Fetching Data..."):
        # 1. Basic Info
        info = fetch_fund_info(fund_code)
        st.header(f"Fund: {fund_code}")
        if not info.empty:
            st.dataframe(info)
        
        # 2. Performance Analysis
        st.subheader("Performance Metrics")
        nav_df = fetch_fund_nav(fund_code)
        if not nav_df.empty:
            # Clean and process
            nav_df['净值日期'] = pd.to_datetime(nav_df['净值日期'])
            nav_df = nav_df.sort_values('净值日期')
            nav_series = nav_df.set_index('净值日期')['单位净值'].astype(float)
            
            # Metrics
            max_dd = calculate_max_drawdown(nav_series)
            ann_ret = calculate_annualized_return(nav_series, len(nav_series))
            
            col1, col2 = st.columns(2)
            col1.metric("Max Drawdown", f"{max_dd:.2%}")
            col2.metric("Annualized Return", f"{ann_ret:.2%}")
            
            # Chart
            fig = px.line(nav_df, x='净值日期', y='单位净值', title='Historical NAV')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No NAV data found.")
            
        # 3. Portfolio Analysis
        st.subheader("Portfolio Changes")
        
        # Comparison between Q1 and Q2 of selected year (Example)
        # In a real app, we might let user select quarters or show all quarters evolution.
        # Let's try to fetch Q1 and Q2 for the selected year.
        
        q1_date = f"{year}-03-31" # Approximate check date, Akshare uses 'year' usually
        q1_holdings = fetch_fund_holdings(fund_code, year)
        
        # Akshare returns all quarters for the year in one DF usually? 
        # Let's check the scraper return. It returns `fund_portfolio_hold_em(date=str(year))`
        # which usually returns the latest reports for that year? Or requires specific filtering?
        # IMPORTANT: 'fund_portfolio_hold_em' with year usually returns the list of holdings for that year's quarters.
        # We need to filter by quarter if the DF has it.
        
        if not q1_holdings.empty and '季度' in q1_holdings.columns:
            # Sort by quarter
            quarters = sorted(q1_holdings['季度'].unique())
            st.write(f"Available Quarters: {quarters}")
            
            if len(quarters) >= 2:
                q_prev_str = quarters[-2]
                q_curr_str = quarters[-1]
                
                st.write(f"Comparing {q_prev_str} vs {q_curr_str}")
                
                h_prev = q1_holdings[q1_holdings['季度'] == q_prev_str]
                h_curr = q1_holdings[q1_holdings['季度'] == q_curr_str]
                
                changes = analyze_position_changes(h_prev, h_curr)
                
                st.write("### Position Changes")
                st.dataframe(changes)
                
                # Mock success calculation
                # Detailed stock price fetching is needed for real success rate.
                success_rate = estimate_adjustment_success(changes, {}) 
                st.metric("Est. Adjustment Success Rate", f"{success_rate:.0%}", help="Mocked data - needs stock prices")
            else:
                st.info("Not enough quarters to compare in this year.")
                st.dataframe(q1_holdings)
        else:
            st.warning("No holdings data or '季度' column missing.")
