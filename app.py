import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime

from src.scraper import fetch_fund_info, fetch_fund_holdings, fetch_fund_nav
from src.analyzer import analyze_position_changes
from src.translations import get_text, translate_df_columns, translate_change_types
from src.data_manager import FUNDS_LIST_PATH # Import FUNDS_LIST_PATH

st.set_page_config(page_title=get_text('app_title'), layout="wide")

st.title(f"📈 {get_text('app_title')}")

# --- Helper for Quarter Date Range ---
def get_quarter_date_range(year, quarter):
    if quarter == 1:
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 3, 31)
    elif quarter == 2:
        start_date = datetime(year, 4, 1)
        end_date = datetime(year, 6, 30)
    elif quarter == 3:
        start_date = datetime(year, 7, 1)
        end_date = datetime(year, 9, 30)
    elif quarter == 4:
        start_date = datetime(year, 10, 1)
        end_date = datetime(year, 12, 31)
    else:
        return None, None # Invalid quarter
    return start_date, end_date

# --- Load Fund List for Selection ---
funds_df = pd.DataFrame()
if os.path.exists(FUNDS_LIST_PATH):
    funds_df = pd.read_csv(FUNDS_LIST_PATH, dtype={'基金代码': str})
    funds_df['display_name'] = funds_df['基金简称'] + " (" + funds_df['基金代码'] + ")"

# Sidebar
st.sidebar.header(get_text('sidebar_header'))

# Fund Selection Method
selection_method = st.sidebar.radio(
    get_text('label_fund_selection_method'),
    [get_text('option_select_fund_name'), get_text('option_enter_fund_code')]
)

selected_fund_code = ""
if selection_method == get_text('option_select_fund_name'):
    if not funds_df.empty:
        fund_options = funds_df['display_name'].tolist()
        # Add a default empty selection to prevent immediate trigger on first load
        fund_options.insert(0, "--请选择基金--")
        selected_fund_display = st.sidebar.selectbox(get_text('label_select_fund'), fund_options, index=0)
        if selected_fund_display != "--请选择基金--":
            selected_fund_code = funds_df[funds_df['display_name'] == selected_fund_display]['基金代码'].iloc[0]
    else:
        st.sidebar.warning(get_text('warn_no_funds_file')) # New translation key needed
        selected_fund_code = st.sidebar.text_input(get_text('label_enter_code'), value="000248")
elif selection_method == get_text('option_enter_fund_code'):
    selected_fund_code = st.sidebar.text_input(get_text('label_enter_code'), value="000248")

fund_code_to_analyze = selected_fund_code.strip()

# Year and Quarter Selection
year = st.sidebar.number_input(get_text('label_year'), min_value=2020, max_value=2025, value=2024)

quarter_map = {
    get_text('quarter_q1'): 1,
    get_text('quarter_q2'): 2,
    get_text('quarter_q3'): 3,
    get_text('quarter_q4'): 4
}
quarter_label = st.sidebar.selectbox(get_text('label_quarter'), list(quarter_map.keys()), index=3) # Default Q4
curr_q = quarter_map[quarter_label]

# Analyze Button - Only enable if fund_code_to_analyze is not empty
if st.sidebar.button(get_text('btn_analyze'), disabled=(not fund_code_to_analyze)):
    with st.spinner(get_text('msg_fetching')):
        # 1. Basic Info - Retained for fund name display in header
        info = fetch_fund_info(fund_code_to_analyze)
        st.header(get_text('header_fund', fund_code=fund_code_to_analyze))
        if not info.empty:
            st.dataframe(info)
        
        # 2. Net Asset Value (NAV) Trend
        st.subheader(get_text('chart_nav_title'))
        nav_df = fetch_fund_nav(fund_code_to_analyze)
        if not nav_df.empty:
            nav_df['净值日期'] = pd.to_datetime(nav_df['净值日期'])
            nav_df = nav_df.sort_values('净值日期')

            # Highlight current selected quarter
            start_date_highlight, end_date_highlight = get_quarter_date_range(year, curr_q)
            
            fig = px.line(nav_df, x='净值日期', y='单位净值', title=get_text('chart_nav_title'))
            
            if start_date_highlight and end_date_highlight:
                fig.add_vrect(x0=start_date_highlight, x1=end_date_highlight, 
                              fillcolor="LightSalmon", opacity=0.4, line_width=0, 
                              annotation_text=f"{year} Q{curr_q}", annotation_position="top left")
            
            st.plotly_chart(fig, width='stretch')
        else:
            st.warning(get_text('warn_no_nav'))
            
        # 3. Current Quarter Holdings and Changes
        st.subheader(get_text('header_portfolio'))
        
        # Calculate Previous Quarter
        if curr_q == 1:
            prev_year = year - 1
            prev_q = 4
        else:
            prev_year = year
            prev_q = curr_q - 1
            
        # Fetch Data
        df_curr_year = fetch_fund_holdings(fund_code_to_analyze, year)
        
        if prev_year != year:
            df_prev_year = fetch_fund_holdings(fund_code_to_analyze, prev_year)
        else:
            df_prev_year = df_curr_year
            
        # Helper to filter quarter
        def get_quarter_data(df, y, q):
            if df.empty or '季度' not in df.columns:
                return pd.DataFrame()
            mask = df['季度'].astype(str).str.contains(f"{y}年{q}季度")
            return df[mask]

        # Extract specific quarters
        h_curr = get_quarter_data(df_curr_year, year, curr_q)
        h_prev = get_quarter_data(df_prev_year, prev_year, prev_q)
        
        # Display Logic
        if not h_curr.empty:
            st.write(f"**{get_text('text_target_quarter', quarter=f'{year} Q{curr_q}')}**")
            st.dataframe(translate_df_columns(h_curr))
            
            if not h_prev.empty:
                st.write(f"**{get_text('text_prev_quarter', quarter=f'{prev_year} Q{prev_q}')}**")
                
                changes = analyze_position_changes(h_prev, h_curr)
                
                st.write(f"### {get_text('header_changes')}")
                
                if not changes.empty:
                    display_changes = changes.copy()
                    display_changes['change_type'] = translate_change_types(display_changes['change_type'])
                    display_changes = translate_df_columns(display_changes)
                    st.dataframe(display_changes)
                else:
                    st.info(get_text('info_no_changes')) 
                
            else:
                st.warning(get_text('warn_no_data_prev', year=prev_year, quarter=f"Q{prev_q}"))
        else:
            st.warning(get_text('warn_no_data_current', year=year, quarter=f"Q{curr_q}"))
            if not df_curr_year.empty and '季度' in df_curr_year.columns:
                st.write(get_text('text_quarters', quarters=sorted(df_curr_year['季度'].unique())))
