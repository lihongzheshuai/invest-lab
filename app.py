import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, date

from src.scraper import fetch_fund_info, fetch_fund_holdings, fetch_fund_nav, batch_fetch_holdings
from src.analyzer import analyze_position_changes, search_funds_by_stocks
from src.translations import get_text, translate_df_columns, translate_change_types
from src.data_manager import FUNDS_LIST_PATH, HOLDINGS_DIR, fetch_and_save_fund_list
from src.utils import get_latest_report_quarter

st.set_page_config(page_title=get_text('app_title'), layout="wide")

st.title(f"📈 {get_text('app_title')}")

# --- Initialize Session State ---
if 'selected_fund_type' not in st.session_state:
    st.session_state.selected_fund_type = "全部 / All"

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
# We force reload if '基金类型' is missing to support the new feature
funds_df = pd.DataFrame()
if os.path.exists(FUNDS_LIST_PATH):
    funds_df = pd.read_csv(FUNDS_LIST_PATH, dtype={'基金代码': str})
    
    if not funds_df.empty:
        # Construct Display Name
        if '基金类型' in funds_df.columns:
            funds_df['display_name'] = funds_df['基金简称'] + " (" + funds_df['基金代码'] + ") - " + funds_df['基金类型']
        else:
            funds_df['display_name'] = funds_df['基金简称'] + " (" + funds_df['基金代码'] + ")"

# Sidebar
st.sidebar.header(get_text('sidebar_header'))

# Mode Selection - "Overview" added as default
mode = st.sidebar.radio(
    "功能模式",
    [get_text('tab_overview'), get_text('tab_analysis'), get_text('tab_search')]
)

if mode == get_text('tab_overview'):
    st.subheader(get_text('tab_overview'))
    
    if not funds_df.empty:
        # 1. Total Count
        total_funds = len(funds_df)
        col1, col2 = st.columns([1, 3])
        with col1:
            st.metric(label=get_text('stat_total_funds'), value=total_funds)
            
            # Show update time if available
            if 'last_updated' in funds_df.columns:
                 last_update = funds_df['last_updated'].dropna().iloc[0] if not funds_df['last_updated'].dropna().empty else "N/A"
                 st.caption(f"上次更新: {last_update}")

        # 2. Type Distribution and Interaction
        if '基金类型' in funds_df.columns:
            type_counts = funds_df['基金类型'].value_counts().reset_index()
            type_counts.columns = ['类型', '数量']
            
            with col2:
                # Top 15 types to avoid clutter in pie chart
                top_types = type_counts.head(15)
                fig = px.pie(top_types, values='数量', names='类型', title=get_text('header_type_dist'), hole=0.4)
                st.plotly_chart(fig, use_container_width=True)
            
            # Standard Dataframe (Removed interactive 'on_select' to ensure stability)
            st.dataframe(
                type_counts, 
                height=300, 
                use_container_width=True
            )

            # --- Dropdown Filter ---
            
            # Prepare options for dropdown
            # specific fix: dropna() before unique() to avoid float(nan) vs str comparison error
            unique_types = funds_df['基金类型'].dropna().unique().tolist()
            all_types = ["全部 / All"] + sorted([str(t) for t in unique_types]) # Ensure all are strings just in case
            
            # Determine index for dropdown based on session state
            try:
                current_index = all_types.index(st.session_state.selected_fund_type)
            except ValueError:
                current_index = 0

            # Dropdown Filter
            # Removed 'key' to avoid session state conflicts, simplified logic
            selected_type = st.selectbox(
                "按类型筛选基金清单 / Filter Fund List by Type",
                all_types,
                index=current_index
            )
            
            # Update session state
            if selected_type != st.session_state.selected_fund_type:
                st.session_state.selected_fund_type = selected_type
                st.rerun() # Force rerun to ensure list updates immediately
            
            # --- Filter and Display Fund List ---
            
            if st.session_state.selected_fund_type == "全部 / All":
                filtered_funds = funds_df
            else:
                filtered_funds = funds_df[funds_df['基金类型'] == st.session_state.selected_fund_type]
            
            st.write(f"基金清单 ({len(filtered_funds)}):")
            st.dataframe(
                filtered_funds[['基金代码', '基金简称', '基金类型']], 
                use_container_width=True,
                hide_index=True
            )
            
        else:
            st.warning("未检测到基金类型数据，无法展示分布图。请检查数据源或更新基金列表。")
    else:
        st.warning(get_text('warn_no_funds_file'))
        if st.button("初始化基金列表"):
            with st.spinner("正在初始化..."):
                fetch_and_save_fund_list()
                st.rerun()

elif mode == get_text('tab_analysis'):
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
                # Extract code from display name, or filter df
                # display name format: Name (Code) - Type
                # Safest to filter df
                selected_row = funds_df[funds_df['display_name'] == selected_fund_display]
                if not selected_row.empty:
                    selected_fund_code = selected_row['基金代码'].iloc[0]
        else:
            st.sidebar.warning(get_text('warn_no_funds_file'))
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
            # fetch_fund_nav now automatically checks freshness and updates if needed
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

elif mode == get_text('tab_search'):
    st.header(get_text('tab_search'))
    
    # Calculate latest available quarter for default
    latest_year, latest_q = get_latest_report_quarter()
    
    # Year Selection for Search (Default to latest available year)
    year = st.sidebar.number_input(get_text('label_year'), min_value=2020, max_value=2025, value=latest_year)
    
    # Input
    stock_input = st.text_area(get_text('label_search_stocks'), height=100, placeholder="例如: 贵州茅台, 600519, 宁德时代")
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        search_clicked = st.button(get_text('btn_search'), type="primary")
        
    if search_clicked and stock_input:
        inputs = stock_input.split(',')
        with st.spinner(get_text('msg_fetching')):
            
            # Filter for "Stock Type" Funds
            filter_codes = None
            if not funds_df.empty and '基金类型' in funds_df.columns:
                # Logic: Type contains "股票" (Stock), "混合" (Mixed), or "指数" (Index)
                # This covers most funds that hold significant equity positions.
                mask = funds_df['基金类型'].astype(str).str.contains('股票|混合|指数', regex=True)
                allowed_df = funds_df[mask]
                filter_codes = allowed_df['基金代码'].tolist()
                
                st.caption(f"已在 {len(allowed_df)} 只股票/混合/指数型基金范围内进行搜索。")
            else:
                st.warning("未检测到基金类型信息，正在全量搜索（可能包含非股票型基金）。请更新基金列表。")
            
            results = search_funds_by_stocks(inputs, HOLDINGS_DIR, year, filter_fund_codes=filter_codes)
            
            if not results.empty:
                st.subheader(get_text('header_search_results'))
                
                # Merge with fund name if available
                if not funds_df.empty:
                    # funds_df has '基金代码' and '基金简称'
                    results['fund_code'] = results['fund_code'].astype(str)
                    merged = pd.merge(results, funds_df[['基金代码', '基金简称', '基金类型']], left_on='fund_code', right_on='基金代码', how='left')
                    merged['fund_name'] = merged['基金简称'].fillna(merged['fund_code'])
                    
                    # Reorder columns - Add Type!
                    display_df = merged[['fund_code', 'fund_name', '基金类型', 'match_count', 'match_degree', 'matched_stocks']]
                else:
                    display_df = results
                
                # Rename columns for display
                display_df = display_df.rename(columns={
                    'fund_code': get_text('label_fund_code'),
                    'fund_name': "基金名称",
                    '基金类型': "类型",
                    'match_count': get_text('col_match_count'),
                    'match_degree': get_text('col_match_degree'),
                    'matched_stocks': get_text('col_matched_stocks')
                })
                
                # Color styling
                st.dataframe(
                    display_df.style.background_gradient(subset=[get_text('col_match_degree')], cmap="Greens"),
                    use_container_width=True
                )
            else:
                st.info("未找到持有这些股票的基金，或本地数据为空。请尝试更新数据。")

    # Data Management Section
    with st.expander("数据管理 / Data Management"):
        st.write("如果查询结果为空，可能是本地没有最新的持仓数据。您可以批量更新。")
        st.write(f"当前预估最新财报季度: **{latest_year} Q{latest_q}**")
        st.write("注意：为了演示性能，默认仅更新 Top 50 热门基金（如有）或列表前 50 个。")
        
        if st.button(get_text('btn_update_data')):
            if funds_df.empty:
                st.error("未找到基金列表，无法更新。")
            else:
                # Select top 50
                targets = funds_df['基金代码'].head(50).tolist()
                
                # Use the latest available year
                update_year = latest_year
                
                progress_bar = st.progress(0, text=get_text('msg_updating'))
                
                def update_progress(i, total, msg):
                    progress_bar.progress((i + 1) / total, text=f"{msg} ({i+1}/{total})")
                
                batch_fetch_holdings(targets, update_year, progress_callback=update_progress)
                
                st.success(get_text('msg_update_complete', success=len(targets), total=len(targets)))