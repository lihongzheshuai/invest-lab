import streamlit as st
import pandas as pd
import plotly.express as px
import os
import asyncio
from datetime import datetime, date

from src.scraper import fetch_fund_info, fetch_fund_holdings, fetch_fund_nav, batch_fetch_holdings, fetch_fund_estimation_batch
from src.analyzer import analyze_position_changes, search_funds_by_stocks, search_funds_by_stocks_async, check_cache_coverage, query_reverse_index_direct, load_reverse_index
from src.translations import get_text, translate_df_columns, translate_change_types
from src.data_manager import FUNDS_LIST_PATH, HOLDINGS_DIR, fetch_and_save_fund_list, load_favorites, add_favorite, remove_favorites
from src.utils import get_latest_report_quarter, run_async_loop
from src.stocks.stocks import get_limit_up_model

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
    try:
        funds_df = pd.read_csv(FUNDS_LIST_PATH, dtype={'基金代码': str}, encoding='utf-8-sig')
    except Exception as e:
        st.error(f"无法读取基金列表文件: {e}")
    
    if not funds_df.empty:
        # Construct Display Name
        if '基金类型' in funds_df.columns:
            funds_df['display_name'] = funds_df['基金简称'] + " (" + funds_df['基金代码'] + ") - " + funds_df['基金类型']
        else:
            funds_df['display_name'] = funds_df['基金简称'] + " (" + funds_df['基金代码'] + ")"

# --- Custom CSS for Styled Tabs ---
st.markdown("""
<style>
    /* Style the Main Title */
    h1 {
        font-size: 2.0rem !important;
        padding-bottom: 1rem;
    }

    /* Style the Tab Labels */
    button[data-baseweb="tab"] div p {
        font-size: 1.2rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# --- Top Level Menu (Tabs) ---
tab_names = [
    f"📊 {get_text('tab_overview')}", 
    f"🔍 {get_text('tab_analysis')}", 
    f"📈 {get_text('tab_search')}"
]
tab_overview, tab_analysis, tab_search = st.tabs(tab_names)

# ==========================================
# Tab 1: Overview
# ==========================================
with tab_overview:
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
            
            # --- Pagination Controls ---
            col_p1, col_p2, col_p3 = st.columns([1, 1, 3])
            
            with col_p1:
                page_size = st.selectbox("每页显示 / Per Page", [20, 50, 100], index=0)
            
            total_rows = len(filtered_funds)
            total_pages = (total_rows // page_size) + (1 if total_rows % page_size > 0 else 0)
            
            with col_p2:
                page_number = st.number_input("页码 / Page", min_value=1, max_value=max(1, total_pages), value=1)
                
            # Calculate Slice
            start_idx = (page_number - 1) * page_size
            end_idx = min(start_idx + page_size, total_rows)
            
            # Display Slice
            st.caption(f"显示第 {start_idx + 1} 到 {end_idx} 条，共 {total_rows} 条")
            
            current_page_df = filtered_funds.iloc[start_idx:end_idx]
            
            st.dataframe(
                current_page_df[['基金代码', '基金简称', '基金类型']], 
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

# ==========================================
# Tab 2: Analysis
# ==========================================
with tab_analysis:
    # --- Favorites Section ---
    with st.expander("❤️ 我的自选基金 / My Favorites", expanded=False):
        fav_df = load_favorites()
        if not fav_df.empty:
            # Control Bar
            c_fav_1, c_fav_2, c_fav_dummy = st.columns([1, 1, 4])
            
            if c_fav_1.button("🔄 刷新估值 / Refresh Est."):
                with st.spinner("Fetching real-time data..."):
                    est_df = fetch_fund_estimation_batch(fav_df['基金代码'].tolist())
                    if not est_df.empty:
                        st.session_state['fav_estimation'] = est_df
                    else:
                        st.warning("Failed to fetch estimation.")
            
            # Data Preparation
            fav_display = fav_df.copy()
            fav_display['基金代码'] = fav_display['基金代码'].astype(str)
            
            # Merge Estimation
            if 'fav_estimation' in st.session_state:
                est_data = st.session_state['fav_estimation']
                est_data['基金代码'] = est_data['基金代码'].astype(str)
                fav_display = pd.merge(fav_display, est_data, on='基金代码', how='left')
            
            # Links
            def get_fund_url(code):
                return f"http://fund.eastmoney.com/{code}.html"
            
            fav_display['代码_URL'] = fav_display['基金代码'].apply(get_fund_url)
            fav_display['名称_URL'] = fav_display.apply(lambda x: f"{x['代码_URL']}#{x['基金名称']}", axis=1)
            
            # Columns
            cols = ['代码_URL', '名称_URL', '基金类型', '估算净值', '估算涨幅', '加入时间']
            cols = [c for c in cols if c in fav_display.columns]
            
            # Display
            event_fav = st.dataframe(
                fav_display[cols],
                column_config={
                    "代码_URL": st.column_config.LinkColumn("基金代码", display_text=r"http://fund\.eastmoney\.com/(\d+)\.html"),
                    "名称_URL": st.column_config.LinkColumn("基金名称", display_text=r".*#(.*)"),
                    "估算涨幅": st.column_config.TextColumn("估算涨幅"), # Keep as text to preserve color/sign if any
                },
                use_container_width=True,
                hide_index=True,
                selection_mode="multi-row",
                on_select="rerun",
                key="fav_table"
            )
            
            # Remove Action
            if event_fav.selection.rows:
                if c_fav_2.button("🗑️ 移除选中 / Remove"):
                    codes_to_remove = fav_display.iloc[event_fav.selection.rows]['基金代码'].tolist()
                    remove_favorites(codes_to_remove)
                    st.rerun()
        else:
            st.info("暂无收藏基金。请在分析或搜索结果中添加。/ No favorites yet.")

    # --- Control Panel (Moved from Sidebar) ---
    with st.container():
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        
        with c1:
            # Fund Selection Method
            selection_method = st.radio(
                get_text('label_fund_selection_method'),
                [get_text('option_select_fund_name'), get_text('option_enter_fund_code')],
                horizontal=True,
                label_visibility="collapsed"
            )

            selected_fund_code = ""
            if selection_method == get_text('option_select_fund_name'):
                if not funds_df.empty:
                    fund_options = funds_df['display_name'].tolist()
                    fund_options.insert(0, "--请选择基金--")
                    selected_fund_display = st.selectbox(get_text('label_select_fund'), fund_options, index=0)
                    if selected_fund_display != "--请选择基金--":
                        selected_row = funds_df[funds_df['display_name'] == selected_fund_display]
                        if not selected_row.empty:
                            selected_fund_code = selected_row['基金代码'].iloc[0]
                else:
                    st.warning(get_text('warn_no_funds_file'))
                    selected_fund_code = st.text_input(get_text('label_enter_code'), value="000248")
            elif selection_method == get_text('option_enter_fund_code'):
                selected_fund_code = st.text_input(get_text('label_enter_code'), value="000248")

            fund_code_to_analyze = selected_fund_code.strip()

        with c2:
            # Year
            year = st.number_input(get_text('label_year'), min_value=2020, max_value=2025, value=2024)

        with c3:
            # Quarter
            quarter_map = {
                get_text('quarter_q1'): 1,
                get_text('quarter_q2'): 2,
                get_text('quarter_q3'): 3,
                get_text('quarter_q4'): 4
            }
            quarter_label = st.selectbox(get_text('label_quarter'), list(quarter_map.keys()), index=3) # Default Q4
            curr_q = quarter_map[quarter_label]

        with c4:
            # Analyze Button
            st.write("") # Spacer
            st.write("") # Spacer
            analyze_clicked = st.button(get_text('btn_analyze'), disabled=(not fund_code_to_analyze), use_container_width=True)

    st.divider()

    # Analyze Logic
    if analyze_clicked or fund_code_to_analyze: 
        if analyze_clicked:
            st.session_state.analyzing_fund = fund_code_to_analyze
            st.session_state.analyzing_year = year
            st.session_state.analyzing_q = curr_q
        
        # Check if we have an active analysis in session or if inputs are valid (auto-show)
        # We prioritize session state if button was clicked to lock in the view, 
        # but also allow dynamic updates if the user just changes dropdowns?
        # Actually, let's strictly follow the 'analyze_clicked' pattern or session state.
        
        pass

    if 'analyzing_fund' in st.session_state and st.session_state.analyzing_fund:
        f_code = st.session_state.analyzing_fund
        y = st.session_state.analyzing_year
        q = st.session_state.analyzing_q
        
        with st.spinner(get_text('msg_fetching')):
            # 1. Basic Info - Retained for fund name display in header
            info = fetch_fund_info(f_code)
            
            # Header & Favorite Button
            c_head, c_btn = st.columns([6, 1])
            c_head.header(get_text('header_fund', fund_code=f_code))
            
            if c_btn.button("❤️ 收藏"):
                # Resolve Name and Type
                f_name = f_code
                f_type = "N/A"
                
                # Try from funds_df (Global list)
                if not funds_df.empty:
                    match = funds_df[funds_df['基金代码'] == f_code]
                    if not match.empty:
                        f_name = match['基金简称'].iloc[0]
                        f_type = match['基金类型'].iloc[0]
                
                # Fallback to info df
                if f_name == f_code and not info.empty and '基金简称' in info.columns:
                    f_name = info['基金简称'].iloc[0]
                    
                if add_favorite(f_code, f_name, f_type):
                    st.toast(f"✅ 已收藏 {f_name}")
                else:
                    st.toast(f"ℹ️ {f_name} 已在收藏列表中")

            if not info.empty:
                st.dataframe(info)
            
            # 2. Net Asset Value (NAV) Trend
            st.subheader(get_text('chart_nav_title'))
            nav_df = fetch_fund_nav(f_code)
            if not nav_df.empty:
                nav_df['净值日期'] = pd.to_datetime(nav_df['净值日期'])
                nav_df = nav_df.sort_values('净值日期')

                # Highlight current selected quarter
                start_date_highlight, end_date_highlight = get_quarter_date_range(y, q)
                
                fig = px.line(nav_df, x='净值日期', y='单位净值', title=get_text('chart_nav_title'))
                
                if start_date_highlight and end_date_highlight:
                    fig.add_vrect(x0=start_date_highlight, x1=end_date_highlight, 
                                  fillcolor="LightSalmon", opacity=0.4, line_width=0, 
                                  annotation_text=f"{y} Q{q}", annotation_position="top left")
                
                st.plotly_chart(fig, width='stretch')
            else:
                st.warning(get_text('warn_no_nav'))
                
            # 3. Current Quarter Holdings and Changes
            st.subheader(get_text('header_portfolio'))
            
            # Calculate Previous Quarter
            if q == 1:
                prev_year = y - 1
                prev_q = 4
            else:
                prev_year = y
                prev_q = q - 1
                
            # Fetch Data
            df_curr_year = fetch_fund_holdings(f_code, y)
            
            if prev_year != y:
                df_prev_year = fetch_fund_holdings(f_code, prev_year)
            else:
                df_prev_year = df_curr_year
                
            # Helper to filter quarter
            def get_quarter_data(df, year_val, q_val):
                if df.empty or '季度' not in df.columns:
                    return pd.DataFrame()
                mask = df['季度'].astype(str).str.contains(f"{year_val}年{q_val}季度")
                return df[mask]

            # Extract specific quarters
            h_curr = get_quarter_data(df_curr_year, y, q)
            h_prev = get_quarter_data(df_prev_year, prev_year, prev_q)
            
            # Display Logic
            if not h_curr.empty:
                st.write(f"**{get_text('text_target_quarter', quarter=f'{y} Q{q}')}**")
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
                st.warning(get_text('warn_no_data_current', year=y, quarter=f"Q{q}"))
                if not df_curr_year.empty and '季度' in df_curr_year.columns:
                    st.write(get_text('text_quarters', quarters=sorted(df_curr_year['季度'].unique())))

# ==========================================
# Tab 3: Search
# ==========================================
with tab_search:
    st.header(get_text('tab_search'))

    # --- Limit Up Stocks Feature (Added) ---
    with st.expander("🔥 每日涨停股池 (辅助填入) / Daily Limit-Up Stocks Helper", expanded=False):
        if st.button("获取/刷新 今日涨停股 / Fetch Limit-Up Stocks"):
             with st.spinner("正在获取涨停数据... / Fetching data..."):
                 try:
                     df_limit = get_limit_up_model()
                     st.session_state['limit_up_df'] = df_limit
                 except Exception as e:
                     st.error(f"获取失败 / Failed: {e}")
        
        if 'limit_up_df' in st.session_state and not st.session_state['limit_up_df'].empty:
             st.caption("👇 勾选下方股票，可一键复制或填入搜索框 / Select stocks below to copy or auto-fill:")
             st.caption("💡 **提示**: 点击表头第一列复选框可**全选**；点击列名可**排序** (排序状态不会因选择而重置)。")
             
             # Access Session State Data
             limit_df_raw = st.session_state['limit_up_df']
             df_display = limit_df_raw.copy()
             
             # Rename '所属行业' to '所属板块'
             if '所属行业' in df_display.columns:
                 df_display = df_display.rename(columns={'所属行业': '所属板块'})
             
             # --- 1. Block Filter (Industry) ---
             if '所属板块' in df_display.columns:
                 # Calculate counts
                 block_counts = df_display['所属板块'].value_counts()
                 # Sort by count desc
                 all_blocks = block_counts.sort_values(ascending=False).index.tolist()
                 
                 def format_block_label(option):
                     return f"{option} ({block_counts.get(option, 0)})"

                 selected_blocks = st.pills(
                     "🔍 按板块筛选 (支持多选) / Filter by Block:", 
                     all_blocks, 
                     selection_mode="multi", 
                     format_func=format_block_label,
                     key="pills_block"
                 )
                 
                 if selected_blocks:
                     df_display = df_display[df_display['所属板块'].isin(selected_blocks)]
            
             # --- 2. Concept Filter (Dynamic) ---
             if '所属概念' in df_display.columns:
                 # Generate concepts from currently visible data (Faceted Search)
                 current_concepts_raw = df_display['所属概念'].fillna("").astype(str)
                 
                 # Count concepts
                 concept_list = []
                 for s in current_concepts_raw:
                     if s:
                         concept_list.extend(s.split(";"))
                 
                 if concept_list:
                     concept_counts = pd.Series(concept_list).value_counts()
                     all_concepts = concept_counts.sort_values(ascending=False).index.tolist()
                     
                     def format_concept_label(option):
                         return f"{option} ({concept_counts.get(option, 0)})"
     
                     selected_concepts = st.pills(
                         "🏷️ 按概念筛选 (支持多选) / Filter by Concept:", 
                         all_concepts, 
                         selection_mode="multi", 
                         format_func=format_concept_label,
                         key="pills_concept"
                     )
                     
                     if selected_concepts:
                         # Filter: Match ANY selected concept
                         def match_concepts(row_concepts):
                             if not row_concepts: return False
                             rc = str(row_concepts)
                             return any(sc in rc for sc in selected_concepts)
                             
                         df_display = df_display[df_display['所属概念'].apply(match_concepts)]

             # --- Prepare Display Data ---
             def get_em_url(code):
                 if str(code).startswith(('6', '9')): prefix = "sh"
                 elif str(code).startswith(('0', '3')): prefix = "sz"
                 elif str(code).startswith(('8', '4')): prefix = "bj"
                 else: prefix = "sz"
                 return f"http://quote.eastmoney.com/{prefix}{code}.html"

             # Update URL columns
             df_display['代码_URL'] = df_display['代码'].apply(get_em_url)
             df_display['名称_URL'] = df_display.apply(lambda x: f"{x['代码_URL']}#{x['名称']}", axis=1)
             
             # Ensure numeric types
             if '最新价' in df_display.columns:
                 df_display['最新价'] = pd.to_numeric(df_display['最新价'], errors='coerce')
             
             # Columns to show
             # Added '所属概念'
             cols = ['日期', '代码_URL', '名称_URL', '所属板块', '所属概念', '最新价', '换手率', '最后封板时间', '连板数']
             cols_to_show = [c for c in cols if c in df_display.columns]

             # Render Dataframe
             event = st.dataframe(
                 df_display[cols_to_show],
                 hide_index=True,
                 use_container_width=True,
                 on_select="rerun",
                 selection_mode="multi-row",
                 key="limit_up_selector",
                 column_config={
                     "代码_URL": st.column_config.LinkColumn(
                         "代码", display_text=r"http://quote\.eastmoney\.com/[a-z]{2}(\d+)\.html"
                     ),
                     "名称_URL": st.column_config.LinkColumn(
                         "名称", display_text=r".*#(.*)"
                     ),
                     "所属概念": st.column_config.TextColumn("所属概念", width="medium"),
                     "最后封板时间": st.column_config.TextColumn("最后封板时间"),
                     "最新价": st.column_config.NumberColumn("最新价", format="%.2f"),
                     "换手率": st.column_config.NumberColumn("换手率", format="%.2f%%"),
                 }
             )
             
             # Process Selection
             selected_rows = event.selection.rows
             if selected_rows:
                 # Use index to retrieve Name from df_display
                 selected_names = df_display.iloc[selected_rows]['名称'].tolist()
                 names_str = ",".join(selected_names)
                 
                 st.caption(f"✅ 已选 {len(selected_names)} 只股票 / Selected:")
                 st.code(names_str, language="text")
                 
                 if st.button("⬇️ 一键填入搜索框 / Fill Search Box"):
                     st.session_state.search_stocks_input = names_str
                     st.rerun()
    
    # Calculate latest available quarter for default
    latest_year, latest_q = get_latest_report_quarter()
    
    # Input - Full Width
    # Ensure key is initialized if not present to avoid errors if accessed before widget creation? 
    # Streamlit handles initialization if key is in widget.
    stock_input = st.text_area(
        get_text('label_search_stocks'), 
        height=100, 
        placeholder="例如: 贵州茅台, 600519, 宁德时代", 
        key="search_stocks_input"
    )
    
    # We use latest_year as the fixed year for search
    year = latest_year
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        search_clicked = st.button(get_text('btn_search'), type="primary")
        
    # --- Search Execution Logic (Smart Resume) ---
    if search_clicked and stock_input:
        inputs = stock_input.split(',')
        
        # 1. Filter Scope
        filter_codes = []
        if not funds_df.empty and '基金类型' in funds_df.columns:
            mask = funds_df['基金类型'].astype(str).str.contains('股票|偏股|指数', regex=True)
            allowed_df = funds_df[mask]
            filter_codes = allowed_df['基金代码'].tolist()
        else:
            if not funds_df.empty:
                filter_codes = funds_df['基金代码'].tolist()

        if not filter_codes:
            st.error("没有可供搜索的基金列表。")
        else:
            # 2. Smart Resume: Split Cached vs Pending
            index_data = load_reverse_index()
            scanned_set = set(index_data.get('scanned_funds', []))
            target_set = set(filter_codes)
            
            cached_codes = list(target_set.intersection(scanned_set))
            pending_codes = list(target_set.difference(scanned_set))
            
            # 3. Retrieve Cached Results Immediately
            accumulated_results = []
            if cached_codes:
                cached_df = query_reverse_index_direct(inputs, cached_codes)
                if not cached_df.empty:
                    accumulated_results = cached_df.to_dict('records')
                    
                    # Pre-display cached results while scanning continues
                    results_df = cached_df.copy()
                    if not funds_df.empty:
                        results_df['fund_code'] = results_df['fund_code'].astype(str)
                        merged = pd.merge(results_df, funds_df[['基金代码', '基金简称', '基金类型']], left_on='fund_code', right_on='基金代码', how='left')
                        merged['fund_name'] = merged['基金简称'].fillna(merged['fund_code'])
                        if 'quarter' not in merged.columns: merged['quarter'] = 'N/A'
                        display_df = merged[['fund_code', 'fund_name', '基金类型', 'quarter', 'match_count', 'match_degree', 'matched_stocks']]
                    else:
                        display_df = results_df
                    
                    # Formatting & Links
                    display_df = display_df.rename(columns={
                        'fund_code': get_text('label_fund_code'),
                        'fund_name': "基金名称",
                        '基金类型': "类型",
                        'quarter': "报告期",
                        'match_count': get_text('col_match_count'),
                        'match_degree': get_text('col_match_degree'),
                        'matched_stocks': get_text('col_matched_stocks')
                    })
                    code_col = get_text('label_fund_code')
                    name_col = "基金名称"
                    display_df[code_col] = display_df[code_col].apply(lambda x: f"http://fund.eastmoney.com/{x}.html")
                    display_df[name_col] = display_df.apply(lambda r: f"http://fund.eastmoney.com/{r[code_col].split('/')[-1]}#{r[name_col]}", axis=1)
                    
                    st.session_state['search_results_df'] = display_df
                    st.toast(f"✅ 已加载 {len(cached_codes)} 条缓存记录")
            
            # 4. Init Search Loop for Pending
            st.session_state.search_results_accumulated = accumulated_results
            st.session_state.search_inputs = inputs
            st.session_state.search_year = year
            
            if pending_codes:
                st.session_state.search_running = True
                st.session_state.search_paused = False
                st.session_state.search_queue = pending_codes
                st.session_state.search_all_codes = filter_codes
                st.session_state.search_total = len(pending_codes) # Track progress of NEW work
                st.rerun()
            else:
                st.session_state.search_running = False
                st.success("✅ 搜索完成 (全部命中缓存)")
                if not accumulated_results:
                     st.session_state['search_results_df'] = pd.DataFrame()
                     st.info("未找到匹配结果")

    # --- Running Loop ---
    if st.session_state.get('search_running'):
        total = st.session_state.search_total
        remaining = len(st.session_state.search_queue)
        done = total - remaining
        
        # Progress
        st.progress(done / total, text=f"正在搜索... {done}/{total} (已找到 {len(st.session_state.search_results_accumulated)} 个匹配)")
        
        # Controls
        c1, c2 = st.columns([1, 4])
        stop = c1.button("停止 / Stop ⏹️")
        pause = c2.button("暂停 / Pause ⏸️" if not st.session_state.search_paused else "继续 / Resume ▶️")
        
        if stop:
            st.session_state.search_running = False
            st.warning("搜索已停止。显示部分结果。")
            st.rerun()
            
        if pause:
            st.session_state.search_paused = not st.session_state.search_paused
            st.rerun()
            
        # Process Chunk
        if not st.session_state.search_paused and st.session_state.search_queue:
            chunk_size = 20
            chunk = st.session_state.search_queue[:chunk_size]
            st.session_state.search_queue = st.session_state.search_queue[chunk_size:]
            
            try:
                partial_results = run_async_loop(search_funds_by_stocks_async(
                    st.session_state.search_inputs,
                    HOLDINGS_DIR,
                    st.session_state.search_year,
                    filter_fund_codes=chunk
                ))
                
                if not partial_results.empty:
                    st.session_state.search_results_accumulated.extend(partial_results.to_dict('records'))
            except Exception as e:
                st.error(f"Error in chunk: {e}")
                
            st.rerun()
            
        elif not st.session_state.search_queue:
            # Finished
            st.session_state.search_running = False
            st.success("搜索完成！")
            st.rerun()

    # --- Convert Accumulated to Display DF ---
    # We do this if 'search_results_df' is empty AND we have accumulated data
    # OR if we just stopped/finished (detected by running=False but accumulated exists?)
    # Simplest: If running is False and accumulated has data, update df.
    # But we want to avoid re-calculating on every unrelated interaction.
    # We'll rely on the fact that we set search_results_df = DataFrame() on init.
    # So if accumulated is present and df is empty (and not running), we convert.
    
    if (not st.session_state.get('search_running') 
        and st.session_state.get('search_results_accumulated') 
        and (st.session_state.get('search_results_df') is None or st.session_state.get('search_results_df').empty)):
        
        results_df = pd.DataFrame(st.session_state.search_results_accumulated)
        if not results_df.empty:
            # Deduplicate by fund_code
            results_df.drop_duplicates(subset=['fund_code'], inplace=True)
            
            # Process for display
            if not funds_df.empty:
                results_df['fund_code'] = results_df['fund_code'].astype(str)
                merged = pd.merge(results_df, funds_df[['基金代码', '基金简称', '基金类型']], left_on='fund_code', right_on='基金代码', how='left')
                merged['fund_name'] = merged['基金简称'].fillna(merged['fund_code'])
                if 'quarter' not in merged.columns: merged['quarter'] = 'N/A'
                display_df = merged[['fund_code', 'fund_name', '基金类型', 'quarter', 'match_count', 'match_degree', 'matched_stocks']]
            else:
                display_df = results_df
            
            # Formatting
            display_df = display_df.rename(columns={
                'fund_code': get_text('label_fund_code'),
                'fund_name': "基金名称",
                '基金类型': "类型",
                'quarter': "报告期",
                'match_count': get_text('col_match_count'),
                'match_degree': get_text('col_match_degree'),
                'matched_stocks': get_text('col_matched_stocks')
            })
            
            # Links
            code_col = get_text('label_fund_code')
            name_col = "基金名称"
            
            # Code column
            display_df[code_col] = display_df[code_col].apply(lambda x: f"http://fund.eastmoney.com/{x}.html")
            # Name column with #Name hack
            display_df[name_col] = display_df.apply(lambda r: f"http://fund.eastmoney.com/{r[code_col].split('/')[-1]}#{r[name_col]}", axis=1)
            
            st.session_state['search_results_df'] = display_df
            st.session_state['search_year_cached'] = st.session_state.search_year
            
            # Clear accumulated to prevent re-processing? No, keep for reference? 
            # Or just set a flag "processed". 
            # Better: The check `st.session_state.get('search_results_df').empty` handles it.
            # If we populated df, it's not empty. So we won't re-enter.



    # Render Results from Session State
    if 'search_results_df' in st.session_state and not st.session_state['search_results_df'].empty:
        display_df = st.session_state['search_results_df']
        cached_year = st.session_state.get('search_year_cached', year) # Fallback to current input year if missing
        
        st.subheader(get_text('header_search_results'))
        
        # --- Pagination for Search Results ---
        c_p1, c_p2, c_p3 = st.columns([1, 1, 3])
        with c_p1:
            sp_size = st.selectbox("每页显示", [10, 20, 50], key="search_page_size")
        
        s_total = len(display_df)
        s_pages = (s_total // sp_size) + (1 if s_total % sp_size > 0 else 0)
        
        with c_p2:
            sp_num = st.number_input("页码", min_value=1, max_value=max(1, s_pages), value=1, key="search_page_num")
            
        s_start = (sp_num - 1) * sp_size
        s_end = min(s_start + sp_size, s_total)
        
        st.caption(f"显示第 {s_start + 1} 到 {s_end} 条，共 {s_total} 条")
        
        page_df = display_df.iloc[s_start:s_end]

        # Interactive Dataframe
        code_col = get_text('label_fund_code')
        name_col = "基金名称"
        
        event = st.dataframe(
            page_df.style.background_gradient(subset=[get_text('col_match_degree')], cmap="Greens"),
            use_container_width=True,
            on_select="rerun",
            selection_mode="multi-row",
            key="search_result_table",
            column_config={
                code_col: st.column_config.LinkColumn(
                    code_col,
                    display_text=r"http://fund\.eastmoney\.com/(\d+)\.html"
                ),
                name_col: st.column_config.LinkColumn(
                    name_col,
                    display_text=r".*#(.*)"
                )
            }
        )
        
        # --- Batch Actions ---
        if event.selection.rows:
            if st.button("❤️ 将选中基金加入收藏 / Batch Favorite"):
                sel_rows = event.selection.rows
                selected_items = page_df.iloc[sel_rows]
                count = 0
                for _, row in selected_items.iterrows():
                    # Extract Code from URL
                    try:
                        c_url = row[code_col]
                        code = c_url.split('/')[-1].replace('.html', '')
                    except: continue
                    
                    # Extract Name from URL
                    try:
                        n_url = row[name_col]
                        name = n_url.split('#')[-1]
                    except: name = code
                    
                    ftype = row['类型'] if '类型' in row else "Unknown"
                    
                    if add_favorite(code, name, ftype):
                        count += 1
                st.toast(f"✅ 已添加 {count} 只基金到收藏")
        
        # --- Detail View on Selection (Show first) ---
        if len(event.selection.rows) > 0:
            selected_idx = event.selection.rows[0]
            # sel_fund_code is now a URL in the dataframe
            sel_url = page_df.iloc[selected_idx][code_col]
            # Extract code: http://fund.eastmoney.com/000001.html -> 000001
            try:
                sel_fund_code = sel_url.split('/')[-1].replace('.html', '')
            except:
                sel_fund_code = "" # Fallback
            
            if sel_fund_code:
                st.divider()
                st.subheader(f"🔍 基金详情: {sel_fund_code}")
                
                with st.spinner("正在加载详情..."):
                    # 1. Info
                    info = fetch_fund_info(sel_fund_code)
                    st.write("**基本信息**")
                    st.dataframe(info, use_container_width=True)
                    
                    # 2. NAV Chart
                    nav_df = fetch_fund_nav(sel_fund_code)
                    if not nav_df.empty:
                        nav_df['净值日期'] = pd.to_datetime(nav_df['净值日期'])
                        nav_df = nav_df.sort_values('净值日期')
                        fig = px.line(nav_df, x='净值日期', y='单位净值', title=f"{sel_fund_code} 历史净值走势")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # 3. Holdings
                    st.write(f"**持仓明细 ({cached_year}年)**")
                    holdings = fetch_fund_holdings(sel_fund_code, cached_year)
                    if not holdings.empty:
                        st.dataframe(translate_df_columns(holdings), use_container_width=True)
                    else:
                        st.warning("暂无持仓数据")