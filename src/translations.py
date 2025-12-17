# Internationalization strings
TRANS = {
    'zh_CN': {
        # UI Structure
        'app_title': "基金投资实验室 (A股)",
        'sidebar_header': "配置",
        'label_fund_code': "基金代码",
        'label_year': "分析年份",
        'label_quarter': "选择季度",
        'quarter_q1': "第一季度 (Q1)",
        'quarter_q2': "第二季度 (Q2)",
        'quarter_q3': "第三季度 (Q3)",
        'quarter_q4': "第四季度 (Q4)",
        'btn_analyze': "开始分析",
        'msg_fetching': "正在获取数据...",
        
        # Headers
        'header_fund': "基金概况: {fund_code}",
        'header_perf': "业绩指标",
        'header_portfolio': "持仓变动分析",
        'header_changes': "持仓变动明细",
        
        # Metrics
        'metric_max_dd': "最大回撤",
        'metric_ann_ret': "年化收益率",
        'metric_success_rate': "预估调仓成功率",
        'help_success_rate': "模拟数据 - 需要股价信息",
        
        # Charts
        'chart_nav_title': "历史净值趋势",
        
        # Messages
        'text_quarters': "可用季度: {quarters}",
        'text_target_quarter': "当前季度持仓: {quarter}",
        'text_prev_quarter': "对比季度持仓: {quarter}",
        'text_compare': "对比季度: {q_prev} vs {q_curr}",
        'info_not_enough_quarters': "该年份没有足够的季度数据进行对比。",
        'warn_no_nav': "未找到净值数据。",
        'warn_no_holdings': "未找到持仓数据或数据格式错误（缺少'季度'列）。",
        'warn_no_data_current': "未找到 {year}年 {quarter} 的持仓数据。",
        'warn_no_data_prev': "未找到上一季度 ({year}年 {quarter}) 的持仓数据，无法计算变动。",
        'label_fund_selection_method': "基金选择方式",
        'option_select_fund_name': "按名称选择",
        'option_enter_fund_code': "直接输入代码",
        'label_select_fund': "选择基金名称",
        'label_enter_code': "输入基金代码",
        'warn_no_funds_file': "未找到基金列表文件，请手动输入基金代码。",
        'info_no_changes': "未检测到持仓变动或数据不匹配。",
        
        # DataFrame Columns (Analyzer output)
        'col_stock_code': "股票代码",
        'col_stock_name': "股票名称",
        'col_mv_prev': "上期持仓市值",
        'col_mv_curr': "本期持仓市值",
        'col_change_type': "变动类型",
        'col_diff': "变动金额",
        
        # Values (Change Types)
        'val_new': "新增",
        'val_delete': "清仓",
        'val_increase': "加仓",
        'val_decrease': "减仓",
        'val_unchanged': "未变",
        
        # Akshare Raw Data Columns (If fallback needed, though usually already Chinese)
        'col_nav_date': "净值日期",
        'col_nav_unit': "单位净值"
    }
}

def get_text(key, lang='zh_CN', **kwargs):
    """
    Retrieve translated text for a given key.
    Supports format placeholders (e.g., {fund_code}).
    """
    t_dict = TRANS.get(lang, TRANS['zh_CN'])
    text = t_dict.get(key, key) # Fallback to key if not found
    
    if kwargs:
        try:
            return text.format(**kwargs)
        except KeyError as e:
            return text # Return unformatted if keys mismatch
    return text

def translate_df_columns(df, lang='zh_CN'):
    """
    renames dataframe columns based on translation dictionary
    """
    t_dict = TRANS.get(lang, TRANS['zh_CN'])
    # Invert mapping? No, we need Map: English_Key -> Chinese_Value
    # But our dict keys are 'col_mv_prev'. We need a map from 'mv_prev' -> '上期持仓市值'
    
    # Let's define the specific column map for analyzer output
    col_map = {
        'mv_prev': t_dict['col_mv_prev'],
        'mv_curr': t_dict['col_mv_curr'],
        'change_type': t_dict['col_change_type'],
        'diff': t_dict['col_diff'],
        '股票代码': t_dict['col_stock_code'],
        '股票名称': t_dict['col_stock_name']
    }
    return df.rename(columns=col_map)

def translate_change_types(series, lang='zh_CN'):
    """
    Translates the values in the change_type column.
    """
    t_dict = TRANS.get(lang, TRANS['zh_CN'])
    val_map = {
        'NEW': t_dict['val_new'],
        'DELETE': t_dict['val_delete'],
        'INCREASE': t_dict['val_increase'],
        'DECREASE': t_dict['val_decrease'],
        'UNCHANGED': t_dict['val_unchanged']
    }
    return series.map(val_map).fillna(series)
