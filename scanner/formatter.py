from typing import Dict, List, Optional, Any
import pandas as pd
from scanner.strategy_guide import (
    get_strategy_playbook,
    format_playbook_html,
    format_playbook_text,
    format_playbook_compact_badge_html,
    format_all_playbooks_appendix_html,
)

MAX_DISPLAY_PER_STRATEGY = 5


def _calculate_confluence(results: Dict[str, pd.DataFrame]) -> Dict[str, List[str]]:
    """
    跨所有策略统计标的命中频次，识别多策略共振标的。
    """
    symbol_to_strats: Dict[str, List[str]] = {}
    for strat_name, df in results.items():
        if df is not None and not df.empty and "Symbol" in df.columns:
            for s in df["Symbol"].dropna().astype(str).str.strip().unique():
                if s:
                    symbol_to_strats.setdefault(s, []).append(strat_name)
    return symbol_to_strats


def _calc_key_price_levels(strategy_name: str, close_val: Any) -> Dict[str, str]:
    """
    根据策略类型与当前收盘价，自动化计算关键位（支撑/买点锚位）与严格止损防守位。
    """
    try:
        val_str = str(close_val).replace("$", "").replace(",", "").strip()
        c = float(val_str)
    except Exception:
        return {"support": "--", "stop_loss": "--"}

    name_lower = strategy_name.lower()
    if "compounder" in name_lower:
        # 欧奈尔价值成长：-7% 机械硬止损，MA20 支撑
        supp = f"${c * 0.95:.2f} (MA20)"
        sl = f"${c * 0.93:.2f} (-7.0%)"
    elif any(k in name_lower for k in ["pocket", "newhigh", "volumesurge"]):
        # 动量突破类：突破平台颈线防守，-4.5% 止损
        supp = f"${c * 0.96:.2f} (突破颈线)"
        sl = f"${c * 0.955:.2f} (-4.5%)"
    elif "penny" in name_lower:
        # 仙股系列：以点火阳线实体中轴或 -5% 为绝对底线
        supp = f"${c * 0.95:.2f} (实体中轴)"
        sl = f"${c * 0.95:.2f} (-5.0%)"
    else:
        # 稳健波段与底背离抄底策略
        supp = f"${c * 0.96:.2f} (波段支撑)"
        sl = f"${c * 0.96:.2f} (-4.0%)"

    return {"support": supp, "stop_loss": sl}


def _format_executive_summary_html(
    results: Dict[str, pd.DataFrame],
    strategy_metadata: Dict[str, str],
    market_regime: Optional[Any],
    confluence_map: Dict[str, List[str]],
) -> str:
    """
    生成邮件顶部的【⚡ 晨读 30 秒操盘速览 (Executive Summary)】卡片，
    涵盖大势定调、仓位建议、大类资产主线、Top 3~5 核心共振焦点池与风控备忘。
    """
    regime_name = getattr(market_regime, "regime_name", "多头进攻期") if market_regime else "顺风多头进攻期"
    temp = getattr(market_regime, "temperature", "75°C (活跃)") if market_regime else "75°C"
    bias_color = getattr(market_regime, "bias_color", "#10b981") if market_regime else "#10b981"
    narrative = getattr(market_regime, "macro_narrative", "") if market_regime else ""

    if "进攻" in regime_name:
        suggested_pos = "60% ~ 80% (积极进攻)"
    elif "超买" in regime_name:
        suggested_pos = "30% ~ 50% (谨慎兑现，防洗盘)"
    elif "恐慌" in regime_name:
        suggested_pos = "10% ~ 20% (极轻仓防守)"
    else:
        suggested_pos = "40% ~ 60% (精选抗跌逆势标的)"

    # 挑选 Top 3~5 只核心焦点股：共振标的优先，其次价值成长与口袋支点
    priority_symbols: List[str] = []
    confluent_syms = [s for s, strats in confluence_map.items() if len(strats) >= 2]
    confluent_syms.sort(key=lambda s: len(confluence_map[s]), reverse=True)
    for s in confluent_syms:
        if s not in priority_symbols:
            priority_symbols.append(s)

    # 其次添加 GrowthCompounder
    gc_df = results.get("GrowthCompounderStrategy", pd.DataFrame())
    if gc_df is not None and not gc_df.empty and "Symbol" in gc_df.columns:
        for s in gc_df["Symbol"].dropna().tolist():
            if s not in priority_symbols:
                priority_symbols.append(s)

    # 再次添加 PocketPivot
    pp_df = results.get("PocketPivotStrategy", pd.DataFrame())
    if pp_df is not None and not pp_df.empty and "Symbol" in pp_df.columns:
        for s in pp_df["Symbol"].dropna().tolist():
            if s not in priority_symbols:
                priority_symbols.append(s)

    # 兜底补充其他策略标的
    for strat, df in results.items():
        if df is not None and not df.empty and "Symbol" in df.columns:
            for s in df["Symbol"].dropna().tolist():
                if s not in priority_symbols:
                    priority_symbols.append(s)

    top_picks = priority_symbols[:5]

    top_picks_html_list = []
    for s in top_picks:
        strats = confluence_map.get(s, [])
        first_strat = strats[0] if strats else ""
        close_display = ""
        for st in strats:
            df = results.get(st, pd.DataFrame())
            if not df.empty and "Symbol" in df.columns and "Close" in df.columns:
                match = df[df["Symbol"] == s]
                if not match.empty:
                    close_display = str(match.iloc[0]["Close"])
                    break

        levels = _calc_key_price_levels(first_strat, close_display)

        if len(strats) >= 2:
            tag_badge = f"<span style='background-color: #fee2e2; color: #dc2626; border: 1px solid #fca5a5; font-size: 11px; font-weight: 700; padding: 1px 6px; border-radius: 4px;'>🔥 {len(strats)}重共振</span>"
        else:
            strat_desc = strategy_metadata.get(first_strat, first_strat).split(" ")[0].replace("策略", "")
            tag_badge = f"<span style='background-color: #e0f2fe; color: #0369a1; border: 1px solid #bae6fd; font-size: 11px; font-weight: 600; padding: 1px 6px; border-radius: 4px;'>{strat_desc}</span>"

        item = f"""
        <div style="margin: 5px 0; padding: 7px 12px; background-color: #ffffff; border: 1px solid #e2e8f0; border-radius: 6px; font-size: 12.5px; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;">
          <div>
            <b style="color: #0f172a; font-size: 13.5px;">{s}</b> 
            <span style="color: #475569; font-weight: 600; margin-left: 4px;">{close_display}</span>
            <span style="margin-left: 8px;">{tag_badge}</span>
          </div>
          <div style="color: #64748b; font-size: 12px;">
            <span>支撑/买点: <b style="color: #334155;">{levels['support']}</b></span> &nbsp;|&nbsp;
            <span>止损参考: <b style="color: #dc2626;">{levels['stop_loss']}</b></span>
          </div>
        </div>
        """
        top_picks_html_list.append(item)

    if not top_picks_html_list:
        picks_content = "<div style='color: #64748b; font-style: italic; padding: 6px 0; font-size: 12.5px;'>今日全市场各策略严格过滤，未出现高确定性标的，建议保持耐心观察，现金防守。</div>"
    else:
        picks_content = "".join(top_picks_html_list)

    narrative_line = ""
    if narrative:
        narrative_line = f"<div style='margin-bottom: 8px; font-size: 12.5px; color: #0f172a; line-height: 1.5;'>🌐 <b>全球跨市场主线：</b><span style='color: #0369a1;'>{narrative}</span></div>"

    html = f"""
<div id="top" style="background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%); border: 1px solid #cbd5e1; border-left: 6px solid #2563eb; border-radius: 8px; padding: 14px 18px; margin-bottom: 22px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
  <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; border-bottom: 1px solid #e2e8f0; padding-bottom: 8px;">
    <span style="font-size: 15px; font-weight: 800; color: #1e3a8a;">⚡ 晨读 30 秒操盘速览 (Executive Summary)</span>
    <span style="font-size: 11.5px; color: #64748b;">早晨极速决策池</span>
  </div>
  <div style="margin-bottom: 8px; font-size: 13px; color: #334155;">
    🧭 <b>大势定调：</b><span style="color: {bias_color}; font-weight: 700;">{regime_name}</span> &nbsp;|&nbsp; 
    <b>情绪温度：</b>{temp} &nbsp;|&nbsp; 
    <b>建议仓位水位：</b><span style="color: #047857; font-weight: 700;">{suggested_pos}</span>
  </div>
  {narrative_line}
  <div style="margin-top: 10px;">
    <div style="font-size: 13px; font-weight: 700; color: #0f172a; margin-bottom: 6px;">🎯 今日 Top 核心共振/精选焦点池：</div>
    {picks_content}
  </div>
  <div style="margin-top: 10px; padding: 6px 10px; background-color: #fef2f2; border: 1px solid #fee2e2; border-radius: 4px; font-size: 11.5px; color: #991b1b;">
    ⚠️ <b>晨间风控纪律备忘：</b>无论盘面多亢奋，坚决以各标的指定止损位作为铁律防守，严禁在未确认企稳前逆势扛单！
  </div>
</div>
"""
    return html.strip()


def _format_strategy_table_html(
    df: pd.DataFrame,
    strategy_name: str,
    confluence_map: Dict[str, List[str]],
) -> str:
    """
    通用策略表格渲染器，具备以下特性：
    1. 限制输出最多展示 Top 5 标的，超量时输出友好截断提示；
    2. 自动标记跨策略【🔥 X重共振】徽章；
    3. 自动计算并输出【关键位与止损参考】列；
    4. 对 GrowthCompounderStrategy 维持翡翠绿尊贵配色，对其他策略维持高对比度雅致浅蓝设计。
    """
    if df.empty:
        return ""

    total_count = len(df)
    df_display = df.head(MAX_DISPLAY_PER_STRATEGY).copy()

    is_compounder = "compounder" in strategy_name.lower()
    th_bg = "linear-gradient(135deg, #065f46 0%, #047857 100%)" if is_compounder else "#f1f5f9"
    th_color = "#ffffff" if is_compounder else "#334155"
    border_color = "#d1fae5" if is_compounder else "#e2e8f0"

    headers = [col for col in df_display.columns]
    has_levels_col = "关键位/止损参考" in headers

    th_cells = [
        f"<th style='background: {th_bg}; color: {th_color}; padding: 8px 12px; font-size: 12.5px; border: 1px solid {border_color}; text-align: left;'>{h}</th>"
        for h in headers
    ]
    if not has_levels_col:
        th_cells.append(
            f"<th style='background: {th_bg}; color: {th_color}; padding: 8px 12px; font-size: 12.5px; border: 1px solid {border_color}; text-align: left;'>关键位 / 止损防守</th>"
        )

    rows_html = []
    for idx, row in df_display.iterrows():
        bg = "#f0fdf4" if is_compounder and idx % 2 == 1 else ("#f8fafc" if idx % 2 == 1 else "#ffffff")
        td_cells = []
        symbol_val = str(row.get("Symbol", ""))
        close_val = row.get("Close", "")

        for col in headers:
            val = str(row[col])
            if col == "Symbol":
                confluent_strats = confluence_map.get(symbol_val, [])
                confluence_badge = ""
                if len(confluent_strats) >= 2:
                    confluence_badge = f"<span style='background-color: #fee2e2; color: #dc2626; border: 1px solid #fca5a5; font-size: 10.5px; font-weight: 700; padding: 1px 5px; border-radius: 4px; margin-left: 6px;'>🔥 {len(confluent_strats)}重共振</span>"
                
                color_sym = "#065f46" if is_compounder else "#0f172a"
                td_cells.append(
                    f"<td style='padding: 8px 12px; border: 1px solid {border_color}; font-weight: 700; color: {color_sym}; font-size: 13.5px;'>{val}{confluence_badge}</td>"
                )
            elif col == "Setup":
                if "突破" in val:
                    badge = f"<span style='background-color: #dcfce7; color: #15803d; font-weight: 600; padding: 2px 7px; border-radius: 4px; font-size: 11.5px;'>{val}</span>"
                else:
                    badge = f"<span style='background-color: #e0f2fe; color: #0369a1; font-weight: 600; padding: 2px 7px; border-radius: 4px; font-size: 11.5px;'>{val}</span>"
                td_cells.append(f"<td style='padding: 8px 12px; border: 1px solid {border_color};'>{badge}</td>")
            elif col == "Growth Score":
                badge = f"<span style='background-color: #fef3c7; color: #b45309; font-weight: 700; padding: 2px 8px; border-radius: 4px; font-size: 12.5px;'>{val}</span>"
                td_cells.append(f"<td style='padding: 8px 12px; border: 1px solid {border_color};'>{badge}</td>")
            elif col in ("Rev Growth", "EPS Growth", "ROE", "Op Margin"):
                td_cells.append(
                    f"<td style='padding: 8px 12px; border: 1px solid {border_color}; color: #047857; font-weight: 600;'>{val}</td>"
                )
            else:
                td_cells.append(
                    f"<td style='padding: 8px 12px; border: 1px solid {border_color}; color: #1e293b;'>{val}</td>"
                )

        if not has_levels_col:
            levels = _calc_key_price_levels(strategy_name, close_val)
            level_html = (
                f"<div style='font-size: 12px; line-height: 1.4;'>"
                f"<span>支撑: <b>{levels['support']}</b></span><br>"
                f"<span>止损: <b style='color:#dc2626;'>{levels['stop_loss']}</b></span>"
                f"</div>"
            )
            td_cells.append(f"<td style='padding: 8px 12px; border: 1px solid {border_color};'>{level_html}</td>")

        rows_html.append(f"<tr style='background-color: {bg};'>{''.join(td_cells)}</tr>")

    table_html = f"""
<table style='border-collapse: collapse; width: 100%; margin: 8px 0 6px 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; font-size: 13px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); border-radius: 6px; overflow: hidden;'>
  <thead><tr>{''.join(th_cells)}</tr></thead>
  <tbody>{''.join(rows_html)}</tbody>
</table>
""".strip()

    truncation_note = ""
    if total_count > MAX_DISPLAY_PER_STRATEGY:
        truncation_note = f"""
<div style="color: #64748b; font-size: 12px; margin: 4px 0 14px 2px; font-style: italic;">
  ⚡ 该策略今日共命中 <b>{total_count}</b> 只标的，晨报精选展示 Top 5（完整股票清单请查看终端控制台或历史日志）。
</div>
""".strip()

    return f"{table_html}\n{truncation_note}".strip()


def format_results_text(
    results: Dict[str, pd.DataFrame],
    strategy_metadata: Dict[str, str],
    market_regime: Optional[Any] = None,
    ai_advice: Optional[Any] = None,
) -> str:
    """
    将扫描结果格式化为纯文本，适用于命令行输出。包含大盘感知、AI 智能决策与各策略实战指南。
    """
    lines: List[str] = []

    if market_regime and hasattr(market_regime, "to_text"):
        lines.append(market_regime.to_text())
        lines.append("")

    if ai_advice and hasattr(ai_advice, "to_text"):
        advice_text = ai_advice.to_text()
        if advice_text and advice_text.strip():
            lines.append(advice_text)
            lines.append("")

    confluence_map = _calculate_confluence(results)

    # 按策略顺序遍历
    for strategy_name in strategy_metadata:
        df = results.get(strategy_name, pd.DataFrame())
        description = strategy_metadata.get(strategy_name, "无描述")
        playbook = get_strategy_playbook(strategy_name, description=description)

        lines.append(f"策略: {description}")
        lines.append(format_playbook_text(playbook))
        lines.append("-" * 60)

        if df.empty:
            if strategy_name == "GrowthCompounderStrategy":
                lines.append("今日全美股未找到兼具 15%+ 营收高增与 Stage 2 突破形态的长持标的（宁缺毋滥，保持耐心）。")
            else:
                lines.append("今日没有找到符合条件的股票。")
        else:
            total_count = len(df)
            df_display = df.head(MAX_DISPLAY_PER_STRATEGY).copy()

            # 在 Symbol 后加上共振标记
            df_print = df_display.copy()
            if "Symbol" in df_print.columns:
                def _mark_sym(s):
                    strats = confluence_map.get(str(s), [])
                    return f"{s} (🔥{len(strats)}重共振)" if len(strats) >= 2 else str(s)
                df_print["Symbol"] = df_print["Symbol"].apply(_mark_sym)

            # 加上关键位参考列
            if "Close" in df_print.columns:
                def _mark_sl(row):
                    lv = _calc_key_price_levels(strategy_name, row.get("Close", ""))
                    return f"止损 {lv['stop_loss']}"
                df_print["Risk Defense"] = df_print.apply(_mark_sl, axis=1)

            column_widths = {
                col: max(df_print[col].astype(str).apply(len).max(), len(col))
                for col in df_print.columns
            }

            header = " | ".join([col.ljust(column_widths[col]) for col in df_print.columns])
            lines.append(header)
            lines.append("-" * len(header))

            for _, row in df_print.iterrows():
                formatted_row = " | ".join(
                    [str(row[col]).ljust(column_widths[col]) for col in df_print.columns]
                )
                lines.append(formatted_row)

            if total_count > MAX_DISPLAY_PER_STRATEGY:
                lines.append(f"⚡ 共命中 {total_count} 只，控制台精选展示前 {MAX_DISPLAY_PER_STRATEGY} 只。")

        lines.append("=" * 60)
        lines.append("")

    if not lines:
        return "本次扫描未发现符合条件的股票。"

    return "\n".join(lines)


def format_results_for_email(
    results: Dict[str, pd.DataFrame],
    strategy_metadata: Dict[str, str],
    market_regime: Optional[Any] = None,
    ai_advice: Optional[Any] = None,
) -> str:
    """
    格式化扫描结果为 HTML 内容，适合用于邮件展示。
    层级结构：
    1. ⚡ 晨读 30 秒操盘速览 (Executive Summary，首屏极速决策池)
    2. 🌍 大盘宏观感知卡片与大类资产主线定调
    3. 🧠 DeepSeek AI 决策复盘卡片
    4. 📈 各策略选股精选池（精简单行胶囊标签 + 策略共振徽章 + 关键位与止损参考 + Top 5 截断）
    5. 📚 附录：量化策略实战交易手册与风控字典 (Playbooks Appendix)
    """
    confluence_map = _calculate_confluence(results)

    html_content = (
        "<style>\n"
        "  table { border-collapse: collapse; width: 100%; margin: 8px 0 14px 0; "
        "font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; "
        "font-size: 13px; }\n"
        "  th { background-color: #f1f5f9; color: #334155; font-weight: 600; text-align: left; "
        "padding: 8px 12px; border: 1px solid #cbd5e1; }\n"
        "  td { padding: 8px 12px; border: 1px solid #e2e8f0; color: #1e293b; }\n"
        "  tr:nth-child(even) { background-color: #f8fafc; }\n"
        "</style>\n"
        f"<h2>美股盘后扫描结果与量化决策内参 - {pd.Timestamp.now().strftime('%Y-%m-%d')}</h2>\n"
    )

    # 1. 增设【30 秒晨报摘要栏 (Executive Summary)】置于首屏
    exec_summary_html = _format_executive_summary_html(
        results, strategy_metadata, market_regime, confluence_map
    )
    html_content += exec_summary_html + "\n"

    # 2. 大盘宏观感知与大类资产联动
    if market_regime and hasattr(market_regime, "to_html"):
        html_content += market_regime.to_html()

    # 3. AI 决策复盘
    if ai_advice and hasattr(ai_advice, "to_html"):
        advice_html = ai_advice.to_html()
        if advice_html and advice_html.strip():
            html_content += advice_html

    # 4. 按策略顺序遍历策略
    for strategy_name in strategy_metadata:
        df = results.get(strategy_name, pd.DataFrame())
        description = strategy_metadata.get(strategy_name, "无描述")
        playbook = get_strategy_playbook(strategy_name, description=description)

        anchor_id = f"appendix-{strategy_name.lower()}"
        # 使用极简单行胶囊标签替代冗长的大段说明
        compact_badge_html = format_playbook_compact_badge_html(playbook, anchor_id=anchor_id)

        html_content += (
            f"<h3 style='color: #1e293b; margin-top: 24px; margin-bottom: 4px;'>{description}</h3>\n"
        )
        html_content += compact_badge_html + "\n"

        if df.empty:
            if strategy_name == "GrowthCompounderStrategy":
                html_content += (
                    "<div style='background-color: #f0fdf4; border: 1px dashed #86efac; border-radius: 6px; padding: 10px 14px; margin: 6px 0 12px 0; color: #166534; font-size: 12.5px;'>\n"
                    "  ⏳ <strong>今日全美股未出现兼具 15%+ 高速成长与 Stage 2 突破形态的标的。</strong><br>\n"
                    "  <span style='font-size: 11.5px; color: #15803d;'>（核心理念：长线复利宁缺毋滥，严格执行基本面与技术面双重门槛，保持耐心等待好公司放量突破）。</span>\n"
                    "</div>\n"
                )
            else:
                html_content += "<p style='color: #64748b; font-style: italic; margin: 6px 0 12px 0; font-size: 12.5px;'>今日没有找到符合条件的股票。</p>\n"
        else:
            table_html = _format_strategy_table_html(df, strategy_name, confluence_map)
            html_content += table_html + "\n"

        html_content += "<hr style='border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;'>\n"

    # 5. 邮件最底部附录：完整实战指南手册
    appendix_html = format_all_playbooks_appendix_html(list(strategy_metadata.keys()))
    html_content += appendix_html + "\n"

    return html_content
