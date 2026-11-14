from typing import Dict, List, Optional, Any
import pandas as pd
from scanner.strategy_guide import (
    get_strategy_playbook,
    format_playbook_html,
    format_playbook_text,
)


def format_results_text(
    results: Dict[str, pd.DataFrame],
    strategy_metadata: Dict[str, str],
    market_regime: Optional[Any] = None,
    ai_advice: Optional[Any] = None,
) -> str:
    """
    将扫描结果格式化为纯文本，适用于命令行输出。包含大盘感知、AI 智能决策与各策略实战指南（目的与玩法）。

    :param results: {策略名称: DataFrame}
    :param strategy_metadata: 策略元数据字典 {策略名称: 描述}
    :param market_regime: 可选的大盘宏观感知结果 (MarketRegimeResult)
    :param ai_advice: 可选的 AI 决策复盘结果 (AIAdvisorResult)
    :return: 格式化后的纯文本内容
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

    # 按策略顺序（在 strategy_metadata 中按顺序排列）遍历策略
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
            column_widths = {
                col: max(df[col].astype(str).apply(len).max(), len(col))
                for col in df.columns
            }

            header = " | ".join([col.ljust(column_widths[col]) for col in df.columns])
            lines.append(header)

            lines.append("-" * len(header))

            for _, row in df.iterrows():
                formatted_row = " | ".join(
                    [str(row[col]).ljust(column_widths[col]) for col in df.columns]
                )
                lines.append(formatted_row)

        lines.append("=" * 60)
        lines.append("")

    if not lines:
        return "本次扫描未发现符合条件的股票。"

    return "\n".join(lines)


def _format_growth_compounder_table_html(df: pd.DataFrame) -> str:
    """
    为欧奈尔 CAN SLIM 价值成长复利策略定制的高对比度、信息层级清晰的 HTML 表格。
    将核心增长指标（Rev Growth, ROE, Growth Score）高亮展示，形态标签采用胶囊徽章。
    """
    if df.empty:
        return ""

    headers = [col for col in df.columns]
    th_cells = "".join(
        [
            f"<th style='background: linear-gradient(135deg, #065f46 0%, #047857 100%); color: #ffffff; padding: 9px 12px; font-size: 12.5px; border: 1px solid #047857; text-align: left;'>{h}</th>"
            for h in headers
        ]
    )

    rows_html = []
    for idx, row in df.iterrows():
        bg = "#f0fdf4" if idx % 2 == 1 else "#ffffff"
        td_cells = []
        for col in headers:
            val = str(row[col])
            if col == "Symbol":
                td_cells.append(
                    f"<td style='padding: 8px 12px; border: 1px solid #d1fae5; font-weight: 700; color: #065f46; font-size: 13.5px;'>{val}</td>"
                )
            elif col == "Setup":
                if "突破" in val:
                    badge = f"<span style='background-color: #dcfce7; color: #15803d; font-weight: 600; padding: 2px 7px; border-radius: 4px; font-size: 11.5px;'>{val}</span>"
                else:
                    badge = f"<span style='background-color: #e0f2fe; color: #0369a1; font-weight: 600; padding: 2px 7px; border-radius: 4px; font-size: 11.5px;'>{val}</span>"
                td_cells.append(f"<td style='padding: 8px 12px; border: 1px solid #d1fae5;'>{badge}</td>")
            elif col == "Growth Score":
                badge = f"<span style='background-color: #fef3c7; color: #b45309; font-weight: 700; padding: 2px 8px; border-radius: 4px; font-size: 12.5px;'>{val}</span>"
                td_cells.append(f"<td style='padding: 8px 12px; border: 1px solid #d1fae5;'>{badge}</td>")
            elif col in ("Rev Growth", "EPS Growth", "ROE", "Op Margin"):
                td_cells.append(
                    f"<td style='padding: 8px 12px; border: 1px solid #d1fae5; color: #047857; font-weight: 600;'>{val}</td>"
                )
            else:
                td_cells.append(
                    f"<td style='padding: 8px 12px; border: 1px solid #d1fae5; color: #1e293b;'>{val}</td>"
                )
        rows_html.append(f"<tr style='background-color: {bg};'>{''.join(td_cells)}</tr>")

    table_html = f"""
<table style='border-collapse: collapse; width: 100%; margin: 10px 0 16px 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; font-size: 13px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); border-radius: 6px; overflow: hidden;'>
  <thead><tr>{th_cells}</tr></thead>
  <tbody>{''.join(rows_html)}</tbody>
</table>
"""
    return table_html.strip()


def format_results_for_email(
    results: Dict[str, pd.DataFrame],
    strategy_metadata: Dict[str, str],
    market_regime: Optional[Any] = None,
    ai_advice: Optional[Any] = None,
) -> str:
    """
    格式化扫描结果为 HTML 内容，适合用于邮件展示。
    包含大盘宏观感知卡片、AI 决策复盘卡片、各策略实战指南卡片（目的、入场时机、持仓周期、止盈目标、止损防守、避坑要点）与选股表格。

    :param results: 策略结果字典 {策略名称: DataFrame}
    :param strategy_metadata: 策略元数据字典 {策略名称: 描述}
    :param market_regime: 可选的大盘宏观感知结果 (MarketRegimeResult)
    :param ai_advice: 可选的 AI 决策复盘结果 (AIAdvisorResult)
    :return: 格式化后的 HTML 内容
    """
    html_content = (
        "<style>\n"
        "  table { border-collapse: collapse; width: 100%; margin: 10px 0 16px 0; "
        "font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; "
        "font-size: 13px; }\n"
        "  th { background-color: #f1f5f9; color: #334155; font-weight: 600; text-align: left; "
        "padding: 8px 12px; border: 1px solid #cbd5e1; }\n"
        "  td { padding: 8px 12px; border: 1px solid #e2e8f0; color: #1e293b; }\n"
        "  tr:nth-child(even) { background-color: #f8fafc; }\n"
        "</style>\n"
        f"<h2>美股盘后扫描结果 - {pd.Timestamp.now().strftime('%Y-%m-%d')}</h2>"
    )

    if market_regime and hasattr(market_regime, "to_html"):
        html_content += market_regime.to_html()

    if ai_advice and hasattr(ai_advice, "to_html"):
        advice_html = ai_advice.to_html()
        if advice_html and advice_html.strip():
            html_content += advice_html

    # 按策略顺序（在 strategy_metadata 中按顺序排列）遍历策略
    for strategy_name in strategy_metadata:
        df = results.get(strategy_name, pd.DataFrame())
        description = strategy_metadata.get(strategy_name, "无描述")
        playbook = get_strategy_playbook(strategy_name, description=description)
        playbook_html = format_playbook_html(playbook)

        html_content += (
            f"<h3 style='color: #1e293b; margin-top: 24px; margin-bottom: 6px;'>{description}</h3>\n"
        )
        html_content += playbook_html + "\n"

        if df.empty:
            if strategy_name == "GrowthCompounderStrategy":
                html_content += (
                    "<div style='background-color: #f0fdf4; border: 1px dashed #86efac; border-radius: 6px; padding: 12px 16px; margin: 8px 0 14px 0; color: #166534; font-size: 13px;'>\n"
                    "  ⏳ <strong>今日全美股未出现兼具 15%+ 高速成长与 Stage 2 突破形态的标的。</strong><br>\n"
                    "  <span style='font-size: 12px; color: #15803d;'>（核心理念：长线复利宁缺毋滥，严格执行基本面与技术面双重门槛，保持耐心等待好公司跌出黄金坑或强势放量突破）。</span>\n"
                    "</div>\n"
                )
            else:
                html_content += "<p style='color: #64748b; font-style: italic; margin: 8px 0 14px 0;'>今日没有找到符合条件的股票。</p>\n"
        else:
            if strategy_name == "GrowthCompounderStrategy":
                html_table = _format_growth_compounder_table_html(df)
                html_content += html_table + "\n"
            else:
                html_table = df.to_html(
                    index=False, table_id=f"results_{strategy_name.lower()}"
                )
                html_content += html_table + "\n"

        html_content += "<hr style='border: none; border-top: 1px solid #e2e8f0; margin: 24px 0;'>\n"

    if not html_content.strip():
        html_content = "<p>本次扫描未发现符合条件的股票。</p>"

    return html_content
