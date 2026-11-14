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
) -> str:
    """
    将扫描结果格式化为纯文本，适用于命令行输出。包含大盘感知与各策略实战指南（目的与玩法）。

    :param results: {策略名称: DataFrame}
    :param strategy_metadata: 策略元数据字典 {策略名称: 描述}
    :param market_regime: 可选的大盘宏观感知结果 (MarketRegimeResult)
    :return: 格式化后的纯文本内容
    """
    lines: List[str] = []

    if market_regime and hasattr(market_regime, "to_text"):
        lines.append(market_regime.to_text())
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


def format_results_for_email(
    results: Dict[str, pd.DataFrame],
    strategy_metadata: Dict[str, str],
    market_regime: Optional[Any] = None,
) -> str:
    """
    格式化扫描结果为 HTML 内容，适合用于邮件展示。
    包含大盘宏观感知卡片、各策略实战指南卡片（目的、入场时机、持仓周期、止盈目标、止损防守、避坑要点）与选股表格。

    :param results: 策略结果字典 {策略名称: DataFrame}
    :param strategy_metadata: 策略元数据字典 {策略名称: 描述}
    :param market_regime: 可选的大盘宏观感知结果 (MarketRegimeResult)
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
            html_content += "<p style='color: #64748b; font-style: italic; margin: 8px 0 14px 0;'>今日没有找到符合条件的股票。</p>\n"
        else:
            html_table = df.to_html(
                index=False, table_id=f"results_{strategy_name.lower()}"
            )
            html_content += html_table + "\n"

        html_content += "<hr style='border: none; border-top: 1px solid #e2e8f0; margin: 24px 0;'>\n"

    if not html_content.strip():
        html_content = "<p>本次扫描未发现符合条件的股票。</p>"

    return html_content
