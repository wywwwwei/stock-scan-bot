from typing import Dict, List
import pandas as pd


def format_results_text(
    results: Dict[str, pd.DataFrame], strategy_metadata: Dict[str, str]
) -> str:
    """
    将扫描结果格式化为纯文本，适用于命令行输出。

    :param results: {策略名称: DataFrame}
    :param strategy_metadata: 策略元数据字典 {策略名称: 描述}
    :return: 格式化后的纯文本内容
    """
    lines: List[str] = []

    # 按策略顺序（在 strategy_metadata 中按顺序排列）遍历策略
    for strategy_name in strategy_metadata:
        df = results.get(strategy_name, pd.DataFrame())
        description = strategy_metadata.get(strategy_name, "无描述")
        lines.append(f"策略: {description}")
        lines.append("=" * 50)

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

        lines.append("")

    if not lines:
        return "本次扫描未发现符合条件的股票。"

    return "\n".join(lines)


def format_results_for_email(
    results: Dict[str, pd.DataFrame], strategy_metadata: Dict[str, str]
) -> str:
    """
    格式化扫描结果为 HTML 内容，适合用于邮件展示

    :param results: 策略结果字典 {策略名称: DataFrame}
    :param strategy_metadata: 策略元数据字典 {策略名称: 描述}
    :return: 格式化后的 HTML 内容
    """
    html_content = (
        f"<h2>美股盘后扫描结果 - {pd.Timestamp.now().strftime('%Y-%m-%d')}</h2>"
    )

    # 按策略顺序（在 strategy_metadata 中按顺序排列）遍历策略
    for strategy_name in strategy_metadata:
        df = results.get(strategy_name, pd.DataFrame())
        description = strategy_metadata.get(strategy_name, "无描述")

        if df.empty:
            html_content += f"<h3>{description}</h3><p>今日没有找到符合条件的股票。</p>"
        else:
            html_content += f"<h3>{description}</h3>"
            html_table = df.to_html(
                index=False, table_id=f"results_{strategy_name.lower()}"
            )
            html_content += html_table

        html_content += "<hr>"

    if not html_content.strip():
        html_content = "<p>本次扫描未发现符合条件的股票。</p>"

    return html_content
