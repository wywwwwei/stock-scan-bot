import re
import html
import requests
import pandas as pd
from typing import Dict, Optional, Any, List

from scanner.config.ai import (
    AI_ENABLED,
    DEEPSEEK_API_KEY,
    DEEPSEEK_BASE_URL,
    DEEPSEEK_MODEL,
    DEEPSEEK_TIMEOUT_SEC,
    AI_MAX_STOCKS_CONTEXT,
    AI_TEMPERATURE,
)


def _markdown_to_html(md_text: str) -> str:
    """
    轻量安全的 Markdown 转换为内联样式 HTML 渲染器，
    确保在各类邮件客户端（QQ邮箱、Gmail、Outlook 等）中保持精致美观排版。
    """
    # 规避基础 XSS 字符（保留后续替换标签）
    text = html.escape(md_text)

    # 标题转换
    text = re.sub(
        r"^###\s+(.*?)$",
        r'<h4 style="color: #4c1d95; font-size: 14px; margin: 12px 0 6px 0; border-left: 3px solid #7c3aed; padding-left: 8px;">\1</h4>',
        text,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"^##\s+(.*?)$",
        r'<h3 style="color: #3b0764; font-size: 15px; margin: 14px 0 8px 0;">\1</h3>',
        text,
        flags=re.MULTILINE,
    )

    # 加粗 **text** 与斜体 *text*
    text = re.sub(r"\*\*(.*?)\*\*", r'<strong style="color: #1e1b4b;">\1</strong>', text)
    text = re.sub(r"\*(.*?)\*", r"<em>\1</em>", text)

    # 无序列表行
    lines = text.split("\n")
    formatted_lines: List[str] = []
    in_ul = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- ") or stripped.startswith("* "):
            if not in_ul:
                formatted_lines.append('<ul style="margin: 4px 0 8px 18px; padding: 0; color: #374151;">')
                in_ul = True
            item_content = stripped[2:]
            formatted_lines.append(f'<li style="margin: 3px 0; line-height: 1.5;">{item_content}</li>')
        else:
            if in_ul:
                formatted_lines.append("</ul>")
                in_ul = False
            if stripped:
                formatted_lines.append(f'<p style="margin: 4px 0 6px 0; line-height: 1.6; color: #374151;">{line}</p>')

    if in_ul:
        formatted_lines.append("</ul>")

    return "\n".join(formatted_lines)


class AIAdvisorResult:
    """
    AI 智能交易内参分析结果容器。
    """

    def __init__(self, content: str, model_name: str = DEEPSEEK_MODEL):
        self.content = content.strip()
        self.model_name = model_name

    def to_text(self) -> str:
        lines = [
            "==================================================",
            f"🤖 AI 首席复盘与操盘内参 ({self.model_name})",
            "--------------------------------------------------",
            self.content,
            "==================================================",
        ]
        return "\n".join(lines)

    def to_html(self) -> str:
        body_html = _markdown_to_html(self.content)
        return f"""
<div style="background: linear-gradient(135deg, #f5f3ff 0%, #ede9fe 100%); border: 1px solid #ddd6fe; border-left: 6px solid #7c3aed; border-radius: 8px; padding: 14px 18px; margin-bottom: 22px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; box-shadow: 0 2px 4px rgba(124, 58, 237, 0.06);">
  <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
    <span style="font-size: 15.5px; font-weight: 700; color: #5b21b6;">🤖 AI 首席复盘与操盘指引</span>
    <span style="background-color: #7c3aed; color: #ffffff; padding: 2px 9px; border-radius: 12px; font-size: 11px; font-weight: 600;">{self.model_name} 驱动</span>
  </div>
  <div style="background-color: #ffffff; border: 1px solid #e9d5ff; border-radius: 6px; padding: 12px 16px; font-size: 13px;">
    {body_html}
  </div>
  <div style="margin-top: 6px; font-size: 11px; color: #7c3aed; text-align: right; font-style: italic;">
    💡 提示：本分析由 DeepSeek 大模型基于大盘量化指标与策略信号合成，仅供交易参考，请严格执行止损。
  </div>
</div>
""".strip()


def _format_context_for_ai(
    results: Dict[str, pd.DataFrame],
    market_regime: Optional[Any] = None,
) -> str:
    """
    将大盘宏观状态与多策略筛选结果压缩格式化为高信息密度文本，降低 Token 消耗。
    """
    lines: List[str] = []

    # 1. 大盘宏观感知上下文
    lines.append("【一、大盘宏观感知数据】")
    if market_regime:
        lines.append(f"- 市场环境评级: {getattr(market_regime, 'regime_name', '未知')}")
        lines.append(f"- 情绪温度: {getattr(market_regime, 'temperature', '未知')}")
        lines.append(f"- 战术指引: {getattr(market_regime, 'tactical_guidance', '无')}")
        qqq = getattr(market_regime, "qqq", None)
        if qqq:
            lines.append(
                f"- 纳指 QQQ: 收盘 ${qqq.close:.2f} ({'+' if qqq.change_1d_pct >= 0 else ''}{qqq.change_1d_pct:.2f}%), "
                f"距20日线: {'+' if qqq.dist_ma20_pct >= 0 else ''}{qqq.dist_ma20_pct:.2f}%, 14-RSI: {qqq.rsi14:.1f}"
            )
        spy = getattr(market_regime, "spy", None)
        if spy:
            lines.append(
                f"- 标普 SPY: 收盘 ${spy.close:.2f} ({'+' if spy.change_1d_pct >= 0 else ''}{spy.change_1d_pct:.2f}%), "
                f"距20日线: {'+' if spy.dist_ma20_pct >= 0 else ''}{spy.dist_ma20_pct:.2f}%, 14-RSI: {spy.rsi14:.1f}"
            )
    else:
        lines.append("- 未提供大盘宏观感知数据")

    lines.append("")

    # 2. 策略命中股票初筛结果
    lines.append("【二、12 大策略命中股票清单】")
    total_hits = 0
    collected_rows = 0

    for strategy_name, df in results.items():
        if df is None or df.empty:
            continue
        count = len(df)
        total_hits += count
        lines.append(f"▶ 策略 [{strategy_name}] 命中 {count} 只:")

        for _, row in df.iterrows():
            if collected_rows >= AI_MAX_STOCKS_CONTEXT:
                lines.append(f"  ... (已达单次送入上限 {AI_MAX_STOCKS_CONTEXT} 只标的，其余略)")
                break

            # 提取简明指标
            row_dict = row.to_dict()
            ticker = row_dict.pop("Symbol", "Unknown")
            close = row_dict.pop("Close", "")
            change = row_dict.pop("Change %", "")
            other_metrics = [f"{k}={v}" for k, v in list(row_dict.items())[:3]]

            metric_str = f"价格: {close}"
            if change:
                metric_str += f" ({change})"
            if other_metrics:
                metric_str += f", 特征: {'; '.join(other_metrics)}"

            lines.append(f"  - {ticker}: {metric_str}")
            collected_rows += 1

    if total_hits == 0:
        lines.append("- 今日 12 大策略均未找到符合触发条件的股票（0 命中）。")

    return "\n".join(lines)


def generate_ai_advice(
    results: Dict[str, pd.DataFrame],
    market_regime: Optional[Any] = None,
) -> Optional[AIAdvisorResult]:
    """
    调用 DeepSeek 极低成本大模型 API，对盘后扫描结果与大盘宏观环境进行多策略协同复盘与主观交易裁决。

    :param results: 各策略命中结果字典 {策略名: DataFrame}
    :param market_regime: 可选的大盘感知结果 (MarketRegimeResult)
    :return: AIAdvisorResult 或在未配置/失败时优雅返回 None
    """
    if not AI_ENABLED:
        return None

    if not DEEPSEEK_API_KEY:
        print("[INFO] DEEPSEEK_API_KEY 未配置，跳过 AI 智能决策分析")
        return None

    context_text = _format_context_for_ai(results, market_regime)

    system_prompt = (
        "你是一名经验丰富的华尔街资深美股量化交易员与对冲基金经理，擅长结合大盘宏观感知（Market Regime）"
        "与经典量化技术形态（欧奈尔机构口袋支点、威科夫破底翻弹簧、MACD水下底背离、TTM挤压等）进行跨维度综合裁决。\n"
        "请根据机器人盘后自动扫描出的“大盘感知指标”和“策略命中清单”，进行高水平的实战复盘与主观裁决。\n\n"
        "【输出要求】（保持客观专业、高信息密度、言简意赅，严禁空话套话，采用标准 Markdown）：\n"
        "### 1. 🌍【大势定调与仓位策略】\n"
        "- 结合 QQQ/SPY 位置与情绪温度，明确大盘是顺风进攻还是逆风蓄力，给出建议总仓位上限（例如 50%~60%）。\n"
        "### 2. ⭐【明日重点精选标的 Top 1~3】（若有标的命中）\n"
        "- 从命中的股票中精选出形态最扎实、胜率与盈亏比最优质的 1~3 只标的，具体指明：\n"
        "  * 推荐理由（为什么选它、量价核心优势）；\n"
        "  * 买入时机与建议关注区间（开盘跟进还是回踩低吸）；\n"
        "  * 明确的防守止损位与波段第一止盈目标。\n"
        "### 3. ⚠️【风险排雷与纪律告诫】\n"
        "- 针对命中股票中的仙股暴拉、高位长上影或假突破嫌疑标的，明确指出风险并告诫仓位控制。\n"
        "（如果今日无股票命中，请重点分析大盘关键多空防守位，并给出持币耐心等待的战术指引）。"
    )

    endpoint = f"{DEEPSEEK_BASE_URL.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": context_text},
        ],
        "temperature": AI_TEMPERATURE,
        "max_tokens": 1000,
        "stream": False,
    }

    try:
        print(f"[INFO] 正在请求 DeepSeek AI 决策分析 ({DEEPSEEK_MODEL})...")
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=DEEPSEEK_TIMEOUT_SEC)
        if resp.status_code == 200:
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            print("[INFO] DeepSeek AI 分析完成")
            return AIAdvisorResult(content=content, model_name=DEEPSEEK_MODEL)
        else:
            print(f"[WARN] DeepSeek API 请求返回异常状态码 {resp.status_code}: {resp.text}")
            return None
    except requests.exceptions.Timeout:
        print(f"[WARN] DeepSeek API 请求超时（阈值 {DEEPSEEK_TIMEOUT_SEC} 秒），跳过 AI 分析")
        return None
    except Exception as e:
        print(f"[WARN] DeepSeek API 调用失败: {e}，跳过 AI 分析")
        return None
