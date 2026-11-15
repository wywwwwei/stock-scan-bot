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
    AI_THINKING_ENABLED,
    AI_MAX_TOKENS,
)


def _markdown_to_html(md_text: str) -> str:
    """
    轻量安全的 Markdown 转换为内联样式 HTML 渲染器，
    确保在各类邮件客户端（QQ邮箱、Gmail、Outlook 等）中保持精致美观排版。
    """
    if not md_text:
        return ""

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
        self.content = content.strip() if content else ""
        self.model_name = model_name

    def to_text(self) -> str:
        if not self.content:
            return ""
        lines = [
            "==================================================",
            f"🤖 AI 首席复盘与操盘内参 ({self.model_name})",
            "--------------------------------------------------",
            self.content,
            "==================================================",
        ]
        return "\n".join(lines)

    def to_html(self) -> str:
        if not self.content:
            return ""
        body_html = _markdown_to_html(self.content)
        if not body_html.strip():
            return ""

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
    lines.append("【一、美股大盘宏观感知数据】")
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

    # 2. 全球跨资产联动行情（金银、油气、加密货币）
    lines.append("【二、全球跨资产联动最新走势（金银、油气、加密资产）】")
    if market_regime:
        has_macro = False
        gold = getattr(market_regime, "gold", None)
        silver = getattr(market_regime, "silver", None)
        if gold or silver:
            has_macro = True
            lines.append("- 🪙 贵金属（避险属性 / 实际利率 / 抗通胀）:")
            if gold:
                lines.append(
                    f"  * 黄金 (GC=F): ${gold.close:.2f} (日内: {'+' if gold.change_1d_pct >= 0 else ''}{gold.change_1d_pct:.2f}%, "
                    f"5日: {'+' if gold.change_5d_pct >= 0 else ''}{gold.change_5d_pct:.2f}%, RSI: {gold.rsi14:.1f}, 距MA20: {gold.dist_ma20_pct:+.1f}%)"
                )
            if silver:
                lines.append(
                    f"  * 白银 (SI=F): ${silver.close:.2f} (日内: {'+' if silver.change_1d_pct >= 0 else ''}{silver.change_1d_pct:.2f}%, "
                    f"5日: {'+' if silver.change_5d_pct >= 0 else ''}{silver.change_5d_pct:.2f}%, RSI: {silver.rsi14:.1f}, 距MA20: {silver.dist_ma20_pct:+.1f}%)"
                )

        oil = getattr(market_regime, "oil", None)
        gas = getattr(market_regime, "gas", None)
        if oil or gas:
            has_macro = True
            lines.append("- 🛢️ 能源大宗（通胀预期 / 工业需求与供给冲击）:")
            if oil:
                lines.append(
                    f"  * WTI原油 (CL=F): ${oil.close:.2f} (日内: {'+' if oil.change_1d_pct >= 0 else ''}{oil.change_1d_pct:.2f}%, "
                    f"5日: {'+' if oil.change_5d_pct >= 0 else ''}{oil.change_5d_pct:.2f}%, RSI: {oil.rsi14:.1f}, 距MA20: {oil.dist_ma20_pct:+.1f}%)"
                )
            if gas:
                lines.append(
                    f"  * 天然气 (NG=F): ${gas.close:.2f} (日内: {'+' if gas.change_1d_pct >= 0 else ''}{gas.change_1d_pct:.2f}%, "
                    f"5日: {'+' if gas.change_5d_pct >= 0 else ''}{gas.change_5d_pct:.2f}%, RSI: {gas.rsi14:.1f}, 距MA20: {gas.dist_ma20_pct:+.1f}%)"
                )

        btc = getattr(market_regime, "btc", None)
        eth = getattr(market_regime, "eth", None)
        if btc or eth:
            has_macro = True
            lines.append("- ⚡ 加密数字资产（全球流动性水龙头 / 高Beta风险偏好温度计）:")
            if btc:
                lines.append(
                    f"  * 比特币 (BTC-USD): ${btc.close:,.2f} (日内: {'+' if btc.change_1d_pct >= 0 else ''}{btc.change_1d_pct:.2f}%, "
                    f"5日: {'+' if btc.change_5d_pct >= 0 else ''}{btc.change_5d_pct:.2f}%, RSI: {btc.rsi14:.1f}, 距MA20: {btc.dist_ma20_pct:+.1f}%)"
                )
            if eth:
                lines.append(
                    f"  * 以太币 (ETH-USD): ${eth.close:,.2f} (日内: {'+' if eth.change_1d_pct >= 0 else ''}{eth.change_1d_pct:.2f}%, "
                    f"5日: {'+' if eth.change_5d_pct >= 0 else ''}{eth.change_5d_pct:.2f}%, RSI: {eth.rsi14:.1f}, 距MA20: {eth.dist_ma20_pct:+.1f}%)"
                )

        cgb_30y = getattr(market_regime, "cgb_30y", None)
        cgb_10y = getattr(market_regime, "cgb_10y", None)
        cgb_short = getattr(market_regime, "cgb_short", None)
        if cgb_30y or cgb_10y or cgb_short:
            has_macro = True
            lines.append("- 🇨🇳 中国长短端利率债（无风险利率走势 / 资产荒与降息宽松预期）:")
            if cgb_30y:
                lines.append(
                    f"  * 30年超长国债 (511090.SS): ${cgb_30y.close:.2f} (日内: {'+' if cgb_30y.change_1d_pct >= 0 else ''}{cgb_30y.change_1d_pct:.2f}%, "
                    f"5日: {'+' if cgb_30y.change_5d_pct >= 0 else ''}{cgb_30y.change_5d_pct:.2f}%, RSI: {cgb_30y.rsi14:.1f}, 距MA20: {cgb_30y.dist_ma20_pct:+.2f}%)"
                )
            if cgb_10y:
                lines.append(
                    f"  * 10年基准国债 (511260.SS): ${cgb_10y.close:.2f} (日内: {'+' if cgb_10y.change_1d_pct >= 0 else ''}{cgb_10y.change_1d_pct:.2f}%, "
                    f"5日: {'+' if cgb_10y.change_5d_pct >= 0 else ''}{cgb_10y.change_5d_pct:.2f}%, RSI: {cgb_10y.rsi14:.1f}, 距MA20: {cgb_10y.dist_ma20_pct:+.2f}%)"
                )
            if cgb_short:
                lines.append(
                    f"  * 短端利率流动性/短融 (511360.SS): ${cgb_short.close:.2f} (日内: {'+' if cgb_short.change_1d_pct >= 0 else ''}{cgb_short.change_1d_pct:.2f}%, "
                    f"5日: {'+' if cgb_short.change_5d_pct >= 0 else ''}{cgb_short.change_5d_pct:.2f}%, RSI: {cgb_short.rsi14:.1f}, 距MA20: {cgb_short.dist_ma20_pct:+.2f}%)"
                )

        credit_chengtou = getattr(market_regime, "credit_chengtou", None)
        credit_corp = getattr(market_regime, "credit_corp", None)
        if credit_chengtou or credit_corp:
            has_macro = True
            lines.append("- 🏢 中国信用债与城投（信用利差 / 化债进程 / 实体债务环境）:")
            if credit_chengtou:
                lines.append(
                    f"  * 城投债ETF (511220.SS): ${credit_chengtou.close:.2f} (日内: {'+' if credit_chengtou.change_1d_pct >= 0 else ''}{credit_chengtou.change_1d_pct:.2f}%, "
                    f"5日: {'+' if credit_chengtou.change_5d_pct >= 0 else ''}{credit_chengtou.change_5d_pct:.2f}%, RSI: {credit_chengtou.rsi14:.1f}, 距MA20: {credit_chengtou.dist_ma20_pct:+.2f}%)"
                )
            if credit_corp:
                lines.append(
                    f"  * 公司债ETF (511030.SS): ${credit_corp.close:.2f} (日内: {'+' if credit_corp.change_1d_pct >= 0 else ''}{credit_corp.change_1d_pct:.2f}%, "
                    f"5日: {'+' if credit_corp.change_5d_pct >= 0 else ''}{credit_corp.change_5d_pct:.2f}%, RSI: {credit_corp.rsi14:.1f}, 距MA20: {credit_corp.dist_ma20_pct:+.2f}%)"
                )

        if not has_macro:
            lines.append("- 暂未获取到宏观大类资产行情数据")
    else:
        lines.append("- 未提供大类资产行情数据")

    lines.append("")

    # 3. 策略命中股票初筛结果
    lines.append("【三、多策略命中股票清单（含长线复利与短线动量）】")
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
        lines.append("- 今日多策略均未找到符合触发条件的股票（0 命中）。")

    return "\n".join(lines)


def generate_ai_advice(
    results: Dict[str, pd.DataFrame],
    market_regime: Optional[Any] = None,
) -> Optional[AIAdvisorResult]:
    """
    调用 DeepSeek 大模型 API，对盘后扫描结果与大盘宏观环境进行多策略协同复盘与主观交易裁决。

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
        "你是一名经验丰富的华尔街资深美股量化交易员与宏观跨资产对冲基金经理，擅长结合全球大类资产联动（金银避险/实际利率、"
        "原油天然气/通胀预期、比特币以太币/全球投机流动性、中国长短利率债与信用债/化债与宏观流动性）与经典量化形态（欧奈尔 CAN SLIM 价值成长、"
        "机构口袋支点、威科夫弹簧坑、MACD水下底背离、TTM变盘挤压等）进行跨市场综合裁决。\n"
        "请根据机器人盘后自动抓取的“大盘宏观感知”、“全球大类资产行情（金银、油气、加密货币、中国利率与信用债）”和“多策略命中股票清单”，进行高水平的实战复盘与跨资产交易内参。\n\n"
        "【输出要求】（保持客观专业、高信息密度、言简意赅，严禁空话套话，采用标准 Markdown 格式输出以下四个部分）：\n"
        "### 1. 🌍【大势定调与仓位策略】\n"
        "- 结合 QQQ/SPY 位置与情绪温度，明确大盘是顺风进攻还是逆风蓄力，给出建议总仓位上限（例如 50%~60%）。\n"
        "### 2. 🌐【全球大类资产与中国股债联动微观复盘】\n"
        "- 🪙 **金银避险与宏观流动性**：结合黄金与白银最新涨跌、5日趋势及 RSI，研判当前避险情绪强度、地缘扰动与美元/实际利率预期；\n"
        "- 🛢️ **油气大宗与通胀预期**：结合 WTI 原油与天然气价格异动，评估大宗商品对二次通胀压力与美联储降息周期的潜在冲击；\n"
        "- ⚡ **比特币/以太币与风险偏好**：结合 BTC 与 ETH 动量与超买超卖状态，研判全球投机流动性水温及对高估值科技股的领先情绪映射；\n"
        "- 🇨🇳 **中国长短端利率债与信用债研判**：\n"
        "  * 【利率债长短端分析】：结合 30 年超长国债、10 年基准国债与短端短融走势，分析中国资产荒博弈、长短端期限利差与央行货币流动性取向；\n"
        "  * 【信用债与城投分析】：结合城投债与公司债表现，剖析一揽子化债背景下的信用利差走势与实体产业融资环境；\n"
        "  * 【对中美利差与美股资产传导】：分析中美利差倒挂、汇率预期对中概股、大宗需求与全球风险资产的映射传导。\n"
        "### 3. ⭐【明日重点精选标的 Top 1~3】（若有标的命中）\n"
        "- 从命中的股票中精选出形态最扎实、胜率与盈亏比最优质的 1~3 只标的（若命中【CAN SLIM 价值成长复利】标的，请重点剖析其基本面与 Stage 2 结构，适合长线持有）：\n"
        "  * 推荐理由（为什么选它、基本面与量价核心优势）；\n"
        "  * 买入时机与建议关注区间（开盘跟进还是回踩低吸）；\n"
        "  * 明确的防守止损位与目标止盈空间。\n"
        "### 4. ⚠️【风险排雷与纪律告诫】\n"
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
        "thinking": {
            "type": "enabled" if AI_THINKING_ENABLED else "disabled"
        },
        "max_tokens": AI_MAX_TOKENS,
        "stream": False,
    }

    # 仅在非思考模式下传递 temperature
    if not AI_THINKING_ENABLED:
        payload["temperature"] = AI_TEMPERATURE

    try:
        thinking_tag = "开启思考" if AI_THINKING_ENABLED else "极速直接输出"
        print(f"[INFO] 正在请求 DeepSeek AI 决策分析 (模型: {DEEPSEEK_MODEL}, 模式: {thinking_tag})...")
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=DEEPSEEK_TIMEOUT_SEC)
        if resp.status_code == 200:
            data = resp.json()
            choices = data.get("choices", [])
            if not choices:
                print(f"[WARN] DeepSeek 返回异常: choices 列表为空")
                return None

            choice = choices[0]
            message = choice.get("message", {})
            content = message.get("content") or ""
            reasoning = message.get("reasoning_content") or ""
            finish_reason = choice.get("finish_reason", "")

            # 优先采用正文 content；若开启思考模式且 content 为空，回退采用 reasoning_content
            final_content = content.strip()
            if not final_content and reasoning.strip():
                print(f"[WARN] DeepSeek content 为空，finish_reason={finish_reason}，回退采用 reasoning_content")
                final_content = reasoning.strip()

            if not final_content:
                print(f"[WARN] DeepSeek 返回内容为空 (finish_reason={finish_reason})，跳过 AI 卡片渲染")
                return None

            print(f"[INFO] DeepSeek AI 分析完成 (字符数: {len(final_content)}, finish_reason: {finish_reason})")
            return AIAdvisorResult(content=final_content, model_name=DEEPSEEK_MODEL)
        else:
            print(f"[WARN] DeepSeek API 请求返回异常状态码 {resp.status_code}: {resp.text}")
            return None
    except requests.exceptions.Timeout:
        print(f"[WARN] DeepSeek API 请求超时（阈值 {DEEPSEEK_TIMEOUT_SEC} 秒），跳过 AI 分析")
        return None
    except Exception as e:
        print(f"[WARN] DeepSeek API 调用失败: {e}，跳过 AI 分析")
        return None
