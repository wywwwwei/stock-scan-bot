from typing import Dict, Any, Optional

# ==============================================================================
# 12 大选股策略实战操作指南 (Strategic Playbooks)
# 涵盖：策略目的 (Purpose)、入场时机 (Entry)、持仓周期 (Holding Period)、
# 止盈目标 (Take Profit)、止损防守 (Stop Loss)、核心纪律要点 (Tips)
# ==============================================================================

STRATEGY_PLAYBOOKS: Dict[str, Dict[str, str]] = {
    "CDSignalStrategy": {
        "name": "CDSignalStrategy",
        "title": "MACD水下金叉底背离抄底策略",
        "category": "大中盘股 / 右侧抄底 / 趋势反转",
        "purpose": "大中盘稳健股波段抄底。捕捉价格创新低/次低但 MACD (DIF/DEA) 水下双波谷抬高、绿柱持续缩短翻红的底背离拐点，在日均交易额 > 1000 万美元的标的中博弈空头衰竭后的波段大反弹。",
        "entry": "水下金叉确认日尾盘介入，或次日开盘确认企稳不破昨日低点时低吸。",
        "holding_period": "10 ~ 20 个交易日（实测 T+20 胜率 65.6%，属于稳健中线波段，切忌频繁日内倒手）。",
        "take_profit": "冲高 +10% ~ +15% 分批止盈；或股价触及 60 日均线阻力位、MACD 零轴上方高位死叉时结清。",
        "stop_loss": "跌破第二波谷最低点下方 2% 坚决止损认错（下破说明背离失败，必须果断离场）。",
        "tips": "流动性极佳，适合中大仓位；在大盘（QQQ/SPY）企稳反弹或中性震荡时胜率最高。",
    },
    "MACrossStrategy": {
        "name": "MACrossStrategy",
        "title": "MA5 上穿 MA10 顺势突破策略",
        "category": "短线动量 / 顺势启动",
        "purpose": "短线顺势动量突破。专挑过去 10 天日均成交额 > 5000 万美元的高流动性白马股，捕捉 5 日均线向上金叉 10 日均线、短期动能加速发散的启动信号。",
        "entry": "金叉确认日尾盘跟进，或次日开盘微调回踩 5 日均线企稳时介入。",
        "holding_period": "3 ~ 5 个交易日（短平快超短线操作）。",
        "take_profit": "冲高 +5% ~ +8% 分批止盈锁定收益；若沿 5 日线强力连阳推升可保留底仓跟踪至跌破 5 日线。",
        "stop_loss": "收盘跌破 10 日均线或跌破信号日开盘价止损（单笔亏损控制在 2%~3% 范围）。",
        "tips": "顺势而为，严禁逆大盘操作；若大盘处于下行趋势（Bearish），均线假金叉极多，须减仓防守。",
    },
    "VolumeSurgeStrategy": {
        "name": "VolumeSurgeStrategy",
        "title": "MA50多头放量突破抢筹策略",
        "category": "趋势顺风 / 主力抢筹突破",
        "purpose": "顺风右侧狙击。专挑位于 50 日均线（MA50）多头生命线上方、单日成交额爆发超 60 日均值 2 倍且温和收涨（+1% ~ +8%）的标的，抓取机构资金建仓抢筹的首发日。",
        "entry": "信号日尾盘收在 +1% ~ +8% 适度启动区间介入（已排除单日暴涨透支的假突破追高陷阱）。",
        "holding_period": "1 ~ 3 个交易日（快进快出，享受主力脉冲放量溢价）。",
        "take_profit": "冲高 +5% ~ +7% 快速止盈落袋，锁定超短线暴增收益。",
        "stop_loss": "以信号日放量阳线开盘价为防守底线，次日一旦收盘跌破开盘价立即无条件离场。",
        "tips": "MA50 向上是持股底气；严禁追买日内涨幅过大或留长上影线冲高回落的诱多股。",
    },
    "TrendPullbackStrategy": {
        "name": "TrendPullbackStrategy",
        "title": "强趋势缩量回踩 MA20 支撑策略",
        "category": "牛股低吸 / 高盈亏比洗盘",
        "purpose": "牛股洗盘低吸法。在 20 日和 50 日均线多头排列（MA20 > MA50 且上市满 200 天 MA50 > MA200）的大牛股中，捕捉极致缩量（成交量 < 均量 60%）回踩 20 日生命线（贴合度 0%~1.5%）的黄金企稳点。",
        "entry": "缩量触及 MA20 且尾盘收阳线或企稳十字星时果断低吸介入。",
        "holding_period": "5 ~ 10 个交易日（波段反弹推升周期）。",
        "take_profit": "博弈反弹前高，第一止盈位 +8% ~ +12%；若强势放量突破前高可顺势移动止盈。",
        "stop_loss": "收盘有效跌破 MA20 超过 1.0% 果断止损（试错成本极小，盈亏比通常 > 3:1）。",
        "tips": "“缩量回踩是洗盘，放量回踩是出货”。回踩当日成交量必须极度萎缩，放量破位切忌接飞刀。",
    },
    "NewHighBreakoutStrategy": {
        "name": "NewHighBreakoutStrategy",
        "title": "52周新高/平台放量突破策略",
        "category": "龙头突破 / 欧奈尔动量领头羊",
        "purpose": "真龙头右侧主升浪狙击。精选价格突破 52 周新高（或近 60 日最高价），单日成交额放量 1.5 倍以上，且收盘价稳收在全天振幅上 25% 极强位置的领涨龙头，上方无历史套牢盘阻力。",
        "entry": "尾盘确认放量突破平台且收在日内最高点附近时直接半路或尾盘跟进。",
        "holding_period": "5 ~ 15 个交易日（波段持股跟踪主升浪）。",
        "take_profit": "突破后享受主升浪，+10% ~ +20% 分批止盈，或以 10 日均线作为移动止盈线跌破全抛。",
        "stop_loss": "以突破平台颈线为绝对防守位，若次日假突破跌回平台下 2%~3% 坚决止损认错。",
        "tips": "只买强者恒强的行业龙头；必须有大盘顺风配合，大盘熊市时新高假突破率极高。",
    },
    "TTMSqueezeStrategy": {
        "name": "TTMSqueezeStrategy",
        "title": "波动率挤压变盘突破策略 (TTM Squeeze)",
        "category": "变盘爆发 / 动量中线（实测胜率 67.5%）",
        "purpose": "经典量化变盘爆发狙击（实测 T+20 胜率 67.5%）。利用布林带收缩进肯特纳通道形成的“极端挤压（Squeeze 蓄力 3~10 天）”，捕捉多空平衡打破、首日向外开花变盘暴发的转折点。",
        "entry": "挤压状态释放第 1 天，首日涨幅处于 +1% ~ +6% 温和启动区间时果断建仓。",
        "holding_period": "10 ~ 20 个交易日（历史数据表明 T+20 盈亏比与胜率最佳）。",
        "take_profit": "波段目标 +8% ~ +15%；当动量柱由深红转浅红或股价远离布林线上轨时分批止盈。",
        "stop_loss": "跌破布林带 20 日中轨或跌破挤压区中轴线止损（约 3% 风险空间）。",
        "tips": "“蓄之既久，发之必速”。挤压时间越长（3~10天），爆发爆发力越强；严禁追高单日已经暴涨 > 8% 严重透支的个股。",
    },
    "PocketPivotStrategy": {
        "name": "PocketPivotStrategy",
        "title": "机构口袋支点突破策略 (欧奈尔/莫拉莱斯模型)",
        "category": "欧奈尔名著模型 / 早期隐蔽建仓突破",
        "purpose": "欧奈尔/莫拉莱斯机构口袋支点突破。捕捉紧贴 MA10 或 MA20 均线（0%~3.5%），单日成交量超过近 10 天所有阴线最大量的机构隐秘建仓点，比传统 52 周新高突破更早、买入成本更低。",
        "entry": "当日放量超过前 10 天阴线最大量、收盘紧贴 MA10/MA20 均线收阳启动时介入。",
        "holding_period": "5 ~ 15 个交易日（波段启动持有至主升浪）。",
        "take_profit": "享受波段主升浪，目标 +10% ~ +20%，或触及上方关键阻力位分批减仓。",
        "stop_loss": "跌破均线（MA10/MA20）下方 2.5% 或跌破当日支点 K 线最低价坚决止损（盈亏比 > 3:1）。",
        "tips": "股票整体须处于良性上涨底座（Base）或主升途中，严禁在长期阴跌破位通道中盲目使用。",
    },
    "RSIDivergenceReboundStrategy": {
        "name": "RSIDivergenceReboundStrategy",
        "title": "RSI 极限超跌与双底背离反弹策略",
        "category": "极限恐慌盘衰竭 / 左侧暴力 V 反",
        "purpose": "极限恐慌盘衰竭后的短线暴力 V 反。捕捉 14-RSI 曾跌破 28 极度超卖冰点区、随后价格二次微创新低而 RSI 显著抬高的双底底背离衰竭形态，空头动能耗尽。",
        "entry": "底背离确立、当日收盘阳线重新收复 5 日均线（MA5）时果断跟进。",
        "holding_period": "3 ~ 5 个交易日（超跌反弹快进快出，切忌贪多长拿）。",
        "take_profit": "冲高 +8% ~ +12%、或 RSI 快速回升至 55~60 中性阻力区时果断结清。",
        "stop_loss": "以二次探底形成的 K 线最低价作为绝对止损位（跌破该低点说明恐慌未尽，必须无条件撤离）。",
        "tips": "逆势短线博弈，仓位宜轻；一旦反弹停滞或受阻于 20 日线，立即保本离场。",
    },
    "PennyBaseIgnitionStrategy": {
        "name": "PennyBaseIgnitionStrategy",
        "title": "仙股低位极度死寂放量起爆策略",
        "category": "微盘仙股 / 主力首日点火爆拉",
        "purpose": "微盘仙股低位极度死寂放量起爆战法。专挑低位横盘死寂 20 天以上（极窄箱体、均线高度粘合），今日突然爆出 3.5 倍以上天量且涨幅超 5% 的首日主力点火大阳线。",
        "entry": "点火首日盘中放量确认或尾盘确立光头大阳线时小仓位轻仓介入。",
        "holding_period": "3 ~ 7 个交易日（仙股具有极强时效性窗口，T+10 前无论盈亏必须全面结清，绝不可做时间的朋友）。",
        "take_profit": "+15% ~ +30% 迅速分批获利了结，切忌格局；若次日涨停或冲高乏力直接全走。",
        "stop_loss": "坚决以当日起爆 K 线实体中轴或开盘价作为止损线，次日一旦大幅低开回补立即砍仓（单笔最大承受 -5%）。",
        "tips": "仙股只用 2%~5% 的小资金娱乐博弈高赔率，严禁重仓或长期套牢死扛。",
    },
    "PennyStealthAccumulationStrategy": {
        "name": "PennyStealthAccumulationStrategy",
        "title": "仙股潜伏：量价背离与暗度陈仓策略",
        "category": "仙股左侧潜伏 / 庄家吸筹暗度陈仓",
        "purpose": "仙股左侧从容埋伏战法。捕捉超跌低位横盘死寂、价格纹丝不动，但 OBV 逆势爬升、阳线成交量显著压倒阴线（阳量比 > 1.8）的庄家暗度陈仓吸筹迹象，先于市场爆拉前潜伏。",
        "entry": "在均线粘合处或箱体下沿挂限价单从容买入，拒绝盘中追高。",
        "holding_period": "5 ~ 15 个交易日（耐得住寂寞，等待主力点火）。",
        "take_profit": "潜伏期间只要某一天突然放量冲高 +20% ~ +40%，当天盘中冲高即是全额止盈良机，绝不留恋。",
        "stop_loss": "跌破近期震荡箱体底部支撑超过 3% 立即止损认错。",
        "tips": "潜伏最忌心浮气躁；若建仓超过 15 个交易日仍无任何资金异动，应主动调仓换股。",
    },
    "PennyWyckoffSpringStrategy": {
        "name": "PennyWyckoffSpringStrategy",
        "title": "威科夫弹簧坑扫损反包埋伏策略",
        "category": "威科夫量价学 / 诱空破底翻反包",
        "purpose": "威科夫量价学 Phase C 终极买点（Spring 弹簧坑效应）。主力击穿关键支撑平台扫出散户止损后迅速收复，今日极度缩量（< 均量 55%）确认测试无供给，浮筹清洗完毕。",
        "entry": "确认收复支撑线之上、今日极度缩量收企稳阳线或十字星时尾盘果断介入。",
        "holding_period": "5 ~ 10 个交易日（波段拉升周期）。",
        "take_profit": "目标冲击震荡箱体上沿或波段暴击，预期收益 +15% ~ +40%。",
        "stop_loss": "以 Spring 下插的针尖最低价下方 2% 作为止损（试错风险极小，盈亏比极高，往往可达 4:1 以上）。",
        "tips": "必须确认股价已“重新站回”支撑线上方；若次日再次放量跌破支撑线，说明并非 Spring 而是真实破位下行，需立刻跑路。",
    },
    "PennyInsideBarRetestStrategy": {
        "name": "PennyInsideBarRetestStrategy",
        "title": "仙股首日放量次日缩量孕线接力策略",
        "category": "仙股次日接力 / 孕线洗盘低吸",
        "purpose": "仙股次日洗盘低吸接力战法。解决第 1 天追不上或买在长上影线套牢的痛点，专在昨日大阳暴涨后、今日缩量（< 昨日 55%）守住实体 50% 中轴时低吸接力，博弈主升第 2 浪。",
        "entry": "次日尾盘确认收在昨日阳线实体中轴上方、成交量缩半时接力介入。",
        "holding_period": "2 ~ 5 个交易日（超短线接力）。",
        "take_profit": "冲破昨日最高点阻力后，目标涨幅 +12% ~ +25% 分批获利出局。",
        "stop_loss": "跌破今日孕线低点或昨日阳线 50% 强支撑位立即止损出局（风险极窄，通常仅 2%~3%）。",
        "tips": "今日缩量程度越极致越安全；若今日放量下杀跌穿昨日开盘价，属于“首日主力诱多出货”，坚决放弃绝不接盘。",
    },
}

# 辅助关键字映射表（用于通过策略描述反查 Playbook，按最具体优先排序）
_KEYWORD_TO_STRATEGY = {
    "RSI 极限超跌": "RSIDivergenceReboundStrategy",
    "双底背离": "RSIDivergenceReboundStrategy",
    "RSI": "RSIDivergenceReboundStrategy",
    "MACD水下金叉": "CDSignalStrategy",
    "MACD": "CDSignalStrategy",
    "MA5 上穿 MA10": "MACrossStrategy",
    "MA50多头且放量": "VolumeSurgeStrategy",
    "成交额放量上涨": "VolumeSurgeStrategy",
    "强趋势缩量回踩": "TrendPullbackStrategy",
    "回踩 MA20": "TrendPullbackStrategy",
    "52周历史新高": "NewHighBreakoutStrategy",
    "新高": "NewHighBreakoutStrategy",
    "波动率挤压": "TTMSqueezeStrategy",
    "TTM": "TTMSqueezeStrategy",
    "机构口袋支点": "PocketPivotStrategy",
    "口袋支点": "PocketPivotStrategy",
    "Pocket": "PocketPivotStrategy",
    "仙股低位极度死寂": "PennyBaseIgnitionStrategy",
    "死寂放量起爆": "PennyBaseIgnitionStrategy",
    "量价背离与暗度陈仓": "PennyStealthAccumulationStrategy",
    "暗度陈仓": "PennyStealthAccumulationStrategy",
    "仙股潜伏": "PennyStealthAccumulationStrategy",
    "威科夫弹簧坑": "PennyWyckoffSpringStrategy",
    "威科夫": "PennyWyckoffSpringStrategy",
    "弹簧坑": "PennyWyckoffSpringStrategy",
    "次日缩量孕线": "PennyInsideBarRetestStrategy",
    "孕线": "PennyInsideBarRetestStrategy",
    "底背离": "CDSignalStrategy",
}


def get_strategy_playbook(
    identifier: str, description: Optional[str] = None
) -> Dict[str, str]:
    """
    根据策略类名、标识符或描述文本，检索对应的实战指南字典。

    :param identifier: 策略类名（如 CDSignalStrategy）或名称
    :param description: 可选的策略描述字符串
    :return: 包含 purpose, entry, holding_period, take_profit, stop_loss, tips 的字典
    """
    if identifier in STRATEGY_PLAYBOOKS:
        return STRATEGY_PLAYBOOKS[identifier]

    # 尝试按描述中的关键字反查
    search_text = f"{identifier} {description or ''}"
    for kw, strategy_name in _KEYWORD_TO_STRATEGY.items():
        if kw in search_text and strategy_name in STRATEGY_PLAYBOOKS:
            return STRATEGY_PLAYBOOKS[strategy_name]

    # 兜底返回默认实战指南
    desc = description or identifier
    return {
        "name": identifier,
        "title": identifier,
        "category": "通用策略",
        "purpose": desc,
        "entry": "信号确认日尾盘或次日开盘确认企稳后介入。",
        "holding_period": "3 ~ 10 个交易日。",
        "take_profit": "短线冲高 +8% ~ +15% 分批止盈锁定利润。",
        "stop_loss": "跌破信号日关键支撑位或亏损达到 3%~5% 严格止损。",
        "tips": "顺应大盘趋势，严格控制单笔仓位，切忌逆势抗单。",
    }


def format_playbook_html(playbook: Dict[str, str]) -> str:
    """
    生成适用于各类邮件客户端（QQ邮箱、Gmail、Outlook等）的实战指南 HTML 卡片。
    采用高兼容性内联 CSS，双主题自适应浅蓝底色。
    """
    category_badge = ""
    if playbook.get("category"):
        category_badge = (
            f'<span style="display: inline-block; background-color: #e0f2fe; color: #0369a1; '
            f'font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 9999px; '
            f'margin-left: 8px; vertical-align: middle;">{playbook["category"]}</span>'
        )

    html = f"""
<div style="background-color: #f8fafc; border-left: 4px solid #2563eb; border-radius: 6px; padding: 12px 16px; margin: 8px 0 14px 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; font-size: 13px; line-height: 1.6; color: #334155; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
  <div style="margin-bottom: 8px;">
    <strong style="color: #1e40af; font-size: 13.5px;">🎯 策略目的：</strong>
    <span style="color: #1e293b;">{playbook.get('purpose', '')}</span>
    {category_badge}
  </div>
  <div style="background-color: #ffffff; border: 1px solid #e2e8f0; border-radius: 4px; padding: 8px 12px; margin-top: 6px;">
    <div style="margin-bottom: 4px; font-weight: 700; color: #0f766e;">🎮 实战玩法指引：</div>
    <div style="color: #475569; font-size: 12.5px;">
      <div style="margin: 3px 0;">• <strong style="color: #334155;">入场时机：</strong>{playbook.get('entry', '')}</div>
      <div style="margin: 3px 0;">• <strong style="color: #334155;">持仓周期：</strong>{playbook.get('holding_period', '')}</div>
      <div style="margin: 3px 0;">• <strong style="color: #334155;">止盈目标：</strong>{playbook.get('take_profit', '')}</div>
      <div style="margin: 3px 0;">• <strong style="color: #334155;">止损防守：</strong>{playbook.get('stop_loss', '')}</div>
      <div style="margin: 3px 0;">• <strong style="color: #334155;">核心要点：</strong><span style="color: #b91c1c;">{playbook.get('tips', '')}</span></div>
    </div>
  </div>
</div>
"""
    return html.strip()


def format_playbook_text(playbook: Dict[str, str]) -> str:
    """
    生成适用于控制台纯文本输出的实战指南。
    """
    lines = [
        f"🎯 策略目的: {playbook.get('purpose', '')}",
        f"🎮 实战玩法: [入场] {playbook.get('entry', '')} | [周期] {playbook.get('holding_period', '')}",
        f"           [止盈] {playbook.get('take_profit', '')} | [止损] {playbook.get('stop_loss', '')}",
        f"💡 核心要点: {playbook.get('tips', '')}",
    ]
    return "\n".join(lines)
