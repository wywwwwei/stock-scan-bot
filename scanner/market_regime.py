import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class IndexSnapshot:
    symbol: str
    close: float
    change_1d_pct: float
    change_5d_pct: float
    ma20: float
    ma50: float
    dist_ma20_pct: float
    dist_ma50_pct: float
    rsi14: float
    is_above_ma20: bool
    is_above_ma50: bool
    name: str = ""
    unit: str = ""


@dataclass
class MarketRegimeResult:
    regime_name: str         # "顺风多头进攻期" / "高位超买预警期" / "逆风回踩蓄力期" / "空头破位防守期"
    regime_badge: str        # "🚀 多头进攻" / "⚠️ 超买预警" / "🛡️ 逆风蓄力" / "❄️ 恐慌防守"
    temperature: str         # "80°C (火热)" / "92°C (过热)" / "45°C (温凉)" / "22°C (冰点)"
    bias_color: str          # "#10b981" (绿/涨) / "#f59e0b" (橙) / "#3b82f6" (蓝) / "#ef4444" (红)
    tactical_guidance: str   # 针对性操作指引
    qqq: Optional[IndexSnapshot] = None
    spy: Optional[IndexSnapshot] = None
    # 全球大类资产联动：金银、油气、加密货币
    gold: Optional[IndexSnapshot] = None    # 黄金 (GC=F)
    silver: Optional[IndexSnapshot] = None  # 白银 (SI=F)
    oil: Optional[IndexSnapshot] = None     # WTI 原油 (CL=F)
    gas: Optional[IndexSnapshot] = None     # 天然气 (NG=F)
    btc: Optional[IndexSnapshot] = None     # 比特币 (BTC-USD)
    eth: Optional[IndexSnapshot] = None     # 以太币 (ETH-USD)
    # 中国长短端利率债与信用债联动
    cgb_30y: Optional[IndexSnapshot] = None    # 30年国债ETF (511090.SS, 超长端利率)
    cgb_10y: Optional[IndexSnapshot] = None    # 10年国债ETF (511260.SS, 长端基准利率)
    cgb_short: Optional[IndexSnapshot] = None  # 短融ETF (511360.SS, 短端利率与流动性)
    credit_chengtou: Optional[IndexSnapshot] = None  # 城投债ETF (511220.SS, 城投信用)
    credit_corp: Optional[IndexSnapshot] = None      # 公司债ETF (511030.SS, 产业公司信用)

    def to_text(self) -> str:
        lines = [
            "==================================================",
            f"🌍 大盘宏观感知 (Market Regime): {self.regime_badge}",
            "--------------------------------------------------",
            f"市场状态: 【{self.regime_name}】 | 市场情绪温度: {self.temperature}",
        ]
        if self.qqq:
            lines.append(
                f"纳指 QQQ: ${self.qqq.close:.2f} ({'+' if self.qqq.change_1d_pct>=0 else ''}{self.qqq.change_1d_pct:.2f}%) | "
                f"vs MA20: {'+' if self.qqq.dist_ma20_pct>=0 else ''}{self.qqq.dist_ma20_pct:.2f}% | "
                f"RSI(14): {self.qqq.rsi14:.1f}"
            )
        if self.spy:
            lines.append(
                f"标普 SPY: ${self.spy.close:.2f} ({'+' if self.spy.change_1d_pct>=0 else ''}{self.spy.change_1d_pct:.2f}%) | "
                f"vs MA20: {'+' if self.spy.dist_ma20_pct>=0 else ''}{self.spy.dist_ma20_pct:.2f}% | "
                f"RSI(14): {self.spy.rsi14:.1f}"
            )

        # 宏观大类资产联动
        macro_lines = []
        if self.gold or self.silver:
            metals = []
            if self.gold:
                metals.append(f"黄金 ${self.gold.close:.2f} ({'+' if self.gold.change_1d_pct>=0 else ''}{self.gold.change_1d_pct:.2f}%)")
            if self.silver:
                metals.append(f"白银 ${self.silver.close:.2f} ({'+' if self.silver.change_1d_pct>=0 else ''}{self.silver.change_1d_pct:.2f}%)")
            macro_lines.append(f"🪙 贵金属: {' | '.join(metals)}")

        if self.oil or self.gas:
            energies = []
            if self.oil:
                energies.append(f"WTI原油 ${self.oil.close:.2f} ({'+' if self.oil.change_1d_pct>=0 else ''}{self.oil.change_1d_pct:.2f}%)")
            if self.gas:
                energies.append(f"天然气 ${self.gas.close:.2f} ({'+' if self.gas.change_1d_pct>=0 else ''}{self.gas.change_1d_pct:.2f}%)")
            macro_lines.append(f"🛢️ 油气能源: {' | '.join(energies)}")

        if self.btc or self.eth:
            cryptos = []
            if self.btc:
                cryptos.append(f"BTC ${self.btc.close:,.2f} ({'+' if self.btc.change_1d_pct>=0 else ''}{self.btc.change_1d_pct:.2f}%)")
            if self.eth:
                cryptos.append(f"ETH ${self.eth.close:,.2f} ({'+' if self.eth.change_1d_pct>=0 else ''}{self.eth.change_1d_pct:.2f}%)")
            macro_lines.append(f"⚡ 数字资产: {' | '.join(cryptos)}")

        if self.cgb_30y or self.cgb_10y or self.cgb_short:
            cgbs = []
            if self.cgb_30y:
                cgbs.append(f"30年超长债 ${self.cgb_30y.close:.2f} ({'+' if self.cgb_30y.change_1d_pct>=0 else ''}{self.cgb_30y.change_1d_pct:.2f}%)")
            if self.cgb_10y:
                cgbs.append(f"10年基准债 ${self.cgb_10y.close:.2f} ({'+' if self.cgb_10y.change_1d_pct>=0 else ''}{self.cgb_10y.change_1d_pct:.2f}%)")
            if self.cgb_short:
                cgbs.append(f"短融 ${self.cgb_short.close:.2f} ({'+' if self.cgb_short.change_1d_pct>=0 else ''}{self.cgb_short.change_1d_pct:.2f}%)")
            macro_lines.append(f"🇨🇳 中国利率债: {' | '.join(cgbs)}")

        if self.credit_chengtou or self.credit_corp:
            credits = []
            if self.credit_chengtou:
                credits.append(f"城投债 ${self.credit_chengtou.close:.2f} ({'+' if self.credit_chengtou.change_1d_pct>=0 else ''}{self.credit_chengtou.change_1d_pct:.2f}%)")
            if self.credit_corp:
                credits.append(f"公司债 ${self.credit_corp.close:.2f} ({'+' if self.credit_corp.change_1d_pct>=0 else ''}{self.credit_corp.change_1d_pct:.2f}%)")
            macro_lines.append(f"🏢 中国信用债: {' | '.join(credits)}")

        if macro_lines:
            lines.append("--------------------------------------------------")
            lines.extend(macro_lines)
            lines.append("--------------------------------------------------")

        lines.append(f"战术指引: {self.tactical_guidance}")
        lines.append("==================================================")
        return "\n".join(lines)

    def to_html(self) -> str:
        qqq_desc = "--"
        if self.qqq:
            q_color = "#16a34a" if self.qqq.change_1d_pct >= 0 else "#dc2626"
            qqq_desc = (
                f"<b>${self.qqq.close:.2f}</b> "
                f"(<span style='color:{q_color}'><b>{'+' if self.qqq.change_1d_pct>=0 else ''}{self.qqq.change_1d_pct:.2f}%</b></span>) | "
                f"距MA20: <b>{'+' if self.qqq.dist_ma20_pct>=0 else ''}{self.qqq.dist_ma20_pct:.2f}%</b> | "
                f"RSI: <b>{self.qqq.rsi14:.1f}</b>"
            )

        spy_desc = "--"
        if self.spy:
            s_color = "#16a34a" if self.spy.change_1d_pct >= 0 else "#dc2626"
            spy_desc = (
                f"<b>${self.spy.close:.2f}</b> "
                f"(<span style='color:{s_color}'><b>{'+' if self.spy.change_1d_pct>=0 else ''}{self.spy.change_1d_pct:.2f}%</b></span>) | "
                f"距MA20: <b>{'+' if self.spy.dist_ma20_pct>=0 else ''}{self.spy.dist_ma20_pct:.2f}%</b> | "
                f"RSI: <b>{self.spy.rsi14:.1f}</b>"
            )

        def _format_asset_badge(snap: Optional[IndexSnapshot]) -> str:
            if not snap:
                return "--"
            color = "#16a34a" if snap.change_1d_pct >= 0 else "#dc2626"
            sign = "+" if snap.change_1d_pct >= 0 else ""
            price_str = f"${snap.close:,.2f}" if snap.close >= 1000 else f"${snap.close:.2f}"
            return f"<b>{price_str}</b> (<span style='color:{color}'><b>{sign}{snap.change_1d_pct:.2f}%</b></span>)"

        macro_html = ""
        has_macro = any([
            self.gold, self.silver, self.oil, self.gas, self.btc, self.eth,
            self.cgb_30y, self.cgb_10y, self.cgb_short, self.credit_chengtou, self.credit_corp
        ])
        if has_macro:
            macro_items = []
            if self.gold or self.silver:
                g_str = _format_asset_badge(self.gold)
                s_str = _format_asset_badge(self.silver)
                macro_items.append(
                    f"<div style='margin-bottom: 4px;'>🪙 <b>贵金属：</b>黄金 {g_str} &nbsp;|&nbsp; 白银 {s_str}</div>"
                )
            if self.oil or self.gas:
                o_str = _format_asset_badge(self.oil)
                ga_str = _format_asset_badge(self.gas)
                macro_items.append(
                    f"<div style='margin-bottom: 4px;'>🛢️ <b>油气能源：</b>WTI原油 {o_str} &nbsp;|&nbsp; 天然气 {ga_str}</div>"
                )
            if self.btc or self.eth:
                b_str = _format_asset_badge(self.btc)
                e_str = _format_asset_badge(self.eth)
                macro_items.append(
                    f"<div style='margin-bottom: 4px;'>⚡ <b>数字资产：</b>比特币 {b_str} &nbsp;|&nbsp; 以太币 {e_str}</div>"
                )
            if self.cgb_30y or self.cgb_10y or self.cgb_short:
                c30_str = _format_asset_badge(self.cgb_30y)
                c10_str = _format_asset_badge(self.cgb_10y)
                cs_str = _format_asset_badge(self.cgb_short)
                macro_items.append(
                    f"<div style='margin-bottom: 4px;'>🇨🇳 <b>中国利率债：</b>30年超长债 {c30_str} &nbsp;|&nbsp; 10年基准债 {c10_str} &nbsp;|&nbsp; 短融/短端 {cs_str}</div>"
                )
            if self.credit_chengtou or self.credit_corp:
                ct_str = _format_asset_badge(self.credit_chengtou)
                cp_str = _format_asset_badge(self.credit_corp)
                macro_items.append(
                    f"<div>🏢 <b>中国信用债：</b>城投债 {ct_str} &nbsp;|&nbsp; 公司债 {cp_str}</div>"
                )

            macro_html = f"""
    <div style="background-color: #ffffff; border: 1px solid #e2e8f0; border-radius: 6px; padding: 8px 12px; margin-top: 6px; font-size: 12.5px; color: #334155;">
      {''.join(macro_items)}
    </div>
"""

        return f"""
<div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-left: 6px solid {self.bias_color}; border-radius: 8px; padding: 14px 18px; margin-bottom: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
  <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
    <span style="font-size: 16px; font-weight: 700; color: #1e293b;">🌍 大盘宏观感知 (Market Regime) 与大类资产联动</span>
    <span style="background-color: {self.bias_color}; color: white; padding: 3px 10px; border-radius: 12px; font-size: 13px; font-weight: 600;">{self.regime_badge}</span>
  </div>
  <div style="font-size: 14px; color: #334155; line-height: 1.6;">
    <div style="margin-bottom: 4px;">
      <b>市场环境评级：</b><span style="color: {self.bias_color}; font-weight: 700;">{self.regime_name}</span> &nbsp;|&nbsp; 
      <b>市场情绪温度：</b>{self.temperature}
    </div>
    <div style="margin-bottom: 4px; font-size: 13px; color: #475569;">
      <b>QQQ (纳指 100):</b> {qqq_desc}<br>
      <b>SPY (标普 500):</b> {spy_desc}
    </div>
    {macro_html}
    <div style="background-color: #ffffff; border: 1px dashed #cbd5e1; border-radius: 6px; padding: 8px 12px; margin-top: 8px; font-size: 13px; color: #0f172a;">
      💡 <b>战术操作指引：</b>{self.tactical_guidance}
    </div>
  </div>
</div>
"""


def _calc_snapshot(
    symbol: str,
    df: Optional[pd.DataFrame],
    name: str = "",
    unit: str = "",
) -> Optional[IndexSnapshot]:
    if df is None or len(df) < 20:
        return None
    close = df["Close"]
    today_close = float(close.iloc[-1])
    prev_close = float(close.iloc[-2])
    change_1d = (today_close - prev_close) / prev_close * 100 if prev_close > 0 else 0.0

    c5 = float(close.iloc[-6]) if len(close) >= 6 else prev_close
    change_5d = (today_close - c5) / c5 * 100 if c5 > 0 else 0.0

    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else ma20

    dist_ma20 = (today_close - ma20) / ma20 * 100 if ma20 > 0 else 0.0
    dist_ma50 = (today_close - ma50) / ma50 * 100 if ma50 > 0 else 0.0

    # 14-period RSI
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1.0 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / 14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi_s = 100.0 - (100.0 / (1.0 + rs))
    rsi14 = float(rsi_s.fillna(50.0).iloc[-1])

    return IndexSnapshot(
        symbol=symbol,
        close=today_close,
        change_1d_pct=change_1d,
        change_5d_pct=change_5d,
        ma20=ma20,
        ma50=ma50,
        dist_ma20_pct=dist_ma20,
        dist_ma50_pct=dist_ma50,
        rsi14=rsi14,
        is_above_ma20=(today_close > ma20),
        is_above_ma50=(today_close > ma50),
        name=name,
        unit=unit,
    )


def detect_market_regime(datasource) -> MarketRegimeResult:
    """
    通过行情源拉取 QQQ、SPY 以及全球大类资产（金银、油气、加密货币、中国长短利率债与信用债）历史，
    评估当前大盘宏观状态、情绪温度与跨资产联动指引。
    """
    symbols = [
        "QQQ", "SPY", "GC=F", "SI=F", "CL=F", "NG=F", "BTC-USD", "ETH-USD",
        "511090.SS", "511260.SS", "511360.SS", "511220.SS", "511030.SS"
    ]
    histories: Dict[str, pd.DataFrame] = {}

    # 优先采用批量拉取接口
    if hasattr(datasource, "history_batch"):
        try:
            histories = datasource.history_batch(symbols, days=60)
        except Exception as e:
            print(f"[WARN] 批量获取宏观大类资产失败: {e}")

    # 对于未返回或不支持 batch 的标的，容错逐个回退拉取
    for s in symbols:
        if s not in histories or histories[s] is None or histories[s].empty:
            if hasattr(datasource, "history"):
                try:
                    df = datasource.history(s, days=60)
                    if df is not None and not df.empty:
                        histories[s] = df
                except Exception:
                    pass

    qqq_snap = _calc_snapshot("QQQ", histories.get("QQQ"), name="纳指 100")
    spy_snap = _calc_snapshot("SPY", histories.get("SPY"), name="标普 500")

    gold_snap = _calc_snapshot("GC=F", histories.get("GC=F"), name="黄金", unit="美元/盎司")
    silver_snap = _calc_snapshot("SI=F", histories.get("SI=F"), name="白银", unit="美元/盎司")
    oil_snap = _calc_snapshot("CL=F", histories.get("CL=F"), name="WTI原油", unit="美元/桶")
    gas_snap = _calc_snapshot("NG=F", histories.get("NG=F"), name="天然气", unit="美元/MMBtu")
    btc_snap = _calc_snapshot("BTC-USD", histories.get("BTC-USD"), name="比特币", unit="USD")
    eth_snap = _calc_snapshot("ETH-USD", histories.get("ETH-USD"), name="以太币", unit="USD")

    cgb_30y_snap = _calc_snapshot("511090.SS", histories.get("511090.SS"), name="30年国债ETF")
    cgb_10y_snap = _calc_snapshot("511260.SS", histories.get("511260.SS"), name="10年国债ETF")
    cgb_short_snap = _calc_snapshot("511360.SS", histories.get("511360.SS"), name="短融ETF")
    credit_chengtou_snap = _calc_snapshot("511220.SS", histories.get("511220.SS"), name="城投债ETF")
    credit_corp_snap = _calc_snapshot("511030.SS", histories.get("511030.SS"), name="公司债ETF")

    # 依据 QQQ（主科技/成长）与 SPY（大盘基准）综合定级
    ref = qqq_snap if qqq_snap else spy_snap
    if not ref:
        return MarketRegimeResult(
            regime_name="大盘数据未就绪",
            regime_badge="⚪ 未知状态",
            temperature="--",
            bias_color="#64748b",
            tactical_guidance="无法获取大盘基准指数行情，请维持常规风控仓位。",
            gold=gold_snap,
            silver=silver_snap,
            oil=oil_snap,
            gas=gas_snap,
            btc=btc_snap,
            eth=eth_snap,
            cgb_30y=cgb_30y_snap,
            cgb_10y=cgb_10y_snap,
            cgb_short=cgb_short_snap,
            credit_chengtou=credit_chengtou_snap,
            credit_corp=credit_corp_snap,
        )

    # 判断规则：
    # 1. 超买预警
    if ref.is_above_ma20 and (ref.rsi14 >= 68.0 or ref.dist_ma20_pct >= 3.5):
        return MarketRegimeResult(
            regime_name="高位超买预警期",
            regime_badge="⚠️ 超买预警",
            temperature="90°C (极度过热)",
            bias_color="#f59e0b",
            tactical_guidance="大盘涨幅过大且处于极端超买区，历史数据显示追涨极易遭遇急跌洗盘！建议分批减仓止盈，严禁追高放量阳线与高位新高。",
            qqq=qqq_snap,
            spy=spy_snap,
            gold=gold_snap,
            silver=silver_snap,
            oil=oil_snap,
            gas=gas_snap,
            btc=btc_snap,
            eth=eth_snap,
            cgb_30y=cgb_30y_snap,
            cgb_10y=cgb_10y_snap,
            cgb_short=cgb_short_snap,
            credit_chengtou=credit_chengtou_snap,
            credit_corp=credit_corp_snap,
        )

    # 2. 顺风多头进攻
    if ref.is_above_ma20 and ref.is_above_ma50 and ref.rsi14 < 68.0:
        return MarketRegimeResult(
            regime_name="顺风多头进攻期",
            regime_badge="🚀 多头进攻",
            temperature="75°C (健康活跃)",
            bias_color="#10b981",
            tactical_guidance="大盘处于健康多头顺风通道，动量持续性较好。可积极参与【机构口袋支点】、【波动率挤压变盘】等右侧起爆策略，止盈设在 +5%~+8%。",
            qqq=qqq_snap,
            spy=spy_snap,
            gold=gold_snap,
            silver=silver_snap,
            oil=oil_snap,
            gas=gas_snap,
            btc=btc_snap,
            eth=eth_snap,
            cgb_30y=cgb_30y_snap,
            cgb_10y=cgb_10y_snap,
            cgb_short=cgb_short_snap,
            credit_chengtou=credit_chengtou_snap,
            credit_corp=credit_corp_snap,
        )

    # 3. 恐慌破位防守
    if not ref.is_above_ma50 and ref.rsi14 <= 35.0:
        return MarketRegimeResult(
            regime_name="恐慌破位冰点期",
            regime_badge="❄️ 恐慌防守",
            temperature="25°C (冰点区)",
            bias_color="#ef4444",
            tactical_guidance="大盘跌破关键均线处于恐慌下行期。以现金防守为主，严禁追突破！仅允许轻仓博弈极低估的【RSI双底背离】与【威科夫极度缩量弹簧坑】短线反抽。",
            qqq=qqq_snap,
            spy=spy_snap,
            gold=gold_snap,
            silver=silver_snap,
            oil=oil_snap,
            gas=gas_snap,
            btc=btc_snap,
            eth=eth_snap,
            cgb_30y=cgb_30y_snap,
            cgb_10y=cgb_10y_snap,
            cgb_short=cgb_short_snap,
            credit_chengtou=credit_chengtou_snap,
            credit_corp=credit_corp_snap,
        )

    # 4. 逆风回踩蓄力（其余默认区间，往往为大盘震荡洗盘、拐点孕育期）
    return MarketRegimeResult(
        regime_name="逆风回踩蓄力期",
        regime_badge="🛡️ 逆风蓄力",
        temperature="45°C (震荡洗盘)",
        bias_color="#3b82f6",
        tactical_guidance="大盘处于均线回踩震荡洗盘期（历史实测此时胜率高达 60%+）。重点布局【波动率挤压变盘】与【底部威科夫/MACD底背离/仙股接力】抗跌标的，迎接共振拐点！",
        qqq=qqq_snap,
        spy=spy_snap,
        gold=gold_snap,
        silver=silver_snap,
        oil=oil_snap,
        gas=gas_snap,
        btc=btc_snap,
        eth=eth_snap,
        cgb_30y=cgb_30y_snap,
        cgb_10y=cgb_10y_snap,
        cgb_short=cgb_short_snap,
        credit_chengtou=credit_chengtou_snap,
        credit_corp=credit_corp_snap,
    )
