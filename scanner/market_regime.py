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


@dataclass
class MarketRegimeResult:
    regime_name: str         # "顺风多头进攻期" / "高位超买预警期" / "逆风回踩蓄力期" / "空头破位防守期"
    regime_badge: str        # "🚀 多头进攻" / "⚠️ 超买预警" / "🛡️ 逆风蓄力" / "❄️ 恐慌防守"
    temperature: str         # "80°C (火热)" / "92°C (过热)" / "45°C (温凉)" / "22°C (冰点)"
    bias_color: str          # "#10b981" (绿/涨) / "#f59e0b" (橙) / "#3b82f6" (蓝) / "#ef4444" (红)
    tactical_guidance: str   # 针对性操作指引
    qqq: Optional[IndexSnapshot] = None
    spy: Optional[IndexSnapshot] = None

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

        return f"""
<div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-left: 6px solid {self.bias_color}; border-radius: 8px; padding: 14px 18px; margin-bottom: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
  <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
    <span style="font-size: 16px; font-weight: 700; color: #1e293b;">🌍 大盘宏观感知 (Market Regime)</span>
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
    <div style="background-color: #ffffff; border: 1px dashed #cbd5e1; border-radius: 6px; padding: 8px 12px; margin-top: 8px; font-size: 13px; color: #0f172a;">
      💡 <b>战术操作指引：</b>{self.tactical_guidance}
    </div>
  </div>
</div>
"""


def _calc_snapshot(symbol: str, df: pd.DataFrame) -> Optional[IndexSnapshot]:
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
    )


def detect_market_regime(datasource) -> MarketRegimeResult:
    """
    通过行情源拉取 QQQ 和 SPY 历史，评估当前大盘宏观状态与操作指引
    """
    qqq_df = None
    spy_df = None
    try:
        qqq_df = datasource.history("QQQ", days=60)
    except Exception as e:
        print(f"[WARN] 获取 QQQ 历史行情失败: {e}")

    try:
        spy_df = datasource.history("SPY", days=60)
    except Exception as e:
        print(f"[WARN] 获取 SPY 历史行情失败: {e}")

    qqq_snap = _calc_snapshot("QQQ", qqq_df)
    spy_snap = _calc_snapshot("SPY", spy_df)

    # 依据 QQQ（主科技/成长）与 SPY（大盘基准）综合定级
    ref = qqq_snap if qqq_snap else spy_snap
    if not ref:
        return MarketRegimeResult(
            regime_name="大盘数据未就绪",
            regime_badge="⚪ 未知状态",
            temperature="--",
            bias_color="#64748b",
            tactical_guidance="无法获取大盘基准指数行情，请维持常规风控仓位。",
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
    )
