import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class TrendPullbackStrategy(BaseStrategy):
    """
    强趋势缩量回踩策略 (Trend Pullback Strategy)

    核心交易逻辑：
    1. 中长线多头趋势：Close > MA50，MA20 >= MA50，若上市满200天要求 MA50 > MA200；
    2. 近期有冲高波段：过去 10 天内最高价曾脱离 MA20 至少 2.5%；
    3. 回踩 MA20 支撑：当日 Low 触及 MA20 上下 1.5% 支撑带，且收盘价稳稳守住 MA20；
    4. 缩量洗盘：当日成交额低于 20 日均量的 75%（无主力抛盘）；
    5. 企稳阳线：当日收盘价 >= 开盘价（收阳线或企稳十字星）。
    """

    def __init__(
        self,
        support_tolerance: float = 0.015,
        max_volume_ratio: float = 0.75,
        min_avg_dollar_vol: float = 5_000_000.0,
    ) -> None:
        self.days_needed: int = 210  # 需要足够行数计算 MA200 与 MA50
        self.support_tolerance = support_tolerance
        self.max_volume_ratio = max_volume_ratio
        self.min_avg_dollar_vol = min_avg_dollar_vol

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "强趋势缩量回踩 MA20 支撑策略 (按贴合度排序)"

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        return self.days_needed

    def get_required_fields(self) -> list[FieldKey]:
        return [
            FieldKey.CLOSE,
            FieldKey.OPEN,
            FieldKey.HIGH,
            FieldKey.LOW,
            FieldKey.DOLLAR_VOLUME,
            FieldKey.MA20,
            FieldKey.MA50,
            FieldKey.MA200,
        ]

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) < self.days_needed - 1:
            return False

        # --- 1. 流动性过滤 ---
        past_20_vol = history[FieldKey.DOLLAR_VOLUME.value].iloc[-20:]
        avg_dollar_vol_20 = past_20_vol.mean()
        if pd.isna(avg_dollar_vol_20) or avg_dollar_vol_20 < self.min_avg_dollar_vol:
            return False

        # --- 2. 大趋势多头排列过滤 ---
        today_close = today[FieldKey.CLOSE.value]
        today_ma20 = today[FieldKey.MA20.value]
        today_ma50 = today[FieldKey.MA50.value]
        today_ma200 = today[FieldKey.MA200.value]

        if pd.isna(today_close) or pd.isna(today_ma20) or pd.isna(today_ma50):
            return False

        # 中线趋势：收盘在 MA50 上方，且 MA20 在 MA50 上方
        if today_close <= today_ma50 or today_ma20 < today_ma50:
            return False

        # 长线趋势：若 MA200 有效，则要求处于牛市多头（MA50 > MA200）
        if not pd.isna(today_ma200) and today_ma50 < today_ma200:
            return False

        # --- 3. 近期冲高确认（过去 10 天曾高出 MA20 至少 2.5%）---
        recent_10 = history.iloc[-10:]
        max_recent_high = recent_10[FieldKey.HIGH.value].max()
        if pd.isna(max_recent_high) or max_recent_high < today_ma20 * 1.025:
            return False

        # --- 4. 回踩 MA20 支撑位且收盘守住 ---
        today_low = today[FieldKey.LOW.value]
        # 最低价触及 MA20 支撑区间（不高于 MA20 * (1 + tolerance)）
        touched_support = today_low <= today_ma20 * (1 + self.support_tolerance)
        # 收盘价守住支撑位（未实质跌破 MA20 * (1 - tolerance)）
        held_support = today_close >= today_ma20 * (1 - self.support_tolerance)

        if not (touched_support and held_support):
            return False

        # --- 5. 缩量洗盘确认（当日成交额 < 20 日均量的 75%）---
        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]
        if pd.isna(today_dollar_vol) or today_dollar_vol <= 0:
            return False

        vol_ratio = today_dollar_vol / avg_dollar_vol_20
        if vol_ratio >= self.max_volume_ratio:
            return False

        # --- 6. 阳线或平盘企稳确认（避免中大阴线砸盘）---
        today_open = today[FieldKey.OPEN.value]
        if pd.isna(today_open) or today_close < today_open:
            return False

        return True

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        today_close = today[FieldKey.CLOSE.value]
        today_ma20 = today[FieldKey.MA20.value]
        past_20_vol = history[FieldKey.DOLLAR_VOLUME.value].iloc[-20:]
        avg_dollar_vol_20 = past_20_vol.mean()
        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]

        dist_pct = (today_close - today_ma20) / today_ma20 * 100
        vol_pct = (today_dollar_vol / avg_dollar_vol_20) * 100

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "MA20": f"${today_ma20:.2f}",
            "Support Dist %": f"{dist_pct:+.2f}%",
            "Vol vs 20d %": f"{vol_pct:.1f}%",
            "Avg Dollar Vol (20d)": f"${avg_dollar_vol_20:,.2f}",
        }

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        # 按离 MA20 支撑位贴合度升序排序（最贴合支撑位的排前面）
        return "Support Dist %"

    def is_sort_ascending(self) -> bool:
        return True
