import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class NewHighBreakoutStrategy(BaseStrategy):
    """
    52周历史新高 / 平台放量突破策略 (New 52-Week High Breakout Strategy)

    核心交易逻辑：
    1. 突破新高：当日收盘价创过去 250 天（约 52 周）最高收盘价，且突破过去最高价；次新股兼容 ATH；
    2. 放量确认：当日成交额 > 过去 50 天平均成交额的 1.5 倍（机构资金加速进场）；
    3. 强势收盘：收盘价落在全天振幅的上 25% 区域，上影线极短，剔除长上影“假突破”；
    4. 实体大阳线：当日涨幅 >= 2.0% 且当日收盘价 > 开盘价；
    5. 流动性门槛：过去 50 天日均成交额 >= 10,000,000 美元（高流动性优质标的）。
    """

    def __init__(
        self,
        min_change_pct: float = 2.0,
        min_volume_ratio: float = 1.5,
        min_avg_dollar_vol: float = 10_000_000.0,
        max_lookback_days: int = 250,
    ) -> None:
        self.days_needed: int = 251  # 250 天历史 + 当日
        self.min_change_pct = min_change_pct
        self.min_volume_ratio = min_volume_ratio
        self.min_avg_dollar_vol = min_avg_dollar_vol
        self.max_lookback_days = max_lookback_days

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "52周历史新高/平台放量突破策略 (按涨幅排序)"

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
        ]

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        # 至少需要 60 根历史 K 线以支撑流动性均线与平台高点计算
        if len(history) < 60:
            return False

        # --- 1. 流动性过滤（过去 50 天均成交额）---
        lookback_vol = min(50, len(history))
        avg_dollar_vol_50 = (
            history[FieldKey.DOLLAR_VOLUME.value].iloc[-lookback_vol:].mean()
        )
        if pd.isna(avg_dollar_vol_50) or avg_dollar_vol_50 < self.min_avg_dollar_vol:
            return False

        # --- 2. 涨幅与实体阳线过滤 ---
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        today_close = today[FieldKey.CLOSE.value]
        today_open = today[FieldKey.OPEN.value]

        if pd.isna(prev_close) or prev_close <= 0 or pd.isna(today_close):
            return False

        change_pct = (today_close - prev_close) / prev_close * 100
        # 涨幅需 >= 门槛，且收盘高于开盘（实体阳线）
        if change_pct < self.min_change_pct or today_close <= today_open:
            return False

        # --- 3. 强势收盘确认（收在全天振幅的上 25% 区域，防长上影假突破）---
        today_high = today[FieldKey.HIGH.value]
        today_low = today[FieldKey.LOW.value]
        candle_range = today_high - today_low

        if candle_range > 0:
            if today_close < today_low + candle_range * 0.75:
                return False

        # --- 4. 放量确认（当日成交额 >= 50 日均额的 1.5 倍）---
        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]
        if pd.isna(today_dollar_vol) or today_dollar_vol <= 0:
            return False

        vol_ratio = today_dollar_vol / avg_dollar_vol_50
        if vol_ratio < self.min_volume_ratio:
            return False

        # --- 5. 新高突破确认（52 周 / 历史最高收盘价与最高价突破）---
        lookback_high = min(self.max_lookback_days, len(history))
        past_window = history.iloc[-lookback_high:]

        past_max_close = past_window[FieldKey.CLOSE.value].max()
        past_max_high = past_window[FieldKey.HIGH.value].max()

        if pd.isna(past_max_close) or pd.isna(past_max_high):
            return False

        # 收盘价创过去区间新高，且今日最高价创出新高
        is_breakout = (today_close > past_max_close) and (today_high >= past_max_high)
        return bool(is_breakout)

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        today_close = today[FieldKey.CLOSE.value]
        change_pct = (today_close - prev_close) / prev_close * 100

        lookback_vol = min(50, len(history))
        avg_dollar_vol_50 = (
            history[FieldKey.DOLLAR_VOLUME.value].iloc[-lookback_vol:].mean()
        )
        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]
        vol_ratio = today_dollar_vol / avg_dollar_vol_50

        lookback_high = min(self.max_lookback_days, len(history))
        breakout_type = (
            "52-Week High" if lookback_high >= 240 else f"ATH ({lookback_high}d)"
        )

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Change %": f"+{change_pct:.2f}%",
            "Ratio (vs 50d)": float(round(vol_ratio, 2)),
            "Breakout Type": breakout_type,
            "Avg Dollar Vol (50d)": f"${avg_dollar_vol_50:,.2f}",
        }

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        # 按当日涨幅降序排列（当日最强势的领涨标的排在最前）
        return "Change %"

    def is_sort_ascending(self) -> bool:
        return False
