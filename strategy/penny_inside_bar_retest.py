import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class PennyInsideBarRetestStrategy(BaseStrategy):
    """
    仙股首日放量次日缩量孕线接力策略 (Penny Inside Bar Retest Strategy)

    核心交易逻辑（彻底解决仙股首日盘中难买、买在长上影线的高危痛点）：
    1. 仙股价格定位：$0.20 <= Close <= $5.00，当日成交额 >= $200,000；
    2. 昨日首日点火大阳线 (Day 1 Ignition)：
       - 昨日涨幅 >= +12.0% 的实体大阳线 (Prev_Close > Prev_Open)；
       - 昨日成交量 >= 过去 20 日均量的 2.0 倍以上（游资主力入场建仓，换手活跃）；
    3. 今日缩量孕线洗盘 (Day 2 Retest & Inside Bar)：
       - 极致缩量：今日成交量 <= 昨日成交量的 55%（无主力出逃抛压，纯属散户洗盘）；
       - 坚决守位：今日最低价稳守在昨日阳线实体 50% 中轴之上 (Today_Low >= Prev_Open + (Prev_Close - Prev_Open) * 0.45)；
       - 孕线形态：今日未被大阴线砸穿，收盘位于日内振幅中上区域；
    4. 极小止损与高盈亏比接力：买在第 2 天洗盘缩量临界点，止损仅需挂在今日孕线低点下方 2%~3%，博弈第 2 浪/第 3 浪主升暴拉。
    """

    def __init__(
        self,
        min_price: float = 0.20,
        max_price: float = 5.00,
        min_prev_gain_pct: float = 12.0,
        max_today_vol_ratio: float = 0.55,
        min_today_dollar_vol: float = 200_000.0,
    ) -> None:
        self.days_needed: int = 30
        self.min_price = min_price
        self.max_price = max_price
        self.min_prev_gain_pct = min_prev_gain_pct
        self.max_today_vol_ratio = max_today_vol_ratio
        self.min_today_dollar_vol = min_today_dollar_vol

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "仙股首日放量次日缩量孕线策略 (按回踩缩量程度排序)"

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        return self.days_needed

    def get_required_fields(self) -> list[FieldKey]:
        return [
            FieldKey.CLOSE,
            FieldKey.OPEN,
            FieldKey.HIGH,
            FieldKey.LOW,
            FieldKey.VOLUME,
            FieldKey.DOLLAR_VOLUME,
        ]

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        # 缩量越极致越靠前
        return "Vol Retract %"

    def is_sort_ascending(self) -> bool:
        return True

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) < self.days_needed - 1:
            return False

        # --- 1. 仙股价格与当日成交额过滤 ---
        today_close = today[FieldKey.CLOSE.value]
        if pd.isna(today_close) or today_close < self.min_price or today_close > self.max_price:
            return False

        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]
        if pd.isna(today_dollar_vol) or today_dollar_vol < self.min_today_dollar_vol:
            return False

        # --- 2. 昨日首日点火大阳线确认 ---
        prev_row = history.iloc[-1]
        prior_close = history[FieldKey.CLOSE.value].iloc[-2]

        prev_close = prev_row[FieldKey.CLOSE.value]
        prev_open = prev_row[FieldKey.OPEN.value]
        prev_volume = prev_row[FieldKey.VOLUME.value]

        if pd.isna(prev_close) or pd.isna(prev_open) or pd.isna(prior_close) or prior_close <= 0:
            return False

        prev_gain_pct = (prev_close - prior_close) / prior_close * 100
        # 昨日涨幅 >= 12% 且为实体阳线
        if prev_gain_pct < self.min_prev_gain_pct or prev_close <= prev_open:
            return False

        # 昨日成交量必须放量 >= 20 日均量的 2.0 倍
        full_volume = pd.concat(
            [history[FieldKey.VOLUME.value], pd.Series([today[FieldKey.VOLUME.value]])],
            ignore_index=True,
        )
        avg_vol_20 = full_volume.iloc[-22:-2].mean()  # 点火前 20 天均量
        if pd.isna(avg_vol_20) or avg_vol_20 <= 0 or prev_volume < avg_vol_20 * 2.0:
            return False

        # --- 3. 今日缩量洗盘确认 (Volume <= 昨日的 55%) ---
        today_volume = today[FieldKey.VOLUME.value]
        if pd.isna(today_volume) or today_volume <= 0 or prev_volume <= 0:
            return False

        vol_retract_ratio = today_volume / prev_volume
        if vol_retract_ratio > self.max_today_vol_ratio:
            return False

        # --- 4. 今日价格守位：最低价守住昨日阳线实体的 45%~50% 中轴 ---
        today_low = today[FieldKey.LOW.value]
        today_high = today[FieldKey.HIGH.value]
        if pd.isna(today_low) or pd.isna(today_high):
            return False

        entity_midpoint = prev_open + (prev_close - prev_open) * 0.45
        if today_low < entity_midpoint:
            return False  # 回踩过深说明主力护盘意愿弱

        # 今日收盘没有形成暴跌长阴线
        today_open = today[FieldKey.OPEN.value]
        if (today_open - today_close) / today_open > 0.05:
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
        today_open = today[FieldKey.OPEN.value]
        prev_row = history.iloc[-1]
        prior_close = history[FieldKey.CLOSE.value].iloc[-2]

        prev_close = prev_row[FieldKey.CLOSE.value]
        prev_gain_pct = (prev_close - prior_close) / prior_close * 100
        prev_volume = prev_row[FieldKey.VOLUME.value]

        today_volume = today[FieldKey.VOLUME.value]
        vol_retract_ratio = (today_volume / prev_volume * 100) if prev_volume > 0 else 0.0
        today_change_pct = (today_close - prev_close) / prev_close * 100

        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Change %": f"{'+' if today_change_pct>=0 else ''}{today_change_pct:.2f}%",
            "Day1 Gain %": f"+{prev_gain_pct:.1f}%",
            "Vol Retract %": f"{vol_retract_ratio:.1f}%",
            "Dollar Volume": f"${today_dollar_vol:,.2f}",
        }
