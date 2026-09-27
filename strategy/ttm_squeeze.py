import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class TTMSqueezeStrategy(BaseStrategy):
    """
    波动率挤压变盘突破策略 (TTM Squeeze Strategy)

    核心交易逻辑：
    1. 充分蓄力：过去 10 个交易日内至少有 3 天处于 Squeeze 挤压状态（布林带在肯特纳通道内）；
    2. 变盘点火 (Squeeze Fired)：昨日或前日仍在挤压，今日脱离挤压且布林带上轨向外扩张；
    3. 向上方向确认：收盘价站在布林带中轨（20日均线）上方；
    4. 强势实体阳线：当日收盘价高于开盘价且涨幅为正；
    5. 流动性过滤：过去 20 天日均成交额 >= 10,000,000 美元。
    """

    def __init__(
        self,
        min_squeeze_days: int = 3,
        min_change_pct: float = 0.5,
        min_avg_dollar_vol: float = 10_000_000.0,
    ) -> None:
        self.days_needed: int = 60  # 60 天历史足以稳定计算 20 EMA, 20 SMA 与 20 ATR
        self.min_squeeze_days = min_squeeze_days
        self.min_change_pct = min_change_pct
        self.min_avg_dollar_vol = min_avg_dollar_vol

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "波动率挤压变盘突破策略 (TTM Squeeze，按涨幅排序)"

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
            FieldKey.BB_UPPER,
            FieldKey.BB_LOWER,
            FieldKey.KC_UPPER,
            FieldKey.KC_LOWER,
            FieldKey.SQUEEZE_ON,
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

        # --- 2. 过去 10 天蓄力天数确认 ---
        past_10_squeeze = history[FieldKey.SQUEEZE_ON.value].iloc[-10:]
        squeeze_count = int(past_10_squeeze.sum())
        if squeeze_count < self.min_squeeze_days:
            return False

        # --- 3. 点火释放状态确认 (Squeeze Fired) ---
        prev_squeeze = bool(history[FieldKey.SQUEEZE_ON.value].iloc[-1])
        prev2_squeeze = bool(history[FieldKey.SQUEEZE_ON.value].iloc[-2])
        if not (prev_squeeze or prev2_squeeze):
            return False  # 如果最近两天完全不在挤压中，说明并非刚启动的变盘第一天

        today_squeeze = bool(today[FieldKey.SQUEEZE_ON.value])
        today_bb_upper = today[FieldKey.BB_UPPER.value]
        today_kc_upper = today[FieldKey.KC_UPPER.value]

        # 变盘开花：今天脱离了挤压状态，或者布林带上轨已突破肯特纳上轨
        is_fired = (not today_squeeze) or (today_bb_upper >= today_kc_upper)
        if not is_fired:
            return False

        # --- 4. 向上变盘与强势形态确认 ---
        today_close = today[FieldKey.CLOSE.value]
        today_bb_lower = today[FieldKey.BB_LOWER.value]
        today_bb_mid = (today_bb_upper + today_bb_lower) / 2.0

        # 收盘价必须在 20 周期中轨上方
        if today_close < today_bb_mid:
            return False

        today_open = today[FieldKey.OPEN.value]
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        change_pct = (today_close - prev_close) / prev_close * 100

        # 收阳线且涨幅达到门槛
        if today_close <= today_open or change_pct < self.min_change_pct:
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
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        change_pct = (today_close - prev_close) / prev_close * 100

        past_10_squeeze = history[FieldKey.SQUEEZE_ON.value].iloc[-10:]
        squeeze_count = int(past_10_squeeze.sum())

        past_20_vol = history[FieldKey.DOLLAR_VOLUME.value].iloc[-20:]
        avg_dollar_vol_20 = past_20_vol.mean()

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Change %": f"+{change_pct:.2f}%",
            "Squeeze (10d)": f"{squeeze_count} days",
            "Status": "Squeeze Fired 🚀",
            "Avg Dollar Vol (20d)": f"${avg_dollar_vol_20:,.2f}",
        }

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        # 按当日涨幅降序排列
        return "Change %"

    def is_sort_ascending(self) -> bool:
        return False
