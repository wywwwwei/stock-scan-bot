import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class PennyBaseIgnitionStrategy(BaseStrategy):
    """
    仙股低位极度死寂放量起爆策略 (Penny Base Ignition Strategy)

    核心交易逻辑：
    1. 仙股价格定位：$0.20 <= Close <= $5.00，专注捕捉低位微盘妖股；
    2. 长期极窄箱体：过去 20 个交易日最高与最低价振幅 <= 28%，表明处于极度死寂磨底期；
    3. 均线极度粘合：昨日 MA5、MA10、MA20 三线极差 <= 5.0%，筹码持仓成本高度收敛；
    4. 今日点火破箱：今日收盘价创近 20 天收盘新高，并站上全部短期均线（Close > MA5, MA10, MA20）；
    5. 成交量惊雷：当日成交量 >= 过去 20 日均量的 3.5 倍以上；
    6. 坚决光头阳线：涨幅 >= +5.0% 且实体阳线，收盘位于全天振幅前 35% 强势区（防冲高回落诱多）；
    7. 流动性防踩踏：当日成交额 >= $2,000,000，确保起爆日交易对手盘深度充足。
    """

    def __init__(
        self,
        min_price: float = 0.20,
        max_price: float = 5.00,
        max_base_amplitude: float = 0.28,
        max_ma_spread: float = 0.05,
        min_volume_ratio: float = 3.5,
        min_change_pct: float = 0.05,
        min_close_to_high_ratio: float = 0.65,
        min_dollar_volume: float = 2_000_000.0,
    ) -> None:
        self.days_needed: int = 45  # 20 天箱体 + 20 天均线收敛预热
        self.min_price = min_price
        self.max_price = max_price
        self.max_base_amplitude = max_base_amplitude
        self.max_ma_spread = max_ma_spread
        self.min_volume_ratio = min_volume_ratio
        self.min_change_pct = min_change_pct
        self.min_close_to_high_ratio = min_close_to_high_ratio
        self.min_dollar_volume = min_dollar_volume

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "仙股低位极度死寂放量起爆策略 (按涨幅排序)"

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        return self.days_needed

    def get_required_fields(self) -> list[FieldKey]:
        return [
            FieldKey.OPEN,
            FieldKey.HIGH,
            FieldKey.LOW,
            FieldKey.CLOSE,
            FieldKey.VOLUME,
            FieldKey.DOLLAR_VOLUME,
            FieldKey.MA5,
            FieldKey.MA10,
            FieldKey.MA20,
        ]

    # ========= 排序规则 =========

    def get_sort_column(self) -> str:
        return "Change %"

    def is_sort_ascending(self) -> bool:
        return False

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) < self.days_needed - 1:
            return False

        # --- 条件 1: 仙股低价与微盘过滤 ---
        today_close = today[FieldKey.CLOSE.value]
        if pd.isna(today_close) or today_close < self.min_price or today_close > self.max_price:
            return False

        # --- 条件 2: 当日基础流动性门槛 ---
        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]
        if pd.isna(today_dollar_vol) or today_dollar_vol < self.min_dollar_volume:
            return False

        # --- 条件 3: 实体阳线与涨幅门槛 ---
        today_open = today[FieldKey.OPEN.value]
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        if pd.isna(prev_close) or prev_close <= 0:
            return False

        change_pct = (today_close - prev_close) / prev_close
        if change_pct < self.min_change_pct or today_close <= today_open:
            return False

        # --- 条件 4: 防长上影线假突破（收盘位于全天高位区）---
        today_high = today[FieldKey.HIGH.value]
        today_low = today[FieldKey.LOW.value]
        range_span = today_high - today_low
        if range_span > 0:
            close_loc = (today_close - today_low) / range_span
            if close_loc < self.min_close_to_high_ratio:
                return False

        # --- 条件 5: 前期 20 交易日极窄死寂箱体 ---
        past_20 = history.iloc[-20:]
        box_high = past_20[FieldKey.HIGH.value].max()
        box_low = past_20[FieldKey.LOW.value].min()
        if pd.isna(box_high) or pd.isna(box_low) or box_low <= 0:
            return False

        box_amplitude = (box_high - box_low) / box_low
        if box_amplitude > self.max_base_amplitude:
            return False

        # --- 条件 6: 昨日短期均线极度粘合缠绕（MA5/10/20）---
        prev_ma5 = past_20[FieldKey.MA5.value].iloc[-1]
        prev_ma10 = past_20[FieldKey.MA10.value].iloc[-1]
        prev_ma20 = past_20[FieldKey.MA20.value].iloc[-1]
        if any(pd.isna([prev_ma5, prev_ma10, prev_ma20])):
            return False

        ma_max = max(prev_ma5, prev_ma10, prev_ma20)
        ma_min = min(prev_ma5, prev_ma10, prev_ma20)
        if ma_min <= 0:
            return False

        ma_spread = (ma_max - ma_min) / ma_min
        if ma_spread > self.max_ma_spread:
            return False

        # --- 条件 7: 今日点火突破箱体最高收盘价与均线 ---
        max_past_close = past_20[FieldKey.CLOSE.value].max()
        if today_close < max_past_close:
            return False

        today_ma5 = today[FieldKey.MA5.value]
        today_ma10 = today[FieldKey.MA10.value]
        today_ma20 = today[FieldKey.MA20.value]
        if (
            today_close <= today_ma5
            or today_close <= today_ma10
            or today_close <= today_ma20
        ):
            return False

        # --- 条件 8: 成交量惊雷脉冲 (>= 20 日均量的 3.5 倍) ---
        vol_20_mean = past_20[FieldKey.VOLUME.value].mean()
        if pd.isna(vol_20_mean) or vol_20_mean <= 0:
            return False

        vol_ratio = today[FieldKey.VOLUME.value] / vol_20_mean
        if vol_ratio < self.min_volume_ratio:
            return False

        return True

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        past_20 = history.iloc[-20:]
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        today_close = today[FieldKey.CLOSE.value]
        change_pct = (today_close - prev_close) / prev_close if prev_close > 0 else 0.0

        vol_20_mean = past_20[FieldKey.VOLUME.value].mean()
        vol_ratio = today[FieldKey.VOLUME.value] / vol_20_mean if vol_20_mean > 0 else 0.0

        box_high = past_20[FieldKey.HIGH.value].max()
        box_low = past_20[FieldKey.LOW.value].min()
        box_amplitude = (box_high - box_low) / box_low if box_low > 0 else 0.0

        prev_ma5 = past_20[FieldKey.MA5.value].iloc[-1]
        prev_ma10 = past_20[FieldKey.MA10.value].iloc[-1]
        prev_ma20 = past_20[FieldKey.MA20.value].iloc[-1]
        ma_min = min(prev_ma5, prev_ma10, prev_ma20)
        ma_max = max(prev_ma5, prev_ma10, prev_ma20)
        ma_spread = (ma_max - ma_min) / ma_min if ma_min > 0 else 0.0

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Change %": f"+{change_pct * 100:.2f}%",
            "Ratio (vs 20d)": f"{vol_ratio:.1f}x",
            "Base Amplitude": f"{box_amplitude * 100:.1f}%",
            "MA Spread": f"{ma_spread * 100:.1f}%",
            "Current Dollar Volume": f"${today[FieldKey.DOLLAR_VOLUME.value]:,.2f}",
        }
