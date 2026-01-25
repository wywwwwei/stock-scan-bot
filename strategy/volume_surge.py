import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class VolumeSurgeStrategy(BaseStrategy):
    """
    成交额异动策略

    条件：
    - 当日成交额 > 过去 60 天平均成交额的 2 倍
    """

    def __init__(self) -> None:
        self.days_needed: int = 61  # 60 天历史 + 当日

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "成交额异动股（当日成交额 > 60 日均值 2 倍）"

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        return self.days_needed

    def get_required_fields(self) -> list[FieldKey]:
        return [
            FieldKey.DOLLAR_VOLUME,
        ]

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) != self.days_needed - 1:
            print(f"[WARN] VolumeSurgeStrategy history 行数异常: {len(history)}")
            return False

        avg_dollar_vol = history[FieldKey.DOLLAR_VOLUME.value].mean()
        if pd.isna(avg_dollar_vol) or avg_dollar_vol <= 0:
            return False

        ratio = today[FieldKey.DOLLAR_VOLUME.value] / avg_dollar_vol
        return ratio > 2

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        avg_dollar_vol = history[FieldKey.DOLLAR_VOLUME.value].mean()
        current_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]

        return {
            "Symbol": symbol,
            "Current Dollar Volume": f"${current_dollar_vol:,.2f}",
            "60-Day Avg Dollar Volume": f"${avg_dollar_vol:,.2f}",
            "Ratio": round(current_dollar_vol / avg_dollar_vol, 2),
        }

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        return "Ratio"

    def is_sort_ascending(self) -> bool:
        return False
