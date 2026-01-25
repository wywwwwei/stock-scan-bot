import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class MACrossStrategy(BaseStrategy):
    """
    MA5 上穿 MA10，且过去 10 天平均成交额 > 5000 万美元
    """

    def __init__(self) -> None:
        self.days_needed: int = 11  # 10 天历史 + 当日

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "MA5 上穿 MA10 且高成交额"

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        return self.days_needed

    def get_required_fields(self) -> list[FieldKey]:
        return [
            FieldKey.CLOSE,
            FieldKey.DOLLAR_VOLUME,
            FieldKey.MA5,
            FieldKey.MA10,
        ]

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) != self.days_needed - 1:
            print("[WARN] MACrossStrategy past_data 行数异常")
            return False

        prev = history.iloc[-1]

        ma_cross = (
            today[FieldKey.MA5.value] > today[FieldKey.MA10.value]
            and prev[FieldKey.MA5.value] <= prev[FieldKey.MA10.value]
        )

        avg_dollar_vol = history[FieldKey.DOLLAR_VOLUME.value].mean()
        high_liquidity = avg_dollar_vol > 50_000_000

        return ma_cross and high_liquidity

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        avg_dollar_vol = history[FieldKey.DOLLAR_VOLUME.value].mean()
        return {
            "Symbol": symbol,
            "Avg Dollar Volume (10-day)": f"${avg_dollar_vol:,.2f}",
        }

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        # 按近 10 日平均成交额排序
        return "Avg Dollar Volume (10-day)"

    def is_sort_ascending(self) -> bool:
        return False
