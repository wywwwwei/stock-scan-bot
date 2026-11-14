import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class VolumeSurgeStrategy(BaseStrategy):
    """
    成交额放量上涨策略

    条件：
    - 当日成交额 > 过去 60 天平均成交额的 2 倍
    - 当日涨幅为正（Close > 前一日 Close），避免选出放量暴跌的雷股
    """

    def __init__(self) -> None:
        self.days_needed: int = 61  # 60 天历史 + 当日

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "成交额放量上涨股（MA50多头且放量>60日均值2倍）"

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        return self.days_needed

    def get_required_fields(self) -> list[FieldKey]:
        return [
            FieldKey.CLOSE,
            FieldKey.DOLLAR_VOLUME,
            FieldKey.MA50,
        ]

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) < self.days_needed - 1:
            return False

        # --- 条件 A: 中期多头趋势过滤（Close > MA50，排除单边阴跌诱多）---
        today_close = today[FieldKey.CLOSE.value]
        today_ma50 = today[FieldKey.MA50.value]
        if pd.isna(today_close) or pd.isna(today_ma50) or today_close <= today_ma50:
            return False

        # --- 条件 B: 适度涨幅区间（+1.0% <= 涨幅 <= +8.0%，防单日透支追高）---
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        if pd.isna(prev_close) or prev_close <= 0:
            return False
        change_pct = (today_close - prev_close) / prev_close
        if change_pct < 0.01 or change_pct > 0.08:
            return False

        # --- 条件 C: 放量判断（当日成交额 > 60 日均额 2 倍）---
        avg_dollar_vol = history[FieldKey.DOLLAR_VOLUME.value].mean()
        if pd.isna(avg_dollar_vol) or avg_dollar_vol <= 0:
            return False

        ratio = today[FieldKey.DOLLAR_VOLUME.value] / avg_dollar_vol
        return bool(ratio > 2)

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        avg_dollar_vol = history[FieldKey.DOLLAR_VOLUME.value].mean()
        current_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        today_close = today[FieldKey.CLOSE.value]
        change_pct = (today_close - prev_close) / prev_close * 100

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Change %": f"+{change_pct:.2f}%",
            "Ratio": float(round(current_dollar_vol / avg_dollar_vol, 2)),
            "Current Dollar Volume": f"${current_dollar_vol:,.2f}",
            "60-Day Avg Dollar Volume": f"${avg_dollar_vol:,.2f}",
        }

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        return "Ratio"

    def is_sort_ascending(self) -> bool:
        return False
