import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class CDSignalStrategy(BaseStrategy):
    """
    策略3: MACD金叉抄底信号且过去10天平均交易额>1000万美元
    宽松版 CD 抄底策略：
    1. 水下金叉：DIF 上穿 DEA，且 DEA < 0（允许 DIF 已接近或略上 0 轴）
    2. MACD 柱：最近三根柱子向上（缩短），前两根必须为绿柱，当前柱可翻红
    3. 底背离：第二个低点价格不高于前低 5% 以上，且第二个低点 DIF 不低于前低 DIF
    """

    def __init__(self) -> None:
        # MACD 计算需要较长预热周期以确保 EMA26 与 DEA9 充分收敛（建议至少 100~120 天以上）
        # 120 天历史能有效消除 MACD 指标漂移误差，对齐主流行情平台数值
        self.days_needed: int = 120

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "MACD水下金叉底背离抄底信号且过去10天平均交易额>1000万美元 (按平均交易额排序)"

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        return self.days_needed

    def get_required_fields(self) -> list[FieldKey]:
        return [
            FieldKey.CLOSE,
            FieldKey.HIGH,
            FieldKey.LOW,
            FieldKey.DOLLAR_VOLUME,
            FieldKey.MACD_DIF,
            FieldKey.MACD_DEA,
            FieldKey.MACD_HISTOGRAM,
        ]

    # ========= 策略判断 =========

    def _evaluate_divergence(
        self, today: pd.Series, history: pd.DataFrame
    ) -> tuple[bool, Dict[str, Any]]:
        """
        基于波谷形态（Swing Low Pivot）判定价格底背离。
        """
        MAX_LOOKBACK = 35
        MIN_VALLEY_DISTANCE = 3
        PRICE_TOLERANCE = 0.05

        lookback = min(MAX_LOOKBACK - 1, len(history))
        window_past = history.iloc[-lookback:].copy()
        current_df = pd.DataFrame([today])
        window = pd.concat([window_past, current_df], axis=0, sort=False).reset_index(
            drop=True
        )
        n = len(window)

        if n < 10:
            return False, {}

        # 第二低点在最近 7 根 K 线内（含今天）
        recent_bars = min(7, n - MIN_VALLEY_DISTANCE)
        second_window = window.iloc[-recent_bars:]
        idx_2 = second_window[FieldKey.LOW.value].idxmin()
        low_2 = float(window.loc[idx_2, FieldKey.LOW.value])
        dif_2 = float(window.loc[idx_2, FieldKey.MACD_DIF.value])

        # 第一低点在 idx_2 之前至少间隔 MIN_VALLEY_DISTANCE 根 K 线
        prev_end = idx_2 - MIN_VALLEY_DISTANCE
        if prev_end < 0:
            return False, {}

        prev_window = window.iloc[: prev_end + 1]
        if prev_window.empty:
            return False, {}

        idx_1 = prev_window[FieldKey.LOW.value].idxmin()
        low_1 = float(window.loc[idx_1, FieldKey.LOW.value])
        dif_1 = float(window.loc[idx_1, FieldKey.MACD_DIF.value])

        # 确认两谷之间有价格反弹或动能回升（排除单边阴跌连续下跌）
        between_window = window.iloc[idx_1 + 1 : idx_2]
        if between_window.empty:
            return False, {}

        between_max_high = float(between_window[FieldKey.HIGH.value].max())
        between_max_dif = float(between_window[FieldKey.MACD_DIF.value].max())
        has_bounce = (between_max_high >= low_1 * 1.01) or (between_max_dif > dif_1)
        if not has_bounce:
            return False, {}

        # 价格容差：第二低点不高于前低 5% 以上（允许二次探底或微抬高的 Higher Low）
        is_price_ok = low_2 <= low_1 * (1 + PRICE_TOLERANCE)
        if not is_price_ok:
            return False, {}

        # 底背离：第二低点的 DIF 不低于第一低点 DIF
        is_dif_ok = dif_2 >= dif_1
        if not is_dif_ok:
            return False, {}

        days_ago_1 = n - 1 - idx_1
        days_ago_2 = n - 1 - idx_2
        info = {
            "low_1": low_1,
            "dif_1": dif_1,
            "days_ago_1": days_ago_1,
            "low_2": low_2,
            "dif_2": dif_2,
            "days_ago_2": days_ago_2,
        }
        return True, info

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) < self.days_needed - 1:
            return False

        # --- 条件 A: 检查过去10天平均成交额 ---
        past_10_dollar_volumes = history[FieldKey.DOLLAR_VOLUME.value].iloc[-10:]
        avg_dollar_vol_10 = past_10_dollar_volumes.mean()
        if pd.isna(avg_dollar_vol_10) or avg_dollar_vol_10 <= 10_000_000:
            return False

        # --- 条件 B: 检查 MACD 金叉抄底信号 ---
        # -------- 条件 1：水下金叉（宽松版）--------
        current_diff = today[FieldKey.MACD_DIF.value]
        current_dea = today[FieldKey.MACD_DEA.value]
        prev_diff = history[FieldKey.MACD_DIF.value].iloc[-1]
        prev_dea = history[FieldKey.MACD_DEA.value].iloc[-1]

        # 宽松金叉：前一日 DIF < DEA，今天 DIF >= DEA
        golden_cross = (prev_diff < prev_dea) and (current_diff >= current_dea)

        # 宽松水下：只要求 DEA < 0（信号线仍在 0 轴下）
        underwater = current_dea < 0

        if not (golden_cross and underwater):
            return False

        # -------- 条件 2：MACD 柱缩短（宽松版）--------
        histogram_current = today[FieldKey.MACD_HISTOGRAM.value]
        histogram_prev1 = history[FieldKey.MACD_HISTOGRAM.value].iloc[-1]  # T-1
        histogram_prev2 = history[FieldKey.MACD_HISTOGRAM.value].iloc[-2]  # T-2

        # 宽松要求：
        # - T-2、T-1 为绿柱（负数）
        # - 柱值单调递增：T-2 < T-1 < T
        #   -> 说明柱子在连续缩短，当前柱可以仍为负，也可以翻红
        shrink_histogram = (
            histogram_prev2 < 0
            and histogram_prev1 < 0
            and histogram_prev2 < histogram_prev1  # 例如 -0.8 < -0.5（在缩短）
            and histogram_prev1 < histogram_current  # 今天继续变大（接近0或翻红）
        )

        if not shrink_histogram:
            return False

        # -------- 条件 3：价格底背离（波谷形态识别版）--------
        is_divergent, _ = self._evaluate_divergence(today, history)
        return is_divergent

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        past_10_dollar_volumes = history[FieldKey.DOLLAR_VOLUME.value].iloc[-10:]
        avg_dollar_vol_10 = past_10_dollar_volumes.mean()
        current_dollar_volume = today[FieldKey.DOLLAR_VOLUME.value]
        current_close = today[FieldKey.CLOSE.value]
        current_dif = today[FieldKey.MACD_DIF.value]
        current_dea = today[FieldKey.MACD_DEA.value]

        _, div_info = self._evaluate_divergence(today, history)
        if div_info:
            div_desc = (
                f"L1:${div_info['low_1']:.2f}(DIF:{div_info['dif_1']:.2f}) -> "
                f"L2:${div_info['low_2']:.2f}(DIF:{div_info['dif_2']:.2f})"
            )
        else:
            div_desc = "形态确认"

        return {
            "Symbol": symbol,
            "Close": f"${current_close:.2f}",
            "MACD(DIF/DEA)": f"{current_dif:.2f} / {current_dea:.2f}",
            "Divergence Details": div_desc,
            "Avg Dollar Volume (10-day)": f"${avg_dollar_vol_10:,.2f}",
            "Current Dollar Volume": f"${current_dollar_volume:,.2f}",
        }

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        # 按近 10 日平均成交额排序
        return "Avg Dollar Volume (10-day)"

    def is_sort_ascending(self) -> bool:
        return False
