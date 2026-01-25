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
        # MACD 计算通常需要较多天数，例如 34 天 (MAX(12, 26) + 9 - 1) 以上的数据来稳定计算
        # 为了确保金叉、背离、柱缩短都能有效判断，我们使用较长的天数
        self.days_needed: int = 50

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "MACD金叉抄底信号且过去10天平均交易额>1000万美元 (按平均交易额排序)"

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        return self.days_needed

    def get_required_fields(self) -> list[FieldKey]:
        # 需要 Close 价格来计算 MACD
        # 需要 DollarVolume 用于交易额条件
        # 需要 Low 价格用于底背离判断
        return [
            FieldKey.CLOSE,
            FieldKey.LOW,
            FieldKey.DOLLAR_VOLUME,
            FieldKey.MACD_DIF,
            FieldKey.MACD_DEA,
            FieldKey.MACD_HISTOGRAM,
        ]

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) != self.days_needed - 1:
            print("[WARN] CDSignalStrategy history 行数异常")
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
            return False  # 如果柱子不符合缩短条件，返回False

        # -------- 条件 3：价格底背离（宽松版）--------
        # 更宽松的参数设置
        MAX_LOOKBACK = 30  # 背离检测的最大回看天数（不含今天）
        RECENT_BARS_FOR_SECOND_LOW = 7  # 第二低点在最近多少根 K 线中寻找
        PRICE_TOLERANCE = 0.05  # 价格容差：第二低点不高于前低 5% 以上

        lookback_past = min(MAX_LOOKBACK, len(history))

        # 最近 lookback_past 根历史 + 今天
        window_past = history.iloc[-lookback_past:].copy()
        current_df = pd.DataFrame([today])
        window_full = pd.concat([window_past, current_df], axis=0, sort=False)

        total_len = len(window_full)
        # 近期窗口至少 1 根，前段也至少 1 根
        recent_n = min(RECENT_BARS_FOR_SECOND_LOW, total_len - 1)
        split_idx = total_len - recent_n
        prev_window = window_full.iloc[:split_idx]  # 用于找第一个低点
        second_window = window_full.iloc[split_idx:]  # 用于找第二个低点（含今天）

        # 找两个价格低点
        prev_price_low = prev_window[FieldKey.LOW.value].min()
        prev_price_low_idx = prev_window[FieldKey.LOW.value].idxmin()

        second_price_low = second_window[FieldKey.LOW.value].min()
        second_price_low_idx = second_window[FieldKey.LOW.value].idxmin()

        # 确保第二个低点在第一个低点之后（按位置判断）
        prev_pos = window_full.index.get_loc(prev_price_low_idx)
        second_pos = window_full.index.get_loc(second_price_low_idx)
        if second_pos <= prev_pos:
            return False

        # 宽松价格条件：第二低点价格 <= 前低 * (1 + 5%)
        # 允许二次探底或略高一点的高低点（Higher Low）
        is_price_retest_or_higher_low = second_price_low <= prev_price_low * (
            1 + PRICE_TOLERANCE
        )
        if not is_price_retest_or_higher_low:
            return False

        # 对应两个低点的 DIF
        prev_dif_at_low = window_full.loc[prev_price_low_idx, FieldKey.MACD_DIF.value]
        second_dif_at_low = window_full.loc[
            second_price_low_idx, FieldKey.MACD_DIF.value
        ]

        # 宽松底背离：第二低点 DIF 不低于前低 DIF（>= 而不是 >）
        return second_dif_at_low >= prev_dif_at_low

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
        return {
            "Symbol": symbol,
            "Current Dollar Volume": f"${current_dollar_volume:,.2f}",
            "Avg Dollar Volume (10-day)": f"${avg_dollar_vol_10:,.2f}",
        }

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        # 按近 10 日平均成交额排序
        return "Avg Dollar Volume (10-day)"

    def is_sort_ascending(self) -> bool:
        return False
