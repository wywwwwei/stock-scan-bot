import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, Optional

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class RSIDivergenceReboundStrategy(BaseStrategy):
    """
    RSI 极限超跌与双底背离反弹策略 (RSI Momentum Divergence Rebound Strategy)

    核心交易逻辑（专门捕捉 3~5 天的高盈亏比暴力 V 反行情）：
    1. 前期深幅恐慌砸盘：在过去 25 根 K 线内，14 周期 RSI 曾深度杀跌跌破 28（进入极端恐慌区 V1）；
    2. 恐慌衰竭与二次探底（底背离确认）：
       - 经历反弹后股价二次下探，价格创下新低或平低（Low(V2) <= Low(V1) * 1.03）；
       - 但动量 RSI 显著抬高（RSI(V2) >= RSI(V1) + 2.5），形成教科书式的动量底背离，空头动能彻底衰竭；
    3. T 日企稳反攻确认：
       - 当日收阳线企稳（Close >= Open）；
       - 收盘价重新攻克并站上 5 日均线 (Close >= MA5)；
       - RSI 处于底部脱离区（RSI <= 55.0），尚未高位钝化，反弹空间巨大；
    4. 基础流动性：Close >= $2.00，20 日均成交额 >= $3,000,000。
    """

    def __init__(
        self,
        min_price: float = 2.0,
        min_avg_dollar_vol: float = 3_000_000.0,
        oversold_threshold: float = 28.0,
        min_rsi_spread: float = 2.5,
    ) -> None:
        self.days_needed: int = 50  # 足够稳定计算 14-RSI 与波谷寻找
        self.min_price = min_price
        self.min_avg_dollar_vol = min_avg_dollar_vol
        self.oversold_threshold = oversold_threshold
        self.min_rsi_spread = min_rsi_spread

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "RSI 极限超跌与双底背离反弹策略 (按背离斜率排序)"

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
            FieldKey.MA5,
            FieldKey.RSI14,
        ]

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        return "RSI Spread"

    def is_sort_ascending(self) -> bool:
        return False

    # ========= 辅助波谷背离算法 =========

    @staticmethod
    def _find_rsi_divergence(
        lows: np.ndarray,
        rsis: np.ndarray,
        oversold_limit: float,
        min_spread: float,
    ) -> Tuple[bool, Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        在近 25 根 K 线内寻找两个分离波谷：
        - V1: 处于更早区间 ([-25, -6])，且 RSI < oversold_limit
        - V2: 处于近期 ([-7, -1])
        - 中间有反弹确认 (最高 RSI 比 V1 高出至少 5 点)
        - 价格 Low(V2) <= Low(V1) * 1.03
        - RSI(V2) >= RSI(V1) + min_spread
        """
        n = len(rsis)
        if n < 15:
            return False, None, None, None, None

        # 寻找 V1
        best_v1_idx = None
        min_v1_rsi = 999.0
        for i in range(max(0, n - 25), n - 6):
            if rsis[i] < oversold_limit and rsis[i] < min_v1_rsi:
                # 检查局部波谷特征
                left = rsis[i - 1] if i > 0 else 999.0
                right = rsis[i + 1] if i < n - 1 else 999.0
                if rsis[i] <= left and rsis[i] <= right:
                    min_v1_rsi = rsis[i]
                    best_v1_idx = i

        if best_v1_idx is None:
            return False, None, None, None, None

        v1_price = lows[best_v1_idx]
        v1_rsi = min_v1_rsi

        # 寻找 V2
        best_v2_idx = None
        min_v2_price = 999999.0
        for j in range(best_v1_idx + 3, n):
            # 要求价格接近或创出新低，且 RSI 明显抬高
            if lows[j] <= v1_price * 1.03 and rsis[j] >= v1_rsi + min_spread:
                if lows[j] < min_v2_price:
                    min_v2_price = lows[j]
                    best_v2_idx = j

        if best_v2_idx is None:
            return False, None, None, None, None

        # 中间反弹确认：两波谷之间的最高 RSI 必须回弹至少 5 点，杜绝单边下行假背离
        inter_rsi_max = np.max(rsis[best_v1_idx + 1 : best_v2_idx])
        if inter_rsi_max < v1_rsi + 4.0:
            return False, None, None, None, None

        v2_price = lows[best_v2_idx]
        v2_rsi = rsis[best_v2_idx]

        return True, v1_price, v1_rsi, v2_price, v2_rsi

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) < self.days_needed - 1:
            return False

        # --- 1. 价格与流动性过滤 ---
        today_close = today[FieldKey.CLOSE.value]
        if pd.isna(today_close) or today_close < self.min_price:
            return False

        past_20_vol = history[FieldKey.DOLLAR_VOLUME.value].iloc[-20:]
        avg_dollar_vol_20 = past_20_vol.mean()
        if pd.isna(avg_dollar_vol_20) or avg_dollar_vol_20 < self.min_avg_dollar_vol:
            return False

        # --- 2. 组装近 25 天行情数组并判定 RSI 底背离 ---
        full_df = pd.concat([history, pd.DataFrame([today])], ignore_index=True)
        recent_25 = full_df.iloc[-25:]

        lows = recent_25[FieldKey.LOW.value].to_numpy(dtype=float)
        rsis = recent_25[FieldKey.RSI14.value].to_numpy(dtype=float)

        has_div, v1_p, v1_r, v2_p, v2_r = self._find_rsi_divergence(
            lows, rsis, self.oversold_threshold, self.min_rsi_spread
        )
        if not has_div:
            return False

        # --- 3. 当日企稳与均线突破确认 ---
        today_open = today[FieldKey.OPEN.value]
        if pd.isna(today_open) or today_close < today_open:
            return False  # 当日必须为阳线或平盘

        today_ma5 = today[FieldKey.MA5.value]
        if pd.isna(today_ma5) or today_close < today_ma5:
            return False  # 收盘重新夺回 5 日线

        today_rsi = today[FieldKey.RSI14.value]
        if pd.isna(today_rsi) or today_rsi > 55.0:
            return False  # 处于底部刚脱离区，尚未高位钝化

        return True

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        today_close = today[FieldKey.CLOSE.value]
        today_rsi = today[FieldKey.RSI14.value]
        today_ma5 = today[FieldKey.MA5.value]

        full_df = pd.concat([history, pd.DataFrame([today])], ignore_index=True)
        recent_25 = full_df.iloc[-25:]
        lows = recent_25[FieldKey.LOW.value].to_numpy(dtype=float)
        rsis = recent_25[FieldKey.RSI14.value].to_numpy(dtype=float)

        _, v1_p, v1_r, v2_p, v2_r = self._find_rsi_divergence(
            lows, rsis, self.oversold_threshold, self.min_rsi_spread
        )

        spread = (v2_r - v1_r) if (v1_r is not None and v2_r is not None) else 0.0

        past_20_vol = history[FieldKey.DOLLAR_VOLUME.value].iloc[-20:]
        avg_dollar_vol_20 = past_20_vol.mean()

        div_desc = f"V1:${v1_p:.2f}(RSI:{v1_r:.1f}) -> V2:${v2_p:.2f}(RSI:{v2_r:.1f})"

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "RSI14": f"{today_rsi:.1f}",
            "MA5": f"${today_ma5:.2f}",
            "RSI Spread": float(round(spread, 1)),
            "Divergence Pattern": div_desc,
            "Avg Dollar Vol (20d)": f"${avg_dollar_vol_20:,.2f}",
        }
