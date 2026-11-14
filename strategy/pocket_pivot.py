import pandas as pd
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class PocketPivotStrategy(BaseStrategy):
    """
    机构口袋支点策略 (Pocket Pivot Strategy - 欧奈尔/莫拉莱斯模型)

    核心交易逻辑（比 52 周新高更早、更便宜入场的右侧机构建仓点）：
    1. 中长线多头趋势：Close > MA50，上市满 200 天要求处于牛市多头（MA50 > MA200）；
    2. 口袋支点量能特征（核心指纹）：
       - 当日成交量严格大于过去 10 个交易日内【所有阴线】的最大成交量（证明机构大单扫筹，力量压过近期所有空头抛盘）；
    3. 均线启动位置（绝不追高）：
       - 当日为实体阳线（Close > Open 且 Close > Prev_Close）；
       - 收盘价紧贴 10 日均线 (MA10) 或 20 日均线 (MA20) 突破启动（0.0% <= (Close - MA) / MA <= 3.5%）；
    4. 基础流动性：股价 >= $3.00，20 日均成交额 >= $5,000,000。
    """

    def __init__(
        self,
        lookback_down_days: int = 10,
        max_dist_from_ma: float = 0.035,
        min_price: float = 3.0,
        min_avg_dollar_vol: float = 5_000_000.0,
    ) -> None:
        self.days_needed: int = 210  # 支持计算 MA200 与 MA50
        self.lookback_down_days = lookback_down_days
        self.max_dist_from_ma = max_dist_from_ma
        self.min_price = min_price
        self.min_avg_dollar_vol = min_avg_dollar_vol

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "机构口袋支点策略 (欧奈尔/莫拉莱斯模型，按放量倍数排序)"

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
            FieldKey.MA10,
            FieldKey.MA20,
            FieldKey.MA50,
            FieldKey.MA200,
        ]

    # ========= 排序语义 =========

    def get_sort_column(self) -> str:
        return "Pocket Ratio"

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

        # --- 1. 价格与流动性过滤 ---
        today_close = today[FieldKey.CLOSE.value]
        if pd.isna(today_close) or today_close < self.min_price:
            return False

        past_20_vol = history[FieldKey.DOLLAR_VOLUME.value].iloc[-20:]
        avg_dollar_vol_20 = past_20_vol.mean()
        if pd.isna(avg_dollar_vol_20) or avg_dollar_vol_20 < self.min_avg_dollar_vol:
            return False

        # --- 2. 大趋势多头排列过滤 (Close > MA50 & MA50 > MA200) ---
        today_ma10 = today[FieldKey.MA10.value]
        today_ma20 = today[FieldKey.MA20.value]
        today_ma50 = today[FieldKey.MA50.value]
        today_ma200 = today[FieldKey.MA200.value]

        if pd.isna(today_ma50) or today_close <= today_ma50:
            return False

        if not pd.isna(today_ma200) and today_ma50 < today_ma200:
            return False

        # --- 3. 实体阳线启动 ---
        today_open = today[FieldKey.OPEN.value]
        prev_close = history[FieldKey.CLOSE.value].iloc[-1]
        if pd.isna(today_open) or today_close <= today_open or today_close <= prev_close:
            return False

        # --- 4. 紧贴关键均线启动（MA10 或 MA20 距离在 0% ~ 3.5% 之间）---
        near_ma10 = False
        if not pd.isna(today_ma10) and today_ma10 > 0:
            dist_10 = (today_close - today_ma10) / today_ma10
            if 0.0 <= dist_10 <= self.max_dist_from_ma:
                near_ma10 = True

        near_ma20 = False
        if not pd.isna(today_ma20) and today_ma20 > 0:
            dist_20 = (today_close - today_ma20) / today_ma20
            if 0.0 <= dist_20 <= self.max_dist_from_ma:
                near_ma20 = True

        if not (near_ma10 or near_ma20):
            return False

        # --- 5. 口袋支点量能：当日成交量 > 过去 10 天所有阴线的最大成交量 ---
        recent_10_hist = history.iloc[-self.lookback_down_days :]
        down_volumes = []

        closes = recent_10_hist[FieldKey.CLOSE.value].values
        opens = recent_10_hist[FieldKey.OPEN.value].values
        volumes = recent_10_hist[FieldKey.VOLUME.value].values
        # 前一日收盘用于判断跳空阴线
        prior_closes = history[FieldKey.CLOSE.value].iloc[-self.lookback_down_days - 1 : -1].values

        for i in range(len(recent_10_hist)):
            is_down = (closes[i] < opens[i]) or (closes[i] < prior_closes[i])
            if is_down and not pd.isna(volumes[i]):
                down_volumes.append(volumes[i])

        today_volume = today[FieldKey.VOLUME.value]
        if pd.isna(today_volume) or today_volume <= 0:
            return False

        if down_volumes:
            max_down_vol = max(down_volumes)
            if today_volume <= max_down_vol:
                return False
        else:
            # 过去10天全为阳线，则要求量能大于 20 日均量
            avg_vol_20 = history[FieldKey.VOLUME.value].iloc[-20:].mean()
            if today_volume <= avg_vol_20 * 1.5:
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

        today_volume = today[FieldKey.VOLUME.value]
        recent_10_hist = history.iloc[-self.lookback_down_days :]
        closes = recent_10_hist[FieldKey.CLOSE.value].values
        opens = recent_10_hist[FieldKey.OPEN.value].values
        volumes = recent_10_hist[FieldKey.VOLUME.value].values
        prior_closes = history[FieldKey.CLOSE.value].iloc[-self.lookback_down_days - 1 : -1].values

        down_vols = [
            volumes[i]
            for i in range(len(recent_10_hist))
            if (closes[i] < opens[i] or closes[i] < prior_closes[i]) and not pd.isna(volumes[i])
        ]
        max_down_vol = max(down_vols) if down_vols else 1.0
        pocket_ratio = today_volume / max_down_vol if max_down_vol > 0 else 1.0

        today_ma10 = today[FieldKey.MA10.value]
        today_ma20 = today[FieldKey.MA20.value]
        pivot_type = "MA10 Pivot" if (0.0 <= (today_close - today_ma10) / today_ma10 <= self.max_dist_from_ma) else "MA20 Pivot"

        past_20_vol = history[FieldKey.DOLLAR_VOLUME.value].iloc[-20:]
        avg_dollar_vol_20 = past_20_vol.mean()

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Change %": f"+{change_pct:.2f}%",
            "Pivot Type": pivot_type,
            "Pocket Ratio": float(round(pocket_ratio, 2)),
            "Avg Dollar Vol (20d)": f"${avg_dollar_vol_20:,.2f}",
        }
