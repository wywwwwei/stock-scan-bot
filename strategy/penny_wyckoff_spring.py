import pandas as pd
import numpy as np
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class PennyWyckoffSpringStrategy(BaseStrategy):
    """
    威科夫弹簧坑扫损反包埋伏策略 (Penny Wyckoff Spring Strategy)

    核心交易逻辑（左侧洗盘终点精准埋伏）：
    1. 仙股价格定位：$0.20 <= Close <= $5.00；
    2. 前期确立筑底支撑：过去 35 个交易日（不含近 5 天）在某一价格水平线上反复获得支撑，形成基准支撑位 S；
    3. 弹簧事件（Spring 挖坑）：近 5 个交易日内曾有 1 天故意砸穿支撑位（Low < S * 0.99）制造破位诱空扫损，但随后迅速收复支撑；
    4. 无供给二次回踩测试（No-Supply Retest）：
       - 今日价格稳稳守在 Spring 挖坑最低价之上（Low >= Spring_Low）；
       - 今日收盘价贴合在支撑位附近（Support * 0.97 <= Close <= Support * 1.12）；
       - 成交量极度萎缩：当日成交量 <= 过去 20 日均量的 70%，证明浮游抛压彻底枯竭；
    5. 极小止损与高盈亏比：止损仅需挂在 Spring 挖坑低点下方 2%~3%，潜在回报 50%~300%。
    """

    def __init__(
        self,
        min_price: float = 0.20,
        max_price: float = 5.00,
        min_avg_dollar_vol: float = 300_000.0,
        min_today_dollar_vol: float = 150_000.0,
        max_retest_volume_ratio: float = 0.70,
    ) -> None:
        self.days_needed: int = 50  # 35 天支撑构筑 + 5 天弹簧与测试 + 20 天均线
        self.min_price = min_price
        self.max_price = max_price
        self.min_avg_dollar_vol = min_avg_dollar_vol
        self.min_today_dollar_vol = min_today_dollar_vol
        self.max_retest_volume_ratio = max_retest_volume_ratio

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "威科夫弹簧坑扫损反包埋伏策略 (按回踩缩量程度排序)"

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
            FieldKey.MA20,
        ]

    # ========= 排序规则 =========

    def get_sort_column(self) -> str:
        return "Vol vs 20d %"

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

        # --- 条件 1: 仙股价格过滤 ---
        today_close = today[FieldKey.CLOSE.value]
        if pd.isna(today_close) or today_close < self.min_price or today_close > self.max_price:
            return False

        # --- 条件 2: 基础流动性门槛 ---
        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]
        if pd.isna(today_dollar_vol) or today_dollar_vol < self.min_today_dollar_vol:
            return False

        full_dollar_vol = pd.concat(
            [history[FieldKey.DOLLAR_VOLUME.value], pd.Series([today_dollar_vol])],
            ignore_index=True,
        )
        avg_dollar_vol_20 = full_dollar_vol.iloc[-20:].mean()
        if pd.isna(avg_dollar_vol_20) or avg_dollar_vol_20 < self.min_avg_dollar_vol:
            return False

        # --- 条件 3: 前期筑底支撑位识别 (history.iloc[-35:-5]) ---
        base_lows = history[FieldKey.LOW.value].iloc[-35:-5]
        support_level = base_lows.min()
        if pd.isna(support_level) or support_level <= 0:
            return False

        base_high = history[FieldKey.HIGH.value].iloc[-35:-5].max()
        if pd.isna(base_high) or (base_high - support_level) / support_level > 0.45:
            return False

        # --- 条件 4: 近 5 个交易日内发生 Spring 挖坑事件 ---
        recent_lows = history[FieldKey.LOW.value].iloc[-5:]
        recent_closes = history[FieldKey.CLOSE.value].iloc[-5:]

        spring_mask = recent_lows < support_level * 0.99
        if not spring_mask.any():
            return False

        spring_low = recent_lows.min()

        # 挖坑后必须曾成功反抽收回支撑位之上
        spring_first_idx = np.where(spring_mask.to_numpy())[0][0]
        post_spring_closes = recent_closes.iloc[spring_first_idx:]
        if post_spring_closes.max() < support_level * 0.98:
            return False

        # --- 条件 5: 今日无供给二次测试 (No-Supply Retest) ---
        today_low = today[FieldKey.LOW.value]
        if pd.isna(today_low) or today_low < spring_low * 0.99:
            return False  # 未跌破 Spring 挖坑低点

        # 今日收盘紧贴支撑位 (-3% ~ +12%)
        if today_close < support_level * 0.97 or today_close > support_level * 1.12:
            return False

        # 成交量极度萎缩（<= 20 日均量的 70%）
        full_volume = pd.concat(
            [history[FieldKey.VOLUME.value], pd.Series([today[FieldKey.VOLUME.value]])],
            ignore_index=True,
        )
        vol_20_mean = full_volume.iloc[-20:].mean()
        if pd.isna(vol_20_mean) or vol_20_mean <= 0:
            return False

        vol_ratio = today[FieldKey.VOLUME.value] / vol_20_mean
        if vol_ratio > self.max_retest_volume_ratio:
            return False

        # 今日非暴跌实体大阴线（实体跌幅不超过 3%）
        today_open = today[FieldKey.OPEN.value]
        if (today_open - today_close) / today_open > 0.03:
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
        base_lows = history[FieldKey.LOW.value].iloc[-35:-5]
        support_level = base_lows.min()

        recent_lows = history[FieldKey.LOW.value].iloc[-5:]
        spring_low = recent_lows.min()

        full_volume = pd.concat(
            [history[FieldKey.VOLUME.value], pd.Series([today[FieldKey.VOLUME.value]])],
            ignore_index=True,
        )
        vol_20_mean = full_volume.iloc[-20:].mean()
        vol_ratio = today[FieldKey.VOLUME.value] / vol_20_mean if vol_20_mean > 0 else 0.0

        full_dollar_vol = pd.concat(
            [history[FieldKey.DOLLAR_VOLUME.value], pd.Series([today[FieldKey.DOLLAR_VOLUME.value]])],
            ignore_index=True,
        )
        avg_dollar_vol_20 = full_dollar_vol.iloc[-20:].mean()

        dist_to_support = (today_close - support_level) / support_level if support_level > 0 else 0.0

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Support Level": f"${support_level:.2f}",
            "Spring Low": f"${spring_low:.2f}",
            "Vol vs 20d %": f"{vol_ratio * 100:.1f}%",
            "Dist to Support": f"{'+' if dist_to_support >= 0 else ''}{dist_to_support * 100:.1f}%",
            "Avg Dollar Vol (20d)": f"${avg_dollar_vol_20:,.2f}",
        }
