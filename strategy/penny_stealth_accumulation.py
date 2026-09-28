import pandas as pd
import numpy as np
from typing import Dict, Any

from strategy.base import BaseStrategy
from scanner.fields import FieldKey


class PennyStealthAccumulationStrategy(BaseStrategy):
    """
    仙股潜伏：量价背离与暗度陈仓策略 (Penny Stealth Accumulation Strategy)

    核心交易逻辑（左侧提前埋伏）：
    1. 仙股低价与超跌：$0.20 <= Close <= $4.00，且处于近 70 日相对低位区（Close <= Max_High * 0.55）；
    2. 极窄死寂箱体：过去 20 交易日最高与最低价振幅 <= 25%，处于深水区无风浪的蓄水状态；
    3. 均线高度粘合：MA5、MA10、MA20 三线极差 <= 4.0%，筹码平均持仓成本高度一致；
    4. OBV 量价正背离：过去 20 天股价走平或微幅波动，但能量潮（OBV）后半段显著高于前半段，资金持续暗中买入；
    5. 阳线成交量显著占优：过去 15 个交易日内，上涨日总成交量 / 下跌日总成交量 >= 1.5 倍（阳放量阴缩量，庄家吃多抛少）；
    6. 收盘重心位于上半区：过去 10 交易日平均收盘位置指标 CLV >= 0.0，显示盘口全天存在持续限价托单；
    7. 基础流动性防死股：过去 20 日日均成交额 >= $300,000，当日成交额 >= $200,000。
    """

    def __init__(
        self,
        min_price: float = 0.20,
        max_price: float = 4.00,
        max_from_high_ratio: float = 0.55,
        max_base_amplitude: float = 0.25,
        max_ma_spread: float = 0.04,
        min_vol_dominance: float = 1.5,
        min_avg_dollar_vol: float = 300_000.0,
        min_today_dollar_vol: float = 200_000.0,
    ) -> None:
        self.days_needed: int = 70  # 足够长周期确认超跌底部、OBV 趋势与均线粘合
        self.min_price = min_price
        self.max_price = max_price
        self.max_from_high_ratio = max_from_high_ratio
        self.max_base_amplitude = max_base_amplitude
        self.max_ma_spread = max_ma_spread
        self.min_vol_dominance = min_vol_dominance
        self.min_avg_dollar_vol = min_avg_dollar_vol
        self.min_today_dollar_vol = min_today_dollar_vol

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "仙股潜伏：量价背离与暗度陈仓策略 (按阳阴量比率排序)"

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
        return "Vol Dominance (15d)"

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

        # --- 条件 1: 仙股低价与基础流动性 ---
        today_close = today[FieldKey.CLOSE.value]
        if pd.isna(today_close) or today_close < self.min_price or today_close > self.max_price:
            return False

        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]
        if pd.isna(today_dollar_vol) or today_dollar_vol < self.min_today_dollar_vol:
            return False

        # 拼接全量序列（含今日）
        full_close = pd.concat(
            [history[FieldKey.CLOSE.value], pd.Series([today_close])],
            ignore_index=True,
        )
        full_volume = pd.concat(
            [history[FieldKey.VOLUME.value], pd.Series([today[FieldKey.VOLUME.value]])],
            ignore_index=True,
        )
        full_high = pd.concat(
            [history[FieldKey.HIGH.value], pd.Series([today[FieldKey.HIGH.value]])],
            ignore_index=True,
        )
        full_low = pd.concat(
            [history[FieldKey.LOW.value], pd.Series([today[FieldKey.LOW.value]])],
            ignore_index=True,
        )
        full_dollar_vol = pd.concat(
            [history[FieldKey.DOLLAR_VOLUME.value], pd.Series([today_dollar_vol])],
            ignore_index=True,
        )

        # 20 日日均成交额
        avg_dollar_vol_20 = full_dollar_vol.iloc[-20:].mean()
        if pd.isna(avg_dollar_vol_20) or avg_dollar_vol_20 < self.min_avg_dollar_vol:
            return False

        # --- 条件 2: 阶段超跌底部确认（非高位滞涨）---
        max_high = full_high.max()
        if pd.isna(max_high) or max_high <= 0:
            return False
        if today_close > max_high * self.max_from_high_ratio:
            return False

        # --- 条件 3: 过去 20 交易日极窄死寂箱体 ---
        past_20_high = full_high.iloc[-20:].max()
        past_20_low = full_low.iloc[-20:].min()
        if pd.isna(past_20_high) or pd.isna(past_20_low) or past_20_low <= 0:
            return False

        box_amplitude = (past_20_high - past_20_low) / past_20_low
        if box_amplitude > self.max_base_amplitude:
            return False

        # 价格没有提前大幅暴涨透支（不超过 20 日初价的 8%）
        if today_close > full_close.iloc[-20] * 1.08:
            return False

        # --- 条件 4: MA5 / MA10 / MA20 三线极度粘合收敛 ---
        ma5 = today[FieldKey.MA5.value]
        ma10 = today[FieldKey.MA10.value]
        ma20 = today[FieldKey.MA20.value]
        if any(pd.isna([ma5, ma10, ma20])):
            return False

        ma_max = max(ma5, ma10, ma20)
        ma_min = min(ma5, ma10, ma20)
        if ma_min <= 0:
            return False

        ma_spread = (ma_max - ma_min) / ma_min
        if ma_spread > self.max_ma_spread:
            return False

        # --- 条件 5: OBV 逆势爬升量价正背离（隐蔽资金流入）---
        price_diff = full_close.diff()
        direction = np.sign(price_diff).fillna(0)
        obv = (direction * full_volume).cumsum()
        past_20_obv = obv.iloc[-20:]

        # 后半段 OBV 均值高于前半段，且最新 OBV 高于 20 日均值
        if past_20_obv.iloc[-10:].mean() <= past_20_obv.iloc[:10].mean():
            return False
        if past_20_obv.iloc[-1] <= past_20_obv.mean():
            return False

        # --- 条件 6: 阳量显著占优（近 15 天上涨总成交量 / 下跌总成交量 >= 1.5）---
        recent_15_diff = price_diff.iloc[-15:]
        recent_15_vol = full_volume.iloc[-15:]
        up_vol = recent_15_vol[recent_15_diff > 0].sum()
        down_vol = recent_15_vol[recent_15_diff < 0].sum()

        if down_vol > 0:
            vol_dominance = up_vol / down_vol
        else:
            vol_dominance = 3.0  # 全是阳线无抛盘

        if vol_dominance < self.min_vol_dominance:
            return False

        # --- 条件 7: 收盘重心微观偏高（近 10 天平均 CLV >= 0.0）---
        h10 = full_high.iloc[-10:]
        l10 = full_low.iloc[-10:]
        c10 = full_close.iloc[-10:]
        span10 = h10 - l10
        clv = np.where(span10 > 0, ((c10 - l10) - (h10 - c10)) / span10, 0)
        if clv.mean() < 0.0:
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
        full_close = pd.concat(
            [history[FieldKey.CLOSE.value], pd.Series([today_close])],
            ignore_index=True,
        )
        full_volume = pd.concat(
            [history[FieldKey.VOLUME.value], pd.Series([today[FieldKey.VOLUME.value]])],
            ignore_index=True,
        )
        full_high = pd.concat(
            [history[FieldKey.HIGH.value], pd.Series([today[FieldKey.HIGH.value]])],
            ignore_index=True,
        )
        full_low = pd.concat(
            [history[FieldKey.LOW.value], pd.Series([today[FieldKey.LOW.value]])],
            ignore_index=True,
        )
        full_dollar_vol = pd.concat(
            [history[FieldKey.DOLLAR_VOLUME.value], pd.Series([today[FieldKey.DOLLAR_VOLUME.value]])],
            ignore_index=True,
        )

        past_20_high = full_high.iloc[-20:].max()
        past_20_low = full_low.iloc[-20:].min()
        box_amplitude = (past_20_high - past_20_low) / past_20_low if past_20_low > 0 else 0.0

        ma5 = today[FieldKey.MA5.value]
        ma10 = today[FieldKey.MA10.value]
        ma20 = today[FieldKey.MA20.value]
        ma_max = max(ma5, ma10, ma20)
        ma_min = min(ma5, ma10, ma20)
        ma_spread = (ma_max - ma_min) / ma_min if ma_min > 0 else 0.0

        price_diff = full_close.diff()
        recent_15_diff = price_diff.iloc[-15:]
        recent_15_vol = full_volume.iloc[-15:]
        up_vol = recent_15_vol[recent_15_diff > 0].sum()
        down_vol = recent_15_vol[recent_15_diff < 0].sum()
        vol_dominance = (up_vol / down_vol) if down_vol > 0 else 3.0

        avg_dollar_vol_20 = full_dollar_vol.iloc[-20:].mean()

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Base Amplitude": f"{box_amplitude * 100:.1f}%",
            "Vol Dominance (15d)": f"{vol_dominance:.1f}x",
            "OBV Trend": "Accumulating 📈",
            "MA Spread": f"{ma_spread * 100:.1f}%",
            "Avg Dollar Vol (20d)": f"${avg_dollar_vol_20:,.2f}",
        }
