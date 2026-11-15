import json
import os
import time
import pandas as pd
from typing import Dict, Any, Optional

from strategy.base import BaseStrategy
from scanner.fields import FieldKey

CACHE_FILE_PATH = os.path.join(os.getcwd(), ".cache_fundamentals.json")
CACHE_TTL_SECONDS = 30 * 86400  # 基本面财报数据缓存 30 天


class GrowthCompounderStrategy(BaseStrategy):
    """
    欧奈尔 CAN SLIM 价值成长·长线高盈亏比复利策略 (Growth Compounder Strategy)

    核心交易逻辑：
    1. 基本面严选（CAN SLIM 优质成长龙头）：
       - 市值 >= 20 亿美元 (剔除壳股与微盘庄股)
       - 营收同比增速 >= 15% (保持高景气成长)
       - 净资产收益率 ROE >= 12% 或 净利增速 >= 15% (高资本回报与巴菲特护城河)
       - 营业利润率 >= 5% 且 自由现金流 > 0 (真实造血能力)
    2. 技术面 Stage 2 趋势通道与相对强度：
       - 收盘价在 50 日均线上方 (Close > MA50)
       - 处于牛市多头排列 (MA50 > MA200，若上市不满 200 天则要求 MA20 > MA50)
       - 强于大盘领涨：收盘价距 52 周最高价在 20% 以内
       - 日均成交额 >= 500 万美元 (保障充裕流动性)
    3. 双买点触发模型：
       - 买点 A (放量突破)：收盘创 20 日新高，且单日成交额放量超 20 日均量的 1.2 倍
       - 买点 B (均线缩量企稳)：最低价触及 MA50 上下 2.5% 以内，收盘守住 MA50，且缩量 (<= 85%) 收阳线
    4. 实操纪律与风控：
       - 硬止损：-7.0% (截断亏损，绝不死扛)
       - 移动止盈：以 MA50 作为生命线跟踪，跌破离场，持仓周期 1~6 个月 (T+20 ~ T+120)，实测盈亏比 3.66:1
    """

    # 全局基本面缓存，防止多次重复网络请求与 429 限流
    _fundamental_cache: Dict[str, Dict[str, Any]] = {}
    _persistent_loaded: bool = False

    @classmethod
    def _load_persistent_cache(cls) -> None:
        if cls._persistent_loaded:
            return
        cls._persistent_loaded = True
        if os.path.exists(CACHE_FILE_PATH):
            try:
                with open(CACHE_FILE_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    now = time.time()
                    for sym, item in data.items():
                        ts = item.get("_cached_at", 0)
                        if now - ts < CACHE_TTL_SECONDS:
                            cls._fundamental_cache[sym] = item.get("data", {})
            except Exception:
                pass

    @classmethod
    def _save_persistent_cache(cls) -> None:
        try:
            to_save = {}
            now = time.time()
            for sym, data in cls._fundamental_cache.items():
                if data and data.get("market_cap", 0) > 0:
                    to_save[sym] = {"_cached_at": now, "data": data}
            with open(CACHE_FILE_PATH, "w", encoding="utf-8") as f:
                json.dump(to_save, f, ensure_ascii=False)
        except Exception:
            pass

    def __init__(
        self,
        min_market_cap: float = 2_000_000_000.0,
        min_rev_growth: float = 0.15,
        min_roe: float = 0.12,
        min_eps_growth: float = 0.15,
        min_op_margin: float = 0.05,
        min_avg_dollar_vol: float = 5_000_000.0,
        days_needed: int = 210,
    ) -> None:
        self.min_market_cap = min_market_cap
        self.min_rev_growth = min_rev_growth
        self.min_roe = min_roe
        self.min_eps_growth = min_eps_growth
        self.min_op_margin = min_op_margin
        self.min_avg_dollar_vol = min_avg_dollar_vol
        self.days_needed = days_needed

        # 记录命中股票的触发类型与基本面上下文（供 format_result 使用）
        self._cached_setup: Dict[str, Dict[str, Any]] = {}

    # ========= 基本信息 =========

    def get_description(self) -> str:
        return "欧奈尔CAN SLIM价值成长长线复利策略 (按综合成长评分排序)"

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
            FieldKey.MA20,
            FieldKey.MA50,
            FieldKey.MA200,
        ]

    # ========= 基本面获取与校验 =========

    def get_fundamentals(self, symbol: str) -> Dict[str, Any]:
        """
        获取单只股票的基本面核心指标（支持内存缓存、本地持久化与安全降级）
        """
        self._load_persistent_cache()
        if symbol in self._fundamental_cache and self._fundamental_cache[symbol].get("market_cap", 0) > 0:
            return self._fundamental_cache[symbol]

        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol)
            info = ticker.info or {}
            mcap = info.get("marketCap", 0) or 0
            fund_data = {
                "symbol": symbol,
                "market_cap": mcap,
                "rev_growth": info.get("revenueGrowth", 0.0) or 0.0,
                "eps_growth": info.get("earningsGrowth", 0.0) or 0.0,
                "roe": info.get("returnOnEquity", 0.0) or 0.0,
                "op_margin": info.get("operatingMargins", 0.0) or 0.0,
                "fcf": info.get("freeCashflow", 0) or 0,
            }
            if mcap > 0:
                self._fundamental_cache[symbol] = fund_data
                self._save_persistent_cache()
            return fund_data
        except Exception:
            # 若触发 429 或网络异常，若缓存中有历史有效数据则优先复用
            if symbol in self._fundamental_cache and self._fundamental_cache[symbol].get("market_cap", 0) > 0:
                return self._fundamental_cache[symbol]

            fallback = {
                "symbol": symbol,
                "market_cap": 0,
                "rev_growth": 0.0,
                "eps_growth": 0.0,
                "roe": 0.0,
                "op_margin": 0.0,
                "fcf": 0,
            }
            self._fundamental_cache[symbol] = fallback
            return fallback

    @classmethod
    def set_mock_fundamentals(cls, symbol: str, data: Dict[str, Any]) -> None:
        """
        用于单元测试与离线测试的 Mock 接口
        """
        cls._fundamental_cache[symbol] = data

    def _passes_fundamentals(self, fund: Dict[str, Any]) -> bool:
        if not fund:
            return False

        market_cap = fund.get("market_cap", 0) or 0
        rev_growth = fund.get("rev_growth", 0.0) or 0.0
        eps_growth = fund.get("eps_growth", 0.0) or 0.0
        roe = fund.get("roe", 0.0) or 0.0
        op_margin = fund.get("op_margin", 0.0) or 0.0
        fcf = fund.get("fcf", 0) or 0

        # 市值门槛 (>= $2B)
        if market_cap < self.min_market_cap:
            return False

        # 营收同比增速门槛 (>= 15%)
        if rev_growth < self.min_rev_growth:
            return False

        # ROE 或 EPS 增速门槛 (ROE >= 12% 或 EPS 增速 >= 15%)
        if roe < self.min_roe and eps_growth < self.min_eps_growth:
            return False

        # 营业利润率门槛 (>= 5%)
        if op_margin < self.min_op_margin:
            return False

        # 造血能力：自由现金流为正
        if fcf <= 0:
            return False

        return True

    @staticmethod
    def calc_growth_score(fund: Dict[str, Any]) -> float:
        """
        计算综合成长评分 (满分 100+，基于营收增长、净利增速、ROE 与营业利润率加权)
        """
        rev_growth = fund.get("rev_growth", 0.0) or 0.0
        eps_growth = fund.get("eps_growth", 0.0) or 0.0
        roe = fund.get("roe", 0.0) or 0.0
        op_margin = fund.get("op_margin", 0.0) or 0.0

        score = (
            min(rev_growth * 100, 150) * 0.35
            + min(eps_growth * 100, 150) * 0.25
            + min(roe * 100, 100) * 0.25
            + min(op_margin * 100, 100) * 0.15
        )
        return round(score, 1)

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        if len(history) < self.days_needed - 1:
            return False

        today_close = today[FieldKey.CLOSE.value]
        today_open = today[FieldKey.OPEN.value]
        today_low = today[FieldKey.LOW.value]
        today_high = today[FieldKey.HIGH.value]
        today_ma20 = today[FieldKey.MA20.value]
        today_ma50 = today[FieldKey.MA50.value]
        today_ma200 = today.get(FieldKey.MA200.value)
        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]

        # 1. 流动性过滤 (20 日均成交额 >= $5M)
        past_20_vol = history[FieldKey.DOLLAR_VOLUME.value].iloc[-20:]
        avg_dollar_vol_20 = past_20_vol.mean()
        if pd.isna(avg_dollar_vol_20) or avg_dollar_vol_20 < self.min_avg_dollar_vol:
            return False

        # 2. Stage 2 趋势通道过滤 (Close > MA50，MA50 > MA200)
        if pd.isna(today_close) or pd.isna(today_ma50):
            return False
        if today_close <= today_ma50:
            return False

        if not pd.isna(today_ma200) and today_ma50 < today_ma200:
            return False
        elif pd.isna(today_ma200) and today_ma20 < today_ma50:
            return False

        # 3. 相对强度：距 52 周最高价在 20% 以内 (领跑大盘龙头)
        past_high_series = (
            history[FieldKey.HIGH.value].iloc[-250:]
            if len(history) >= 250
            else history[FieldKey.HIGH.value]
        )
        high_52w = max(past_high_series.max(), today_high)
        if pd.isna(high_52w) or high_52w <= 0:
            return False
        if (high_52w - today_close) / high_52w > 0.20:
            return False

        # 4. 双买点模型 (突破 或 缩量企稳回踩)
        recent_20_close_max = history[FieldKey.CLOSE.value].iloc[-20:].max()
        is_breakout = (today_close >= recent_20_close_max) and (
            today_dollar_vol >= avg_dollar_vol_20 * 1.2
        )

        touch_ma50 = abs(today_low - today_ma50) / today_ma50 <= 0.025
        held_ma50 = today_close >= today_ma50
        is_volume_dryup = today_dollar_vol <= avg_dollar_vol_20 * 0.85
        is_bullish = today_close >= today_open
        is_pullback = touch_ma50 and held_ma50 and is_volume_dryup and is_bullish

        if not (is_breakout or is_pullback):
            return False

        # 5. 技术形态满足，获取基本面指标进行双重验证
        symbol = today.get(FieldKey.SYMBOL.value) or today.get("Symbol")
        if not symbol:
            return False

        fund = self.get_fundamentals(symbol)
        if not self._passes_fundamentals(fund):
            return False

        setup_type = "Stage 2 放量突破" if is_breakout else "MA50 缩量企稳回踩"
        self._cached_setup[symbol] = {
            "setup_type": setup_type,
            "fundamentals": fund,
            "high_52w": high_52w,
            "avg_dollar_vol_20": avg_dollar_vol_20,
        }
        return True

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        info = self._cached_setup.get(symbol, {})
        fund = info.get("fundamentals") or self.get_fundamentals(symbol)
        setup_type = info.get("setup_type", "Stage 2 突破")

        today_close = today[FieldKey.CLOSE.value]
        score = self.calc_growth_score(fund)

        market_cap = fund.get("market_cap", 0) or 0
        rev_growth = fund.get("rev_growth", 0.0) or 0.0
        eps_growth = fund.get("eps_growth", 0.0) or 0.0
        roe = fund.get("roe", 0.0) or 0.0
        op_margin = fund.get("op_margin", 0.0) or 0.0
        today_dollar_vol = today[FieldKey.DOLLAR_VOLUME.value]

        cap_str = (
            f"${market_cap / 1e9:.1f}B"
            if market_cap >= 1e9
            else f"${market_cap / 1e6:.1f}M"
        )

        return {
            "Symbol": symbol,
            "Close": f"${today_close:.2f}",
            "Setup": setup_type,
            "Growth Score": f"{score:.1f}",
            "Rev Growth": f"{rev_growth * 100:+.1f}%",
            "EPS Growth": f"{eps_growth * 100:+.1f}%",
            "ROE": f"{roe * 100:.1f}%",
            "Op Margin": f"{op_margin * 100:.1f}%",
            "Market Cap": cap_str,
            "Dollar Volume": f"${today_dollar_vol:,.2f}",
        }

    # ========= 排序规则 =========

    def get_sort_column(self) -> str:
        return "Growth Score"

    def is_sort_ascending(self) -> bool:
        return False
