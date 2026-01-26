from typing import Dict, List
from strategy.volume_surge import VolumeSurgeStrategy
from strategy.ma_cross import MACrossStrategy
from strategy.cd_signal import CDSignalStrategy
from scanner.prefilters.liquidity import LiquidityAndPriceFilter

# 业务扫描配置（股票池 / 策略 / 并发 / 速率）

# ===== 默认策略（未在 STOCK_STRATEGY_MAP 中单独指定的股票使用）=====
EXECUTE_STRATEGIES = [
    CDSignalStrategy(),
    MACrossStrategy(),
    VolumeSurgeStrategy(),
]

# ===== 股票 → 策略 映射（可覆盖默认策略）=====
# key为股票代码，value为策略实例列表。
# 如果股票不在这个映射中，则使用 EXECUTE_STRATEGIES
STOCK_STRATEGY_MAP: Dict[str, List] = {
    # "AAPL": [VolumeSurgeStrategy()], # AAPL只扫描成交量
    # "MSFT": [MACrossStrategy(), CDSignalStrategy()], # MSFT扫描均线和CD
    # "ZYME": [CDSignalStrategy()], # ZYME只扫描CD
}

# ===== 扫描股票池 =====
# 留空([])则扫描所有股票
TARGET_STOCKS: List[str] = [
    # "AAPL", "MSFT", "GOOGL" # 示例：只扫描这几只股票
    # 如果列表为空，则扫描所有股票
]

# ===== 股票池预过滤器 =====
#
# Prefilter 用于在“扫描全部 NASDAQ 股票”时，提前剔除
# 明显不具备交易价值的股票，以减少数据请求量和扫描成本。
#
# ⚠️ 重要约定：
# - Prefilter 只在 TARGET_STOCKS 为空时生效
# - 如果用户显式指定了 TARGET_STOCKS，则认为用户已明确
#   选择了扫描对象，不再额外应用 Prefilter
#
# Prefilter 的典型用途包括：
# - 剔除低流动性股票（如平均成交额过低）
# - 剔除极低价股票（如 penny stocks）
# - 剔除不活跃股票
#
# Prefilter 不应包含复杂技术指标判断，
# 仅用于“是否值得进一步扫描”的粗过滤。
#
PREFILTERS = [
    LiquidityAndPriceFilter(
        min_avg_dollar_volume=3_000_000,  # 最近 N 天平均成交额不少于 300 万美元
        min_close_price=0.5,  # 最新收盘价不少于 0.5 美元
        lookback_days=10,  # 用于计算平均成交额的回看天数，不能超过PREFILTER_MAX_LOOKBACK_DAYS
    )
]
PREFILTER_MAX_LOOKBACK_DAYS = 30

# ===== 并发参数 =====
SCAN_MAX_WORKERS: int = 10

# ===== yfinance 限流参数 =====
YF_MAX_CALLS_PER_SEC: int = 1
