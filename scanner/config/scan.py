from typing import Dict, List
from strategy.volume_surge import VolumeSurgeStrategy
from strategy.ma_cross import MACrossStrategy
from strategy.cd_signal import CDSignalStrategy

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

# ===== 并发参数 =====
SCAN_MAX_WORKERS: int = 10

# ===== yfinance 限流参数 =====
YF_MAX_CALLS_PER_SEC: int = 20
