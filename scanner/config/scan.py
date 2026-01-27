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

# ===== Prefilter 参数（仅用于扫描全量 NASDAQ 时）=====
#
# Prefilter 的定位：
# - 仅作为“股票池入口的粗过滤”
# - 目的是大幅减少后续策略阶段的扫描数量
# - 不参与任何策略判断，也不计算技术指标
#
# 注意：
# - Prefilter 只在 TARGET_STOCKS 为空时生效
# - 如果用户显式指定了 TARGET_STOCKS，将完全跳过 Prefilter

# 最低单日成交额（美元）
# 用于剔除：
# - 流动性极差的股票
# - 几乎没有交易的壳股 / 僵尸股
PREFILTER_MIN_DOLLAR_VOLUME = 1_000_000
# 最低收盘价
# 用于剔除：
# - 仙股（Penny Stocks），长期小于1可能有退市风险
# - 容易出现极端波动 / 数据噪声的标的
PREFILTER_MIN_CLOSE_PRICE = 0.5

# ===== 并发参数 =====
SCAN_MAX_WORKERS: int = 10

# ===== yfinance 限流参数 =====
YF_MAX_CALLS_PER_SEC: int = 3
