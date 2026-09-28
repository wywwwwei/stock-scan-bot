from typing import Dict, List
from strategy.volume_surge import VolumeSurgeStrategy
from strategy.ma_cross import MACrossStrategy
from strategy.cd_signal import CDSignalStrategy
from strategy.trend_pullback import TrendPullbackStrategy
from strategy.new_high_breakout import NewHighBreakoutStrategy
from strategy.ttm_squeeze import TTMSqueezeStrategy
from strategy.penny_base_ignition import PennyBaseIgnitionStrategy
from strategy.penny_stealth_accumulation import PennyStealthAccumulationStrategy

# 业务扫描配置（股票池 / 策略 / 并发 / 速率）

# ===== 默认策略（未在 STOCK_STRATEGY_MAP 中单独指定的股票使用）=====
EXECUTE_STRATEGIES = [
    CDSignalStrategy(),
    MACrossStrategy(),
    VolumeSurgeStrategy(),
    TrendPullbackStrategy(),
    NewHighBreakoutStrategy(),
    TTMSqueezeStrategy(),
    PennyBaseIgnitionStrategy(),
    PennyStealthAccumulationStrategy(),
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

# 是否在扫描股票池中包含 ETF
# - True : 包含各类 ETF 基金（如 TQQQ, SOXL, 行业与杠杆 ETF 等）
# - False: 自动剔除 ETF，仅扫描真正的公司普通个股（默认推荐）
INCLUDE_ETFS: bool = False

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

# Prefilter 开关
# 是否在扫描「全量 NASDAQ」时启用 Prefilter
# - True : 启用（默认，推荐）
# - False: 完全关闭，直接扫描全量 NASDAQ
ENABLE_PREFILTER: bool = True

# 最低单日成交额（美元）
# 用于剔除：
# - 流动性极差的股票
# - 几乎没有交易的壳股 / 僵尸股
PREFILTER_MIN_DOLLAR_VOLUME = 2_000_000
# 最低收盘价
# 用于剔除：
# - 仙股（Penny Stocks），长期小于1可能有退市风险
# - 容易出现极端波动 / 数据噪声的标的
PREFILTER_MIN_CLOSE_PRICE = 1

# Prefilter 批处理大小（建议 60~100，避免单批过大或过碎）
PREFILTER_BATCH_SIZE: int = 80

# 批次之间的休眠间隔（秒，主动冷却避免触发 Yahoo 429 限流）
PREFILTER_SLEEP_SEC: float = 0.3

# ===== 并发参数 =====
SCAN_MAX_WORKERS: int = 10

# ===== yfinance 限流参数 =====
YF_MAX_CALLS_PER_SEC: int = 4

# ===== 历史行情批量拉取参数 =====
# 采用“受控小批次 + 批次间充分休眠”模式，大幅减少 HTTP 请求数，同时避免 429 限流
HISTORY_BATCH_SIZE: int = 40          # 历史数据每批请求的股票数量（建议 30~50）
HISTORY_BATCH_SLEEP_SEC: float = 1.0  # 批次之间的休眠间隔（秒，建议 1.0~1.5 秒）
HISTORY_TIMEOUT_SEC: float = 30.0     # 单批 HTTP 超时时间（秒）

