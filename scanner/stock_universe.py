from typing import List
import requests
import pandas as pd
import io

from scanner.prefilter_datasource import YahooBatchPrefilterDataSource
from scanner.fields import FieldKey
from scanner.config.scan import (
    ENABLE_PREFILTER,
    PREFILTER_MIN_CLOSE_PRICE,
    PREFILTER_MIN_DOLLAR_VOLUME,
)

NASDAQ_LIST_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"


def get_nasdaq_symbols() -> List[str]:
    """
    从 NASDAQ 官方下载 nasdaqlisted.txt，解析股票代码列表。

    :return: 股票代码列表（可能为空）
    """
    try:
        print("[INFO] 下载 NASDAQ 股票列表...")
        response = requests.get(NASDAQ_LIST_URL, timeout=15)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text), sep="|")

        if "Symbol" not in df.columns:
            print("[ERROR] NASDAQ 文件中不存在 Symbol 列")
            return []

        # 清洗非法 Symbol
        df = df.dropna(subset=["Symbol"])
        df = df[df["Symbol"].str.strip().ne("")]

        symbols = df["Symbol"].tolist()
        print(f"[INFO] 成功获取 NASDAQ 股票 {len(symbols)} 只")

        return symbols

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] 下载 NASDAQ 股票列表失败: {e}")
        return []

    except pd.errors.EmptyDataError:
        print("[ERROR] NASDAQ 股票文件为空或格式错误")
        return []

    except Exception as e:
        print(f"[ERROR] 解析 NASDAQ 股票列表异常: {e}")
        return []


def resolve_stock_universe(target_stocks: List[str]) -> List[str]:
    """
    构建最终扫描股票池（Universe）。

    决策规则（按优先级）：
    1. 若 TARGET_STOCKS 非空：
       - 仅扫描用户显式指定的股票
       - 只保留其中实际存在于 NASDAQ 的代码
       - 不应用 Prefilter（完全尊重用户选择）

    2. 若 TARGET_STOCKS 为空：
       - 若 ENABLE_PREFILTER = True：
           使用「全量 NASDAQ + Prefilter」自动缩小股票池
       - 若 ENABLE_PREFILTER = False：
           直接扫描全量 NASDAQ 股票（不做任何预过滤）

    设计说明：
    - Prefilter 仅用于缩小股票池规模，降低后续扫描成本
    - Prefilter 不参与任何策略判断
    - Prefilter 只在“扫描全市场”时生效

    :param target_stocks:
        用户显式指定的股票列表（可为空）
    :return:
        最终用于扫描的股票代码列表
    """
    all_symbols = get_nasdaq_symbols()

    if not all_symbols:
        print("[ERROR] 无法获取 NASDAQ 股票列表，终止扫描")
        return []

    # ===== 用户显式指定股票（不应用 Prefilter）=====
    if target_stocks:
        print(f"[INFO] 使用 TARGET_STOCKS，共 {len(target_stocks)} 只")
        valid = [s for s in target_stocks if s in all_symbols]
        print(f"[INFO] 过滤后有效股票 {len(valid)} / {len(target_stocks)}")
        return valid

    # ===== 全量 NASDAQ（是否启用 Prefilter）=====
    if not ENABLE_PREFILTER:
        print(
            "[INFO] TARGET_STOCKS 为空，但已关闭 Prefilter，"
            f"将直接扫描全量 NASDAQ（{len(all_symbols)} 只）"
        )
        return all_symbols

    return build_universe_with_prefilter(all_symbols)


def build_universe_with_prefilter(all_symbols: List[str]) -> List[str]:
    """
    基于全量 NASDAQ 股票列表，应用基础 Prefilter 构建扫描股票池。

    设计说明：
    - 仅在 TARGET_STOCKS 为空且 ENABLE_PREFILTER=True 时调用
    - 每只股票仅使用「最近一个已完成交易日」的数据
    - Prefilter 目标是：
        用极低的判断成本，显著减少后续 run 阶段的 HTTP 请求数量

    Prefilter 不做的事情：
    - 不计算技术指标
    - 不参与策略逻辑
    - 不保证精确，只保证“足够粗”

    :param all_symbols:
        全量 NASDAQ 股票代码列表
    :return:
        通过 Prefilter 的股票列表
    """

    print("[INFO] TARGET_STOCKS 为空，将扫描全量 NASDAQ，并应用 Prefilter")
    print(
        f"[INFO] Prefilter 条件："
        f"最低成交额={PREFILTER_MIN_DOLLAR_VOLUME:,.0f} USD，"
        f"最低收盘价={PREFILTER_MIN_CLOSE_PRICE}"
    )

    datasource = YahooBatchPrefilterDataSource(
        batch_size=25,
        sleep_sec=0.1,
    )
    bars = datasource.fetch_last_completed_bars(all_symbols)

    filtered = [
        symbol
        for symbol, bar in bars.items()
        if passes_basic_prefilter(
            bar,
            PREFILTER_MIN_DOLLAR_VOLUME,
            PREFILTER_MIN_CLOSE_PRICE,
        )
    ]

    datasource.stats.print_summary("Prefilter (Yahoo Batch)")

    return filtered


def passes_basic_prefilter(
    bar: pd.Series,
    min_dollar_volume: float,
    min_close_price: float,
) -> bool:
    """
    基础流动性 / 价格预过滤（单日级别）。

    使用约定：
    - bar 必须是「最近一个已完成交易日」
    - 不使用盘中或未收盘数据
    - 不计算任何技术指标
    """

    if bar is None or bar.empty:
        return False

    close = bar[FieldKey.CLOSE.value]
    volume = bar[FieldKey.VOLUME.value]

    if close < min_close_price:
        return False

    if close * volume < min_dollar_volume:
        return False

    return True
