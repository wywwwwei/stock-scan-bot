from typing import List
import requests
import pandas as pd
import io

from scanner.datasource import YahooFinanceDataSource
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


def resolve_stock_universe(
    target_stocks: List[str], datasource: YahooFinanceDataSource
) -> List[str]:
    """
    构建最终扫描股票池（Universe）。

    规则：
    1. 若 TARGET_STOCKS 非空：
       - 仅扫描其中存在于 NASDAQ 的股票
       - 不应用 Prefilter（完全尊重用户显式选择）
    2. 若 TARGET_STOCKS 为空：
       - 若 ENABLE_PREFILTER=True：
           使用全量 NASDAQ + Prefilter
       - 若 ENABLE_PREFILTER=False：
           直接扫描全量 NASDAQ

    :param target_stocks: 用户显式指定的股票列表
    :param datasource: YahooFinance 数据源（用于 Prefilter）
    :return: 最终用于扫描的股票列表
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

    return build_universe_with_prefilter(all_symbols, datasource)


def build_universe_with_prefilter(
    all_symbols: List[str], datasource: YahooFinanceDataSource
) -> List[str]:
    """
    基于全量 NASDAQ 股票列表，应用基础 Prefilter 构建扫描股票池。

    设计说明：
    - Prefilter 与 run 阶段共用 YahooFinanceDataSource
    - 使用 RateLimiter 控制整体请求速率
    - Prefilter 只使用「最近一个已完成交易日」
    - 目标是以最小成本，显著缩小后续扫描规模
    """

    print("[INFO] TARGET_STOCKS 为空，将扫描全量 NASDAQ，并应用 Prefilter")
    print(
        f"[INFO] Prefilter 条件："
        f"最低成交额={PREFILTER_MIN_DOLLAR_VOLUME:,.0f} USD，"
        f"最低收盘价={PREFILTER_MIN_CLOSE_PRICE}"
    )

    filtered: List[str] = []
    total = len(all_symbols)

    for idx, symbol in enumerate(all_symbols, start=1):
        df = datasource.history(symbol, days=2)
        bar = extract_last_completed_bar(df)
        if bar is None:
            continue

        if passes_basic_prefilter(
            df.iloc[-1],
            PREFILTER_MIN_DOLLAR_VOLUME,
            PREFILTER_MIN_CLOSE_PRICE,
        ):
            filtered.append(symbol)

        if idx % 100 == 0 or idx == total:
            print(f"[INFO] Prefilter 进度 {idx}/{total}")

    print(f"[INFO] Prefilter 完成，通过 {len(filtered)} / {total} 只股票")
    if not filtered:
        print("[WARN] Prefilter 后股票池为空，请检查过滤条件是否过严")

    datasource.stats.print_summary("Prefilter")
    datasource.stats.reset()

    return filtered


def extract_last_completed_bar(df: pd.DataFrame) -> pd.Series | None:
    """
    从 YahooFinance 返回的 DataFrame 中提取
    「最近一个已完成交易日」的 bar。

    规则：
    - 至少需要 2 行数据
    - 始终取倒数第二行
    """

    if df is None or df.empty:
        return None

    # 至少要有 2 天，才能保证上一交易日存在
    if len(df) < 2:
        return None

    return df.iloc[-2]


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
