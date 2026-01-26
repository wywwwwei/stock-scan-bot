from typing import List
import requests
import pandas as pd
import io

from scanner.datasource import YahooFinanceDataSource
from scanner.prefilters.prefilter import StockPreFilter
from scanner.config.scan import PREFILTER_MAX_LOOKBACK_DAYS

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
    target_stocks: List[str],
    datasource: YahooFinanceDataSource,
    prefilters: List[StockPreFilter],
) -> List[str]:
    """
    构建最终扫描股票池（Universe）。

    规则：
    1. 若 TARGET_STOCKS 非空：
       - 仅扫描其中存在于 NASDAQ 的股票
       - 不应用 Prefilter（完全尊重用户选择）
    2. 若 TARGET_STOCKS 为空：
       - 使用全量 NASDAQ 股票
       - 应用 Prefilter 自动收缩股票池

    :param target_stocks: 用户显式指定的股票列表
    :param datasource: 数据源（用于 Prefilter）
    :param prefilters: Prefilter 列表（仅在扫描全体 NASDAQ 时生效）
    :return: 最终用于扫描的股票列表
    """
    all_symbols = get_nasdaq_symbols()

    if not all_symbols:
        print("[ERROR] 无法获取 NASDAQ 股票列表，终止扫描")
        return []

    if target_stocks:
        print(f"[INFO] 使用 TARGET_STOCKS，共 {len(target_stocks)} 只")
        valid = [s for s in target_stocks if s in all_symbols]
        print(f"[INFO] 过滤后有效股票 {len(valid)} / {len(target_stocks)}")
        return valid

    filtered_symbols = apply_prefilters(all_symbols, datasource, prefilters)
    return filtered_symbols


def apply_prefilters(
    symbols: List[str],
    datasource: YahooFinanceDataSource,
    prefilters: List[StockPreFilter],
) -> List[str]:
    """
    对股票池应用 Prefilter，仅用于扫描“全体 NASDAQ”的场景。

    :param symbols: 初始股票池（通常为全量 NASDAQ）
    :param datasource: 数据源，用于获取少量历史数据
    :param prefilters: Prefilter 实例列表
    :return: 通过 Prefilter 的股票列表
    """
    if not prefilters:
        print("[INFO] 未配置 PREFILTERS，跳过预过滤")
        return symbols

    kept: List[str] = []

    print(f"[INFO] 开始应用 Prefilter，初始股票数={len(symbols)}")

    for idx, symbol in enumerate(symbols, start=1):
        try:
            df: pd.DataFrame = datasource.history(
                symbol,
                days=PREFILTER_MAX_LOOKBACK_DAYS,
            )

            if all(p.filter(symbol, df) for p in prefilters):
                kept.append(symbol)

        except Exception as e:
            print(f"[WARN] Prefilter 跳过 {symbol}: {e}")

        if idx % 100 == 0:
            print(f"[INFO] Prefilter 进度 {idx}/{len(symbols)}")

    print(f"[INFO] Prefilter 完成，通过 {len(kept)} / {len(symbols)} 只股票")
    return kept
