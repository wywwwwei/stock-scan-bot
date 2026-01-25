from typing import List
import requests
import pandas as pd
import io


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
    根据 TARGET_STOCKS 决定最终扫描股票池。

    规则：
    - 若 TARGET_STOCKS 非空 → 仅扫描其中存在于 NASDAQ 的股票
    - 若 TARGET_STOCKS 为空 → 扫描全部 NASDAQ 股票

    :param target_stocks: 用户配置的目标股票列表
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

    print(
        f"[INFO] TARGET_STOCKS 为空，将扫描全部 " f"{len(all_symbols)} 只 NASDAQ 股票"
    )
    return all_symbols
