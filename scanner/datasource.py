from typing import List, Dict
import yfinance as yf
import pandas as pd
import time

from utils.rate_limiter import RateLimiter
from utils.request_stat import RequestStats


class YahooFinanceDataSource:
    """
    yfinance 数据访问层
    - 内置 RateLimiter
    - 支持单股拉取 history() 与受控批量拉取 history_batch()
    - 所有网络请求必须经过这里
    """

    def __init__(self, max_calls_per_sec: int):
        # 平滑限流：每 (1/max_calls_per_sec) 秒放行 1 次请求
        self._limiter = RateLimiter(1, 1.0 / max_calls_per_sec)
        self.stats = RequestStats()

    def history(self, ticker: str, days: int) -> pd.DataFrame:
        """
        拉取单只股票最近 N 天历史行情

        :param ticker: 股票代码
        :param days: 最近天数
        :return: 历史行情 DataFrame（可能为空）
        """
        if days <= 0:
            print(f"[WARN] {ticker} 请求天数非法: {days}")
            return pd.DataFrame()

        t0 = time.perf_counter()
        t1 = t0
        t2 = t0
        success = False
        try:
            self._limiter.acquire()
            t1 = time.perf_counter()

            df = yf.Ticker(ticker).history(period=f"{days}d")
            t2 = time.perf_counter()

            if df is None or df.empty:
                print(f"[WARN] {ticker} 历史数据为空")
                return pd.DataFrame()
            success = True
            return df

        except Exception as e:
            print(f"[ERROR] yfinance 请求失败 {ticker}: {e}")
            return pd.DataFrame()

        finally:
            t_end = time.perf_counter()

            wait_time = t1 - t0
            request_time = t2 - t1
            total_time = t_end - t0

            self.stats.record(
                success,
                wait_time,
                request_time,
                total_time,
            )

    def history_batch(
        self,
        tickers: List[str],
        days: int,
        timeout_sec: float = 30.0,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量拉取多只股票最近 N 天历史行情（采用 yf.download 批量接口）

        :param tickers: 股票代码列表
        :param days: 最近天数
        :param timeout_sec: HTTP 请求超时时间（秒）
        :return: { ticker: DataFrame } 字典
        """
        if not tickers or days <= 0:
            return {}

        results: Dict[str, pd.DataFrame] = {}
        t0 = time.perf_counter()
        t1 = t0
        t2 = t0
        success = False

        try:
            self._limiter.acquire()
            t1 = time.perf_counter()

            df = yf.download(
                tickers=" ".join(tickers),
                period=f"{days}d",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=False,
                progress=False,
                timeout=timeout_sec,
            )
            t2 = time.perf_counter()

            if df is not None and not df.empty:
                # 1. 多股票场景：columns 为 MultiIndex (ticker, field)
                if df.columns.nlevels == 2:
                    level_0_tickers = set(df.columns.get_level_values(0))
                    for ticker in tickers:
                        if ticker in level_0_tickers:
                            sub_df = df[ticker].dropna(how="all")
                            if not sub_df.empty:
                                results[ticker] = sub_df
                # 2. 单股票场景：columns 可能为单层 Index
                elif df.columns.nlevels == 1 and len(tickers) == 1:
                    ticker = tickers[0]
                    sub_df = df.dropna(how="all")
                    if not sub_df.empty:
                        results[ticker] = sub_df

            success = bool(results)

        except Exception as e:
            print(f"[ERROR] yfinance 批量拉取异常 ({len(tickers)} 只股票): {e}")

        finally:
            t_end = time.perf_counter()
            self.stats.record(
                success=success,
                wait_time=t1 - t0,
                request_time=t2 - t1,
                total_time=t_end - t0,
            )

        return results

