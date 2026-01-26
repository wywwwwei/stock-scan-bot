import yfinance as yf
import pandas as pd
import time

from utils.rate_limiter import RateLimiter
from utils.request_stat import RequestStats


class YahooFinanceDataSource:
    """
    yfinance 数据访问层
    - 内置 RateLimiter
    - 所有网络请求必须经过这里
    """

    def __init__(self, max_calls_per_sec: int):
        # 平滑限流：每 (1/max_calls_per_sec) 秒放行 1 次请求
        self._limiter = RateLimiter(1, 1.0 / max_calls_per_sec)
        self.stats = RequestStats()

    def history(self, ticker: str, days: int) -> pd.DataFrame:
        """
        拉取指定股票最近 N 天历史行情

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
