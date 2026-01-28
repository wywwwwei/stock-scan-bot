from typing import List, Dict
import time

import pandas as pd
import yfinance as yf

from scanner.fields import FieldKey
from utils.request_stat import RequestStats


class YahooBatchPrefilterDataSource:
    """
    Yahoo Finance 批量 Prefilter 数据源

    用途：
    - 仅用于构建全量 NASDAQ universe
    - 基于「上一个完整交易日」
    - 极大减少 HTTP 请求数量
    """

    def __init__(
        self,
        batch_size: int = 100,
        timeout_sec: float = 20.0,
        sleep_sec: float = 0.0,
    ):
        """
        :param batch_size: 每批请求的 ticker 数量（建议 50~200）
        :param timeout_sec: 单次 HTTP 超时
        :param sleep_sec: 每批请求后的 sleep（防止被 Yahoo 限制）
        """
        self.batch_size = batch_size
        self.timeout_sec = timeout_sec
        self.sleep_sec = sleep_sec

        self.stats = RequestStats()

    # ===============================
    # Public API
    # ===============================

    def fetch_last_completed_bars(
        self,
        symbols: List[str],
    ) -> Dict[str, pd.Series]:
        """
        批量获取每只股票「上一个完整交易日」的数据

        :return:
            {
                "AAPL": pd.Series,
                "MSFT": pd.Series,
                ...
            }
        """
        results: Dict[str, pd.Series] = {}

        total = len(symbols)
        batches = [
            symbols[i : i + self.batch_size] for i in range(0, total, self.batch_size)
        ]

        print(
            f"[INFO] Yahoo Batch Prefilter: "
            f"{total} symbols → {len(batches)} batches "
            f"(batch_size={self.batch_size})"
        )

        for idx, batch in enumerate(batches, start=1):
            t0 = time.perf_counter()
            t1 = t0
            success = False

            try:
                df = yf.download(
                    tickers=" ".join(batch),
                    period="2d",
                    interval="1d",
                    group_by="ticker",
                    auto_adjust=False,
                    threads=False,
                    progress=False,
                    timeout=self.timeout_sec,
                )
                t1 = time.perf_counter()

                if df is None or df.empty:
                    raise RuntimeError("empty dataframe")

                self._extract_last_completed_day(df, batch, results)

                success = True

            except Exception as e:
                print(f"[WARN] Yahoo batch prefilter failed: {e}")

            finally:
                t2 = time.perf_counter()

                self.stats.record(
                    success=success,
                    wait_time=0.0,
                    request_time=(t1 - t0),
                    total_time=(t2 - t0),
                )

                if self.sleep_sec > 0:
                    time.sleep(self.sleep_sec)

                if idx % 5 == 0 or idx == len(batches):
                    print(f"[INFO] Prefilter batch progress " f"{idx}/{len(batches)}")

        return results

    def _extract_last_completed_day(
        self,
        df: pd.DataFrame,
        batch: List[str],
        out: Dict[str, pd.Series],
    ) -> None:
        """
        从 yf.download 的结果中，提取每个 symbol 的上一交易日数据
        """
        for symbol in batch:
            try:
                if symbol not in df.columns.get_level_values(0):
                    continue

                sub = df[symbol].dropna(how="all")
                if len(sub) < 2:
                    continue

                bar = sub.iloc[-2]  # 上一个完整交易日

                out[symbol] = pd.Series(
                    {
                        FieldKey.CLOSE.value: bar["Close"],
                        FieldKey.VOLUME.value: bar["Volume"],
                    }
                )

            except Exception:
                continue
