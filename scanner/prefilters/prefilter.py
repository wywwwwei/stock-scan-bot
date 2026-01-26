from typing import Protocol
import pandas as pd


class StockPreFilter(Protocol):
    """
    股票池预过滤器接口
    """

    def filter(
        self,
        symbol: str,
        df: pd.DataFrame,
    ) -> bool:
        """
        返回 True 表示保留该股票
        返回 False 表示剔除
        """
        ...
