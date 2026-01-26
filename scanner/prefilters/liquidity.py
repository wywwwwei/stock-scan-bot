import pandas as pd
from scanner.prefilters.prefilter import StockPreFilter
from scanner.fields import FieldKey


class LiquidityAndPriceFilter:
    """
    过滤：
    - 平均成交额 < min_avg_dollar_volume
    - 最新收盘价 < min_close_price
    """

    def __init__(
        self,
        min_avg_dollar_volume: float = 5_000_000,
        min_close_price: float = 0.5,
        lookback_days: int = 20,
    ):
        self.min_avg_dollar_volume = min_avg_dollar_volume
        self.min_close_price = min_close_price
        self.lookback_days = lookback_days

    def filter(self, symbol: str, df: pd.DataFrame) -> bool:
        if df is None or df.empty:
            return False

        # 最新收盘价
        latest_close = df[FieldKey.CLOSE].iloc[-1]
        if latest_close < self.min_close_price:
            return False

        # 平均成交额
        recent = df.tail(self.lookback_days)
        avg_dollar_volume = (recent[FieldKey.CLOSE] * recent[FieldKey.VOLUME]).mean()

        return avg_dollar_volume >= self.min_avg_dollar_volume
