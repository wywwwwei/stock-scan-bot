from enum import Enum


class FieldKey(str, Enum):
    """
    DataFrame 中可能存在 / 最终需要存在的字段语义

    - Enum 成员名：语义层（全大写，下划线）
    - value：DataFrame 实际列名
    """

    # ========= 原始行情字段（datasource 一定会提供） =========

    SYMBOL = "Symbol"  # 股票代码
    OPEN = "Open"  # 开盘价
    HIGH = "High"  # 最高价
    LOW = "Low"  # 最低价
    CLOSE = "Close"  # 收盘价
    VOLUME = "Volume"  # 成交量（股数）

    # ========= 派生字段（需要 preprocess / indicator 计算） =========

    DOLLAR_VOLUME = "Dollar_Volume"  # 成交额 = Close * Volume

    MA5 = "MA5"  # 5 日均线
    MA10 = "MA10"  # 10 日均线

    MACD_DIF = "MACD_DIF"  # MACD 快线（DIF）
    MACD_DEA = "MACD_DEA"  # MACD 慢线（DEA）
    MACD_HISTOGRAM = "MACD_Histogram"  # MACD 柱状图
