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
    MA20 = "MA20"  # 20 日均线
    MA50 = "MA50"  # 50 日均线
    MA200 = "MA200"  # 200 日均线

    MACD_DIF = "MACD_DIF"  # MACD 快线（DIF）
    MACD_DEA = "MACD_DEA"  # MACD 慢线（DEA）
    MACD_HISTOGRAM = "MACD_Histogram"  # MACD 柱状图

    # ========= 波动率挤压字段（TTM Squeeze） =========
    BB_UPPER = "BB_Upper"  # 布林带上轨
    BB_LOWER = "BB_Lower"  # 布林带下轨
    KC_UPPER = "KC_Upper"  # 肯特纳通道上轨
    KC_LOWER = "KC_Lower"  # 肯特纳通道下轨
    SQUEEZE_ON = "Squeeze_On"  # 是否处于挤压状态 (布林带在肯特纳通道内)
