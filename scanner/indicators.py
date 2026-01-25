from typing import Callable, Dict, Iterable, List
import pandas as pd

from scanner.fields import FieldKey

# ========= Indicator 数据结构 =========


class Indicator:
    """
    指标族（Indicator Family）

    一次计算可能生成多个 DataFrame 列
    """

    def __init__(
        self,
        name: str,
        output_columns: Iterable[FieldKey],
        func: Callable[[pd.DataFrame], pd.DataFrame],
    ):
        self.name = name
        self.output_columns = list(output_columns)
        self.func = func


# 全局指标注册表
INDICATORS: Dict[str, Indicator] = {}


def register_indicator(
    name: str,
    output_columns: Iterable[FieldKey],
) -> Callable[[Callable[[pd.DataFrame], pd.DataFrame]], Callable]:
    """
    注册一个指标族

    :param name:
        指标名称（仅用于日志 / 可读性）
    :param output_columns:
        该指标族会生成的 FieldKey
    """

    def wrapper(func: Callable[[pd.DataFrame], pd.DataFrame]):
        if name in INDICATORS:
            raise RuntimeError(f"重复注册指标: {name}")

        INDICATORS[name] = Indicator(
            name=name,
            output_columns=output_columns,
            func=func,
        )
        return func

    return wrapper


# ========= 指标计算 =========


@register_indicator(
    name="DollarVolume",
    output_columns=[FieldKey.DOLLAR_VOLUME],
)
def calc_dollar_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
    成交额（Dollar Volume）

    计算公式：
        Dollar_Volume = Close * Volume

    依赖字段：
        - Close
        - Volume
    """
    if (
        FieldKey.CLOSE.value not in df.columns
        or FieldKey.VOLUME.value not in df.columns
    ):
        raise RuntimeError("计算 DollarVolume 需要字段 Close 和 Volume")

    df[FieldKey.DOLLAR_VOLUME.value] = (
        df[FieldKey.CLOSE.value] * df[FieldKey.VOLUME.value]
    )
    return df


@register_indicator(
    name="MA",
    output_columns=[FieldKey.MA5, FieldKey.MA10],
)
def calc_ma(df: pd.DataFrame) -> pd.DataFrame:
    """
    简单移动平均线（MA）

    - MA5
    - MA10

    依赖字段：
        - Close
    """
    if FieldKey.CLOSE.value not in df.columns:
        raise RuntimeError("计算 MA 需要字段 Close")

    df[FieldKey.MA5.value] = df[FieldKey.CLOSE.value].rolling(5).mean()
    df[FieldKey.MA10.value] = df[FieldKey.CLOSE.value].rolling(10).mean()
    return df


@register_indicator(
    name="MACD",
    output_columns=[
        FieldKey.MACD_DIF,
        FieldKey.MACD_DEA,
        FieldKey.MACD_HISTOGRAM,
    ],
)
def calc_macd(df: pd.DataFrame) -> pd.DataFrame:
    """
    MACD 指标（12, 26, 9）

    生成字段：
        - MACD_DIF
        - MACD_DEA
        - MACD_Histogram

    依赖字段：
        - Close
    """
    if FieldKey.CLOSE.value not in df.columns:
        raise RuntimeError("计算 MACD 需要字段 Close")

    close = df[FieldKey.CLOSE.value]

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()

    df[FieldKey.MACD_DIF.value] = ema12 - ema26
    df[FieldKey.MACD_DEA.value] = (
        df[FieldKey.MACD_DIF.value].ewm(span=9, adjust=False).mean()
    )
    df[FieldKey.MACD_HISTOGRAM.value] = (
        df[FieldKey.MACD_DIF.value] - df[FieldKey.MACD_DEA.value]
    )
    return df


# ========= preprocess 主入口 =========


def preprocess_data(
    df: pd.DataFrame,
    required_fields: List[FieldKey],
) -> pd.DataFrame:
    """
    根据策略声明的 required_fields，
    自动计算所需的指标族，直到满足所有字段。

    :param df:
        原始历史行情 DataFrame
    :param required_fields:
        策略所需的 FieldKey 列表
    :raises RuntimeError:
        若最终仍缺失字段
    """
    if df is None or df.empty:
        raise RuntimeError("历史数据为空，无法计算指标")

    required_set = set(required_fields)

    for indicator in INDICATORS.values():
        # 该指标族是否“被需要”
        if not required_set.intersection(indicator.output_columns):
            continue

        # 是否已经全部存在
        if all(field.value in df.columns for field in indicator.output_columns):
            continue

        try:
            df = indicator.func(df)
        except Exception as e:
            raise RuntimeError(f"指标 {indicator.name} 计算失败: {e}") from e

    # 最终校验
    missing = [field.value for field in required_set if field.value not in df.columns]
    if missing:
        raise RuntimeError(f"指标计算后仍缺失字段: {missing}")

    return df
