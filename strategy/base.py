from typing import List, Dict, Any
import pandas as pd

from scanner.fields import FieldKey


class BaseStrategy:
    """
    所有选股策略的基类。

    约定：
    - DataFrame 内部字段统一使用 FieldKey
    - format_result 输出的是“展示层 dict”
    - 排序基于 format_result 的 key，而不是 df 字段
    """

    # ========= 基本信息 =========

    def get_name(self) -> str:
        """
        返回策略唯一名称（默认使用类名）
        """
        return self.__class__.__name__

    def get_description(self) -> str:
        """
        返回策略的文字描述（用于日志 / 邮件）
        """
        raise NotImplementedError

    # ========= 数据需求 =========

    def get_required_days(self) -> int:
        """
        返回该策略所需的历史数据天数（包含当日）
        """
        raise NotImplementedError

    def get_required_fields(self) -> List[FieldKey]:
        """
        返回该策略执行所需的 DataFrame 字段语义（FieldKey）

        preprocess 会根据这些字段，自动补齐指标计算。
        """
        return [
            FieldKey.CLOSE,
            FieldKey.VOLUME,
        ]

    # ========= 策略判断 =========

    def check_condition(
        self,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> bool:
        """
        判断当日是否满足策略条件。

        :param today:
            当日数据（df 的最后一行）
        :param history:
            历史数据（不包含当日）
        """
        raise NotImplementedError

    # ========= 结果输出 =========

    def format_result(
        self,
        symbol: str,
        today: pd.Series,
        history: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        将命中结果格式化为 dict，用于展示 / 邮件 / 排序。

        约定：
        - key 为“展示字段名”（字符串）
        - value 为可直接展示的值（str / number）
        """
        raise NotImplementedError

    # ========= 排序规则 =========

    def get_sort_column(self) -> str:
        """
        返回排序所使用的字段名。

        ⚠️ 该字段名必须存在于 format_result() 返回的 dict 中。
        """
        raise NotImplementedError

    def is_sort_ascending(self) -> bool:
        """
        返回排序方向：
        - True  : 升序
        - False : 降序（默认）
        """
        return False
