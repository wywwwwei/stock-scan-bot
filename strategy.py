import pandas as pd


class BaseStrategy:
    """
    所有筛选策略的基类。
    定义了策略必须实现的接口。
    """

    def get_name(self):
        """返回此策略的唯一标识名，默认为类名"""
        return self.__class__.__name__

    def get_required_days(self):
        """返回此策略需要获取的历史数据天数"""
        raise NotImplementedError

    def get_required_data_columns(self):
        """返回此策略需要的特定数据列名列表"""
        # 默认需要 Close 和 Volume
        return ["Close", "Volume"]

    def check_condition(self, data_row, past_data):
        """
        检查单个交易日的数据是否满足策略条件。
        :param data_row: 当日数据 (pandas Series)
        :param past_data: 历史数据 (pandas DataFrame)
        :return: True if condition is met, False otherwise
        """
        raise NotImplementedError

    def format_result(self, ticker, data_row, past_data):
        """
        格式化满足条件的股票结果。
        :param ticker: 股票代码
        :param data_row: 当日数据 (pandas Series)
        :param past_data: 历史数据 (pandas DataFrame)
        :return: A dictionary representing the result row.
        """
        raise NotImplementedError

    def get_description(self):
        """返回策略的描述，用于邮件标题"""
        raise NotImplementedError

    def get_sort_column(self):
        """返回用于排序的列名"""
        raise NotImplementedError

    def get_sort_ascending(self):
        """返回排序顺序 (True for ascending, False for descending)"""
        # 默认按降序排列
        return False


class VolumeSurgeStrategy(BaseStrategy):
    """
    策略1: 成交额异动股
    当日成交额大于过去60天平均成交额2倍。
    """

    def __init__(self):
        self.days_needed = 61  # 60天历史数据 + 当日

    def get_required_days(self):
        return self.days_needed

    def get_required_data_columns(self):
        cols = super().get_required_data_columns()
        return cols + ["DollarVolume"]  # 需要计算成交额

    def check_condition(self, data_row, past_data):
        if len(past_data) != self.days_needed - 1:
            print(
                f"警告: VolumeSurgeStrategy 预期 past_data 有 {self.days_needed - 1} 行，但收到了 {len(past_data)} 行。"
            )
            return False

        # 过去60天的成交额
        avg_dollar_vol_60 = past_data["DollarVolume"].mean()
        if pd.isna(avg_dollar_vol_60) or avg_dollar_vol_60 == 0:
            return False

        current_dollar_volume = data_row["DollarVolume"]
        ratio = current_dollar_volume / avg_dollar_vol_60
        return ratio > 2

    def format_result(self, ticker, data_row, past_data):
        avg_dollar_vol_60 = past_data["DollarVolume"].mean()
        current_dollar_volume = data_row["DollarVolume"]
        ratio = current_dollar_volume / avg_dollar_vol_60
        return {
            "Symbol": ticker,
            "Current Dollar Volume": f"${current_dollar_volume:,.2f}",
            "60-Day Avg Dollar Volume": f"${avg_dollar_vol_60:,.2f}",
            "Ratio (Current / 60-Day Avg)": f"{ratio:.2f}",
        }

    def get_description(self):
        return "成交额异动股 (按当日成交额排序)"

    def get_sort_column(self):
        return "Current Dollar Volume Num"

    def get_sort_ascending(self):
        return False  # 按成交额降序


class MACrossStrategy(BaseStrategy):
    """
    策略2: MA5上穿MA10且过去10天平均交易额>5000万美元
    """

    def __init__(self):
        self.days_needed = 11  # 10天历史数据 + 当日

    def get_required_days(self):
        return self.days_needed

    def get_required_data_columns(self):
        cols = super().get_required_data_columns()
        return cols + ["DollarVolume", "MA5", "MA10"]  # 需要均线和成交额

    def check_condition(self, data_row, past_data):
        if len(past_data) != self.days_needed - 1:
            print(
                f"警告: MACrossStrategy 预期 past_data 有 {self.days_needed - 1} 行，但收到了 {len(past_data)} 行。"
            )
            return False

        # 检查MA5是否上穿MA10
        current_ma5 = data_row["MA5"]
        current_ma10 = data_row["MA10"]
        # 需要获取前一天的数据
        prev_data = past_data.iloc[-1]
        prev_ma5 = prev_data["MA5"]
        prev_ma10 = prev_data["MA10"]

        ma_condition_met = current_ma5 > current_ma10 and prev_ma5 <= prev_ma10

        # 检查过去10天平均成交额
        avg_dollar_vol_10 = past_data["DollarVolume"].mean()
        if pd.isna(avg_dollar_vol_10) or avg_dollar_vol_10 == 0:
            return False

        high_dollar_vol_condition_met = avg_dollar_vol_10 > 50_000_000

        return ma_condition_met and high_dollar_vol_condition_met

    def format_result(self, ticker, data_row, past_data):
        avg_dollar_vol_10 = past_data["DollarVolume"].mean()
        return {
            "Symbol": ticker,
            "Avg Dollar Volume (10-day)": f"${avg_dollar_vol_10:,.2f}",
        }

    def get_description(self):
        return "MA5上穿MA10且过去10天平均交易额>5000万美元 (按平均交易额排序)"

    def get_sort_column(self):
        return "Avg Dollar Volume (10-day) Num"

    def get_sort_ascending(self):
        return False  # 按成交额降序


class CDSignalStrategy(BaseStrategy):
    """
    策略3: 模拟CD指标抄底信号且过去10天平均交易额>5000万美元
    """

    def __init__(self):
        self.days_needed = 30  # 用于CD分析和价格趋势判断

    def get_required_days(self):
        return self.days_needed

    def get_required_data_columns(self):
        cols = super().get_required_data_columns()
        # 需要用于计算CD、判断价格趋势的数据
        return cols + [
            "DollarVolume",
            "Simulated_Delta",
            "Cumulative_Simulated_Delta",
            "Close",
            "Low",
            "High",
        ]

    def check_condition(self, data_row, past_data):
        if len(past_data) != self.days_needed - 1:
            print(
                f"警告: CDSignalStrategy 预期 past_data 有 {self.days_needed - 1} 行，但收到了 {len(past_data)} 行。"
            )
            return False

        # --- 条件 A: 检查过去10天平均成交额 ---
        past_10_dollar_volumes = past_data["DollarVolume"].iloc[-10:]
        avg_dollar_vol_10 = past_10_dollar_volumes.mean()
        if pd.isna(avg_dollar_vol_10) or avg_dollar_vol_10 <= 100_000_000:
            return False

        # --- 条件 B: 检查模拟CD抄底信号 ---
        # B1: 价格是否处于明确的下降趋势？检查最近5天的收盘价是否都低于15天前的收盘价
        recent_closes = past_data["Close"].iloc[-6:-1]
        price_15_days_ago = past_data["Close"].iloc[-15]
        is_price_down_trend = (recent_closes < price_15_days_ago).all()

        if not is_price_down_trend:
            return False  # 如果价格不在下降趋势，直接返回False

        # B2: 累计模拟Delta是否出现明确的反转迹象？检查最近3天的累计Delta是否形成上升趋势
        recent_cum_delta = past_data["Cumulative_Simulated_Delta"].iloc[
            -3:
        ]  # 最近3天的累计Delta
        # 检查是否是单调递增的 (例如，[a, b, c] 满足 a < b < c)
        is_cum_delta_upward_trend = (
            recent_cum_delta.iloc[0] < recent_cum_delta.iloc[1]
            and recent_cum_delta.iloc[1] < recent_cum_delta.iloc[2]
        )

        # B3: 当日模拟Delta是否为正？ (作为买盘力量的补充证据)
        today_simulated_delta = data_row["Simulated_Delta"]
        is_today_delta_positive = today_simulated_delta > 0

        is_cd_signal = is_cum_delta_upward_trend and is_today_delta_positive

        return is_cd_signal

    def format_result(self, ticker, data_row, past_data):
        past_10_dollar_volumes = past_data["DollarVolume"].iloc[-10:]
        avg_dollar_vol_10 = past_10_dollar_volumes.mean()
        return {
            "Symbol": ticker,
            "Avg Dollar Volume (10-day)": f"${avg_dollar_vol_10:,.2f}",
            "Simulated_CD_Status": "Confirmed Bullish Divergence (Buy Signal)",
        }

    def get_description(self):
        return "模拟CD指标抄底信号且过去10天平均交易额>5000万美元 (按平均交易额排序)"

    def get_sort_column(self):
        return "Avg Dollar Volume (10-day) Num"

    def get_sort_ascending(self):
        return False  # 按成交额降序
