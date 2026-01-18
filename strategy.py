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
    策略3: MACD金叉抄底信号且过去10天平均交易额>1000万美元
    宽松版 CD 抄底策略：
    1. 水下金叉：DIF 上穿 DEA，且 DEA < 0（允许 DIF 已接近或略上 0 轴）
    2. MACD 柱：最近三根柱子向上（缩短），前两根必须为绿柱，当前柱可翻红
    3. 底背离：第二个低点价格不高于前低 5% 以上，且第二个低点 DIF 不低于前低 DIF
    """

    def __init__(self):
        # MACD 计算通常需要较多天数，例如 34 天 (MAX(12, 26) + 9 - 1) 以上的数据来稳定计算
        # 为了确保金叉、背离、柱缩短都能有效判断，我们使用较长的天数
        self.days_needed = 50

    def get_required_days(self):
        return self.days_needed

    def get_required_data_columns(self):
        cols = super().get_required_data_columns()
        # 需要 Close 价格来计算 MACD
        # 需要 DollarVolume 用于交易额条件
        # 需要 Low 价格用于底背离判断
        return cols + [
            "DollarVolume",
            "MACD_DIF",
            "MACD_DEA",
            "MACD_Histogram",
            "Close",
            "Low",
        ]

    def check_condition(self, data_row, past_data):
        if len(past_data) != self.days_needed - 1:
            print(
                f"警告: CDSignalStrategy 预期 past_data 有 {self.days_needed - 1} 行，但收到了 {len(past_data)} 行。"
            )
            return False

        # --- 条件 A: 检查过去10天平均成交额 ---
        past_10_dollar_volumes = past_data['DollarVolume'].iloc[-10:]
        avg_dollar_vol_10 = past_10_dollar_volumes.mean()
        if pd.isna(avg_dollar_vol_10) or avg_dollar_vol_10 <= 10_000_000:
            return False

        # --- 条件 B: 检查 MACD 金叉抄底信号 ---
        # -------- 条件 1：水下金叉（宽松版）--------
        current_diff = data_row["MACD_DIF"]
        current_dea = data_row["MACD_DEA"]
        prev_diff = past_data["MACD_DIF"].iloc[-1]
        prev_dea = past_data["MACD_DEA"].iloc[-1]

        # 宽松金叉：前一日 DIF < DEA，今天 DIF >= DEA
        golden_cross = (prev_diff < prev_dea) and (current_diff >= current_dea)

        # 宽松水下：只要求 DEA < 0（信号线仍在 0 轴下）
        underwater = current_dea < 0

        if not (golden_cross and underwater):
            return False

        # -------- 条件 2：MACD 柱缩短（宽松版）--------
        histogram_current = data_row["MACD_Histogram"]
        histogram_prev1 = past_data["MACD_Histogram"].iloc[-1]  # T-1
        histogram_prev2 = past_data["MACD_Histogram"].iloc[-2]  # T-2

        # 宽松要求：
        # - T-2、T-1 为绿柱（负数）
        # - 柱值单调递增：T-2 < T-1 < T
        #   -> 说明柱子在连续缩短，当前柱可以仍为负，也可以翻红
        shrink_histogram = (
            histogram_prev2 < 0
            and histogram_prev1 < 0
            and histogram_prev2 < histogram_prev1  # 例如 -0.8 < -0.5（在缩短）
            and histogram_prev1 < histogram_current  # 今天继续变大（接近0或翻红）
        )

        if not shrink_histogram:
            return False  # 如果柱子不符合缩短条件，返回False

        # -------- 条件 3：价格底背离（宽松版）--------
        # 更宽松的参数设置
        MAX_LOOKBACK = 30  # 背离检测的最大回看天数（不含今天）
        RECENT_BARS_FOR_SECOND_LOW = 7  # 第二低点在最近多少根 K 线中寻找
        PRICE_TOLERANCE = 0.05  # 价格容差：第二低点不高于前低 5% 以上

        lookback_past = min(MAX_LOOKBACK, len(past_data))

        # 最近 lookback_past 根历史 + 今天
        window_past = past_data.iloc[-lookback_past:].copy()
        current_df = pd.DataFrame([data_row])
        window_full = pd.concat([window_past, current_df], axis=0, sort=False)

        total_len = len(window_full)
        # 近期窗口至少 1 根，前段也至少 1 根
        recent_n = min(RECENT_BARS_FOR_SECOND_LOW, total_len - 1)
        split_idx = total_len - recent_n
        prev_window = window_full.iloc[:split_idx]  # 用于找第一个低点
        second_window = window_full.iloc[split_idx:]  # 用于找第二个低点（含今天）

        # 找两个价格低点
        prev_price_low = prev_window["Low"].min()
        prev_price_low_idx = prev_window["Low"].idxmin()

        second_price_low = second_window["Low"].min()
        second_price_low_idx = second_window["Low"].idxmin()

        # 确保第二个低点在第一个低点之后（按位置判断）
        prev_pos = window_full.index.get_loc(prev_price_low_idx)
        second_pos = window_full.index.get_loc(second_price_low_idx)
        if second_pos <= prev_pos:
            return False

        # 宽松价格条件：第二低点价格 <= 前低 * (1 + 5%)
        # 允许二次探底或略高一点的高低点（Higher Low）
        is_price_retest_or_higher_low = second_price_low <= prev_price_low * (
            1 + PRICE_TOLERANCE
        )
        if not is_price_retest_or_higher_low:
            return False

        # 对应两个低点的 DIF
        prev_dif_at_low = window_full.loc[prev_price_low_idx, "MACD_DIF"]
        second_dif_at_low = window_full.loc[second_price_low_idx, "MACD_DIF"]

        # 宽松底背离：第二低点 DIF 不低于前低 DIF（>= 而不是 >）
        return second_dif_at_low >= prev_dif_at_low

    def format_result(self, ticker, data_row, past_data):
        past_10_dollar_volumes = past_data["DollarVolume"].iloc[-10:]
        avg_dollar_vol_10 = past_10_dollar_volumes.mean()
        current_dollar_volume = data_row['DollarVolume']
        return {
            "Symbol": ticker,
            'Current Dollar Volume': f"${current_dollar_volume:,.2f}",
            "Avg Dollar Volume (10-day)": f"${avg_dollar_vol_10:,.2f}",
        }

    def get_description(self):
        return "MACD金叉抄底信号且过去10天平均交易额>1000万美元 (按平均交易额排序)"

    def get_sort_column(self):
        return "Avg Dollar Volume (10-day) Num"

    def get_sort_ascending(self):
        return False  # 按成交额降序
