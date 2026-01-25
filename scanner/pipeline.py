import pandas as pd
import re
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.progress_logger import ProgressLogger
from scanner.indicators import preprocess_data
from strategy.base import BaseStrategy


class StockScanner:
    """
    股票扫描 Pipeline
    - 阶段 1：按股票策略需求拉取历史数据
    - 阶段 2：并发执行策略（纯计算）
    - 阶段 3：排序
    """

    def __init__(
        self,
        datasource,
        stock_strategy_map: Dict[str, List],
        default_strategies: List,
    ):
        self.datasource = datasource
        self.stock_strategy_map = stock_strategy_map
        self.default_strategies = default_strategies

    def all_possible_strategies(self) -> List:
        seen = set()
        result = []

        for strategy in self.default_strategies:
            if strategy not in seen:
                seen.add(strategy)
                result.append(strategy)

        for strategy_list in self.stock_strategy_map.values():
            for strategy in strategy_list:
                if strategy not in seen:
                    seen.add(strategy)
                    result.append(strategy)

        return result

    def run(
        self,
        stock_symbols: List[str],
        max_workers: int,
    ) -> Tuple[Dict[str, List[dict]], Dict[str, str]]:
        """
        执行并发扫描并返回排序后的结果和策略元数据

        :param stock_symbols: 股票列表
        :param max_workers: 最大并发线程数
        :return: Tuple (扫描结果, 策略元数据)

        """
        histories = self.fetch_histories(stock_symbols)

        # 生成部分策略信息给外部使用
        all_possible_strategies = self.all_possible_strategies()
        strategy_metadata = {
            strategy.get_name(): strategy.get_description()
            for strategy in all_possible_strategies
        }

        aggregated: Dict[str, List[dict]] = {}

        print(f"[INFO] 开始并发扫描，workers={max_workers}")

        def task(ticker: str, hist: pd.DataFrame):
            strategies = self.stock_strategy_map.get(ticker, self.default_strategies)
            return self._scan_single_stock(ticker, hist, strategies)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(task, t, h): t for t, h in histories.items()}

            for future in as_completed(futures):
                try:
                    stock_results = future.result()
                    for name, rows in stock_results.items():
                        aggregated.setdefault(name, []).extend(rows)
                except Exception as e:
                    print(f"[ERROR] 并发任务异常: {e}")

        print("[INFO] 扫描完成")

        # 处理排序
        sorted_results = self.sort_all_results(aggregated)
        return sorted_results, strategy_metadata

    def fetch_histories(self, tickers: List[str]) -> Dict[str, pd.DataFrame]:
        """
        拉取历史数据

        :param tickers: 股票列表
        :return: { ticker: DataFrame }
        """
        histories: Dict[str, pd.DataFrame] = {}

        total = len(tickers)
        print(f"[INFO] 开始拉取历史数据，共 {total} 只股票")

        if total == 0:
            print("[WARN] 股票列表为空，跳过历史数据拉取")
            return histories

        progress = ProgressLogger(total)

        for idx, ticker in enumerate(tickers, start=1):
            strategies = self.stock_strategy_map.get(ticker, self.default_strategies)

            if not strategies:
                print(f"[WARN] {ticker} 无策略配置，跳过")
                progress.log(idx)
                continue

            # 计算该股票所需的最大历史天数
            max_days = max(s.get_required_days() for s in strategies)
            df = self.datasource.history(ticker, max_days + 10)

            if df.empty:
                print(f"[WARN] {ticker} 历史数据为空，跳过")
                progress.log(idx)
                continue

            histories[ticker] = df
            progress.log(idx)

        print(f"[INFO] 历史数据拉取完成：{len(histories)} / {len(tickers)}")
        return histories

    def _scan_single_stock(
        self,
        ticker: str,
        hist_df: pd.DataFrame,
        strategies: List,
    ) -> Dict[str, List[dict]]:
        """
        单股票策略执行（纯计算）
        """
        results: Dict[str, List[dict]] = {}

        # 汇总所有策略所需的数据列, 根据策略需求对历史数据进行预处理（如 MA / MACD 等）
        required_columns = set()
        for s in strategies:
            required_columns.update(s.get_required_fields())
        hist_df = preprocess_data(hist_df, list(required_columns))

        for strategy in strategies:
            days = strategy.get_required_days()
            if len(hist_df) < days:
                continue

            past = hist_df.iloc[-days:-1]
            today = hist_df.iloc[-1]

            try:
                if strategy.check_condition(today, past):
                    row = strategy.format_result(ticker, today, past)
                    results.setdefault(strategy.get_name(), []).append(row)
                    print(f"发现 {strategy.get_name()} 信号股票: {ticker}")
            except Exception as e:
                print(f"[ERROR] 策略异常 {ticker} {strategy.get_name()}: {e}")

        return results

    def sort_all_results(
        self, raw_results: Dict[str, List[dict]]
    ) -> Dict[str, pd.DataFrame]:
        """
        对所有策略的扫描结果进行排序。

        :param raw_results: scanner.run() 返回的原始结果
        :return: 排序后的 DataFrame
        """
        sorted_dataframes: Dict[str, pd.DataFrame] = {}

        # 获取所有策略，进行排序
        for strategy_name, results in raw_results.items():
            # 找到该策略对应的策略实例
            strategy = None
            for s in self.all_possible_strategies():
                if s.get_name() == strategy_name:
                    strategy = s
                    break
            if strategy is None:
                print(f"[WARN] 找不到策略: {strategy_name}")
                continue

            # 对每个策略的结果进行排序
            df_sorted = self.sort_results_for_strategy(strategy, results)
            sorted_dataframes[strategy_name] = df_sorted

        return sorted_dataframes

    def sort_results_for_strategy(
        self,
        strategy: BaseStrategy,
        results: List[Dict],
    ) -> pd.DataFrame:
        """
        对单个策略的扫描结果进行排序。

        :param strategy: 策略实例
        :param results: format_result 输出的 dict 列表
        :return: 排序后的 DataFrame
        """
        df = pd.DataFrame(results)

        if df.empty:
            return df

        sort_col = strategy.get_sort_column()

        if sort_col not in df.columns:
            print(f"[WARN] 排序字段 '{sort_col}' 不存在于结果中，跳过排序")
            return df

        # 生成内部排序列，避免污染展示字段
        internal_sort_col = "__sort_value__"

        df[internal_sort_col] = df[sort_col].apply(self._parse_sort_value)

        if df[internal_sort_col].isna().all():
            print(f"[WARN] 排序字段 '{sort_col}' 无法解析为数值，跳过排序")
            return df.drop(columns=[internal_sort_col])

        df = (
            df.sort_values(
                by=internal_sort_col,
                ascending=strategy.is_sort_ascending(),
            )
            .drop(columns=[internal_sort_col])
            .reset_index(drop=True)
        )

        return df

    def _parse_sort_value(self, value):
        """
        将 format_result 中的展示值转换为可排序的数值。

        支持：
        - "$12,345,678.90"
        - "123,456"
        - 纯数字
        """
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            # 去掉 $, , 等符号
            cleaned = re.sub(r"[,$]", "", value)
            try:
                return float(cleaned)
            except ValueError:
                return None

        return None
