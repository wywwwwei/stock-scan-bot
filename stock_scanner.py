import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import requests
import io
import os
import time
import yfinance as yf
from strategy import *
from rate_limiter import *
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 配置区域 ---

# 1. 邮件发送配置
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = os.getenv("EMAIL_NAME")
SENDER_PASSWORD = os.getenv("EMAIL_PASSWORD")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")
if not SENDER_EMAIL:
    raise ValueError("环境变量 'EMAIL_NAME' 未设置")
if not SENDER_PASSWORD:
    raise ValueError("环境变量 'EMAIL_PASSWORD' 未设置")
if not RECIPIENT_EMAIL:
    raise ValueError("环境变量 'RECIPIENT_EMAIL' 未设置")

# 2. 定义默认要执行的策略
EXECUTE_STRATEGIES = [
    VolumeSurgeStrategy(),
    MACrossStrategy(),
    CDSignalStrategy(),
]

# 3. 目标股票列表。留空([])则扫描所有股票。
TARGET_STOCKS = [
    # "AAPL", "MSFT", "GOOGL" # 示例：只扫描这几只股票
    # 如果列表为空，则扫描所有股票
]

# 4. 为特定股票指定策略。key为股票代码，value为策略实例列表。
# 如果股票不在这个映射中，则使用 EXECUTE_STRATEGIES
STOCK_STRATEGY_MAP = {
    # "AAPL": [VolumeSurgeStrategy()], # AAPL只扫描成交量
    # "MSFT": [MACrossStrategy(), CDSignalStrategy()], # MSFT扫描均线和CD
    # "ZYME": [CDSignalStrategy()], # ZYME只扫描CD
}

# --- 配置区域结束 ---

# ===== yfinance 全局限流 =====
YF_RATE_LIMITER = RateLimiter(max_calls=10, period=1.0)


def safe_history(ticker, period):
    """
    安全地获取股票历史行情数据（统一入口）

    所有对 yf.Ticker().history 的调用必须通过此函数，
    以确保全局限流策略生效，避免触发 Yahoo Finance 的反爬机制。

    :param ticker: 股票代码（如 'AAPL'）
    :param period: yfinance 支持的时间区间字符串（如 '30d'）
    :return: 包含历史行情的 DataFrame；失败时返回空 DataFrame
    """
    try:
        YF_RATE_LIMITER.acquire()
        return yf.Ticker(ticker).history(period=period)
    except Exception as e:
        print(f"[yfinance异常] {ticker}: {e}")
        return pd.DataFrame()


def get_all_required_data_columns(strategies):
    """汇总所有策略需要的列"""
    all_cols = set(["Close", "Volume"])
    for strategy in strategies:
        all_cols.update(strategy.get_required_data_columns())
    return list(all_cols)


def preprocess_data(hist_data, all_required_columns):
    """
    根据策略要求预处理数据，例如计算均线、成交额等。
    """
    if "DollarVolume" in all_required_columns:
        hist_data["DollarVolume"] = hist_data["Close"] * hist_data["Volume"]

    if "MA5" in all_required_columns:
        hist_data["MA5"] = hist_data["Close"].rolling(window=5).mean()

    if "MA10" in all_required_columns:
        hist_data["MA10"] = hist_data["Close"].rolling(window=10).mean()

    if "Simulated_Delta" in all_required_columns:
        hist_data["Price_Change"] = hist_data["Close"].diff()
        hist_data["Simulated_Delta"] = hist_data["Price_Change"] * hist_data["Volume"]
        hist_data["Cumulative_Simulated_Delta"] = hist_data["Simulated_Delta"].cumsum()

    # MACD 计算 (12, 26, 9)
    if any(
        col in all_required_columns
        for col in ["MACD_DIF", "MACD_DEA", "MACD_Histogram"]
    ):
        exp1 = hist_data["Close"].ewm(span=12, adjust=False).mean()
        exp2 = hist_data["Close"].ewm(span=26, adjust=False).mean()
        hist_data["MACD_DIF"] = exp1 - exp2
        hist_data["MACD_DEA"] = hist_data["MACD_DIF"].ewm(span=9, adjust=False).mean()
        hist_data["MACD_Histogram"] = hist_data["MACD_DIF"] - hist_data["MACD_DEA"]

    return hist_data


def fetch_all_histories(
    stock_list: list, stock_strategy_map: dict, default_strategies: list
) -> dict:
    """
    阶段 1：按股票所需策略拉取历史行情数据（受控、低频）

    :param stock_list: 待扫描的股票代码列表
    :param stock_strategy_map: { ticker: [策略实例列表] }
    :param default_strategies: 默认策略列表（EXECUTE_STRATEGIES）
    :return: { ticker: 历史行情 DataFrame }
    """
    print("阶段 1：开始拉取历史数据（限流）...")

    all_histories = {}

    for i, ticker in enumerate(stock_list, 1):
        # 计算该股票所需的最大历史天数
        strategies = stock_strategy_map.get(ticker, default_strategies)
        max_days_needed = max(strategy.get_required_days() for strategy in strategies)

        hist = safe_history(ticker, period=f"{max_days_needed + 10}d")
        if hist is not None and not hist.empty:
            all_histories[ticker] = hist
        else:
            print(f"[跳过] {ticker} 无历史数据")

    print(f"历史数据拉取完成，共 {len(all_histories)} 只股票")
    return all_histories


def scan_single_stock_from_df(ticker, hist_data_raw, strategies):
    """
    阶段 2：基于已获取的历史数据执行策略扫描（纯计算）

    ⚠️ 本函数不允许调用任何 yfinance 接口

    :param ticker: 股票代码
    :param hist_data_raw: 阶段 1 拉取的原始历史行情数据
    :param strategies: 需要执行的策略实例列表
    :return: { strategy_name: [格式化后的结果 dict, ...] }
    """

    # 汇总所有策略所需的数据列, 根据策略需求对历史数据进行预处理（如 MA / MACD 等）
    all_required_cols = get_all_required_data_columns(strategies)
    hist_data_processed = preprocess_data(hist_data_raw.copy(), all_required_cols)

    results = {}

    for strategy in strategies:
        days_needed = strategy.get_required_days()

        if len(hist_data_processed) < days_needed:
            continue

        strategy_past_data = hist_data_processed.iloc[-days_needed:-1]
        strategy_data_row = hist_data_processed.iloc[-1]

        try:
            if strategy.check_condition(strategy_data_row, strategy_past_data):
                formatted_result = strategy.format_result(
                    ticker, strategy_data_row, strategy_past_data
                )
                results.setdefault(strategy.get_name(), []).append(formatted_result)
                print(f"发现 {strategy.get_name()} 信号股票: {ticker}")
        except Exception as e:
            print(f"[策略异常] {ticker} {strategy.get_name()}: {e}")

    return results


def scan_stocks_stage2_concurrent(
    all_histories: dict,
    stock_strategy_map: dict,
    default_strategies: list,
    max_workers: int = 10,
) -> dict:
    """
    阶段 2：并发执行策略扫描

    :param all_histories: { ticker: 历史行情 DataFrame }
    :param stock_strategy_map: { ticker: [策略实例列表] }
    :param default_strategies: EXECUTE_STRATEGIES
    :param max_workers: 并发线程数
    :return: { strategy_name: 排序后的 DataFrame }
    """

    print("阶段 2：开始并发执行策略扫描...")

    all_possible_strategies = set(EXECUTE_STRATEGIES)
    for strategy_list in stock_strategy_map.values():
        all_possible_strategies.update(strategy_list)

    all_results = {strategy.get_name(): [] for strategy in all_possible_strategies}

    def task(ticker, hist):
        """
        单股票扫描任务（供线程池调用）
        """
        strategies = stock_strategy_map.get(ticker, default_strategies)
        return scan_single_stock_from_df(ticker, hist, strategies)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(task, ticker, hist): ticker
            for ticker, hist in all_histories.items()
        }

        for future in as_completed(futures):
            try:
                stock_results = future.result()
                for strategy_name, results_list in stock_results.items():
                    all_results[strategy_name].extend(results_list)
            except Exception as e:
                print(f"[并发异常] {e}")

    # 排序
    dataframes = {}
    for strategy in all_possible_strategies:
        class_name = strategy.get_name()
        df = pd.DataFrame(all_results[class_name])
        if not df.empty:
            sort_col_display = strategy.get_sort_column().replace(" Num", "")
            if sort_col_display in df.columns:
                df[strategy.get_sort_column()] = (
                    df[sort_col_display]
                    .str.replace("$", "")
                    .str.replace(",", "")
                    .astype(float)
                )
                df = (
                    df.sort_values(
                        by=strategy.get_sort_column(),
                        ascending=strategy.get_sort_ascending(),
                    )
                    .drop(columns=[strategy.get_sort_column()])
                    .reset_index(drop=True)
                )
        dataframes[class_name] = df

    return dataframes


def send_email(dataframes_by_strategy):
    """
    将多种策略的结果数据框发送到指定邮箱。
    """
    html_content = (
        f"<h2>美股盘后扫描结果 - {pd.Timestamp.now().strftime('%Y-%m-%d')}</h2>"
    )

    for strategy_instance in EXECUTE_STRATEGIES:
        class_name = strategy_instance.get_name()
        df = dataframes_by_strategy.get(class_name, pd.DataFrame())

        if df.empty:
            html_content += f"<h3>{strategy_instance.get_description()}</h3><p>今日没有找到符合条件的股票。</p>"
        else:
            html_content += f"<h3>{strategy_instance.get_description()}</h3>"
            html_table = df.to_html(
                index=False, table_id=f"results_{class_name.lower()}"
            )
            html_content += html_table

    message = MIMEMultipart("alternative")
    message["Subject"] = f"美股盘后扫描结果 - {pd.Timestamp.now().strftime('%Y-%m-%d')}"
    message["From"] = SENDER_EMAIL
    message["To"] = RECIPIENT_EMAIL

    part = MIMEText(html_content, "html")
    message.attach(part)

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, message.as_string())
        print("邮件发送成功！")
    except Exception as e:
        print(f"邮件发送失败: {e}")


def get_nasdaq_symbols():
    """
    从NASDAQ官网下载并解析nasdaqlisted.txt文件，获取股票代码列表。
    """
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text), sep="|")

        # 过滤 Symbol 列为 nan 的行
        df_cleaned = df.dropna(subset=["Symbol"])
        # 过滤 Symbol 列为空字符串或只包含空白字符的行
        df_cleaned = df_cleaned[df_cleaned["Symbol"].str.strip().ne("")]

        symbols = df_cleaned["Symbol"].tolist()
        print(f"成功获取纳斯达克上市股票列表，共 {len(symbols)} 只。")
        return symbols
    except requests.exceptions.RequestException as e:
        print(f"下载股票列表失败: {e}")
        return []
    except pd.errors.EmptyDataError:
        print("下载的文件为空或格式不正确。")
        return []
    except Exception as e:
        print(f"解析股票列表时发生错误: {e}")
        return []


def main():
    """
    主函数
    """

    print("正在下载纳斯达克股票代码列表...")
    stock_symbols = get_nasdaq_symbols()

    if not stock_symbols:
        print("无法获取股票列表，脚本终止。")
        return

    if TARGET_STOCKS:
        # 如果 TARGET_STOCKS 不为空，则只扫描列表中的股票
        print(f"使用目标股票列表，共 {len(TARGET_STOCKS)} 只。")
        # 过滤掉不在总列表中的股票代码
        valid_target_stocks = [sym for sym in TARGET_STOCKS if sym in stock_symbols]
        print(f"过滤后，有效目标股票 {len(valid_target_stocks)} 只。")
        stock_symbols_to_scan = valid_target_stocks
    else:
        # 如果 TARGET_STOCKS 为空，则扫描所有股票
        print(f"TARGET_STOCKS 为空，将扫描所有 {len(stock_symbols)} 只股票。")
        stock_symbols_to_scan = stock_symbols

    print(f"开始扫描 {len(stock_symbols_to_scan)} 只纳斯达克股票...")

    # 阶段 1：拉数据
    all_histories = fetch_all_histories(
        stock_symbols_to_scan, STOCK_STRATEGY_MAP, EXECUTE_STRATEGIES
    )

    # 阶段 2：并发算策略
    results_dataframes = scan_stocks_stage2_concurrent(
        all_histories, STOCK_STRATEGY_MAP, EXECUTE_STRATEGIES, max_workers=10
    )

    print("扫描完成，正在发送邮件...")
    send_email(results_dataframes)
    print("脚本执行完毕。")


if __name__ == "__main__":
    main()
