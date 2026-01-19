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

# 2. 定义要执行的策略
EXECUTE_STRATEGIES = [
    VolumeSurgeStrategy(),
    MACrossStrategy(),
    CDSignalStrategy(),
]

# --- 配置区域结束 ---


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
    if any(col in all_required_columns for col in ['MACD_DIF', 'MACD_DEA', 'MACD_Histogram']):
        exp1 = hist_data['Close'].ewm(span=12, adjust=False).mean()
        exp2 = hist_data['Close'].ewm(span=26, adjust=False).mean()
        hist_data['MACD_DIF'] = exp1 - exp2
        hist_data['MACD_DEA'] = hist_data['MACD_DIF'].ewm(span=9, adjust=False).mean()
        hist_data['MACD_Histogram'] = hist_data['MACD_DIF'] - hist_data['MACD_DEA']

    return hist_data


def scan_single_stock(ticker, strategies):
    """
    对单个股票执行所有策略扫描。
    """

    stock = yf.Ticker(ticker)
    # 获取所有策略需要的最大天数的历史数据
    max_days_needed = max(strategy.get_required_days() for strategy in strategies)
    hist_data_raw = stock.history(period=f"{max_days_needed + 10}d")

    if hist_data_raw is None or hist_data_raw.empty:
        print(f"警告: {ticker} 的原始历史数据为空或获取失败，跳过。")
        return {}

    all_required_cols = get_all_required_data_columns(strategies)
    hist_data_processed = preprocess_data(hist_data_raw.copy(), all_required_cols)

    if hist_data_processed is None or hist_data_processed.empty:
        print(f"警告: {ticker} 的预处理数据为空，跳过。")
        return {}

    results = {}
    for strategy in strategies:
        days_needed = strategy.get_required_days()

        if len(hist_data_processed) < days_needed:
            # print(f"警告: {ticker} 的数据不足以执行 {strategy.get_name()} 策略 (需要{days_needed}天, 有{len(hist_data_processed)}天)。")
            # 如果数据不够，继续下一个策略，而不是跳过整个股票
            continue

        strategy_past_data = hist_data_processed.iloc[-days_needed:-1]
        strategy_data_row = hist_data_processed.iloc[-1]

        try:
            if strategy.check_condition(strategy_data_row, strategy_past_data):
                formatted_result = strategy.format_result(
                    ticker, strategy_data_row, strategy_past_data
                )
                if strategy.get_name() not in results:
                    results[strategy.get_name()] = []
                results[strategy.get_name()].append(formatted_result)
                print(f"发现 {strategy.get_name()} 信号股票: {ticker}")
        except Exception as e:
            print(f"执行 {strategy.get_name()} 策略时，处理股票 {ticker} 出错: {e}")

    time.sleep(0.2)

    return results


def scan_stocks(stock_list, strategies):
    """
    遍历股票列表，执行所有策略扫描。
    """
    all_results = {strategy.get_name(): [] for strategy in strategies}

    for ticker in stock_list:
        try:
            stock_results = scan_single_stock(ticker, strategies)
            for strategy_class, results_list in stock_results.items():
                all_results[strategy_class].extend(results_list)
        except Exception as e:
            print(f"处理股票 {ticker} 时出错: {e}")

    # 排序
    dataframes = {}
    for strategy in strategies:
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
        df_cleaned = df.dropna(subset=['Symbol'])
        # 过滤 Symbol 列为空字符串或只包含空白字符的行
        df_cleaned = df_cleaned[df_cleaned['Symbol'].str.strip().ne('')]

        symbols = df_cleaned['Symbol'].tolist()
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

    print(f"开始扫描 {len(stock_symbols)} 只纳斯达克股票...")
    results_dataframes = scan_stocks(stock_symbols, EXECUTE_STRATEGIES)

    print("扫描完成，正在发送邮件...")
    send_email(results_dataframes)
    print("脚本执行完毕。")


if __name__ == "__main__":
    main()
