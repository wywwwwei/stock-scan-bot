import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import requests
import io
import os
import yfinance as yf

# --- 配置区域 ---
# 1. 邮件发送配置
SMTP_SERVER = "smtp.gmail.com"  # Outlook的SMTP服务器地址
SMTP_PORT = 587  # Outlook的STARTTLS端口
SENDER_EMAIL = os.getenv('EMAIL_NAME')
SENDER_PASSWORD = os.getenv('EMAIL_PASSWORD')
RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL')
if not SENDER_EMAIL:
    raise ValueError("环境变量 'EMAIL_NAME' 未设置")
if not SENDER_PASSWORD:
    raise ValueError("环境变量 'EMAIL_PASSWORD' 未设置")
if not RECIPIENT_EMAIL:
    raise ValueError("环境变量 'RECIPIENT_EMAIL' 未设置")

def get_nasdaq_symbols():
    """
    从NASDAQ官网下载并解析nasdaqlisted.txt文件，获取股票代码列表。
    """
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
    try:
        response = requests.get(url)
        response.raise_for_status()  # 如果请求失败，会抛出异常
        # 使用io.StringIO将响应内容包装成文件对象，然后用pandas读取
        df = pd.read_csv(io.StringIO(response.text), sep="|")
        # 'Symbol' 列包含股票代码
        symbols = df["Symbol"].tolist()
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


def get_stock_data(ticker):
    """
    获取指定股票的数据，用于计算成交额和均线。
    """

    # 创建一个Ticker对象
    stock = yf.Ticker(ticker)

    # 获取历史数据，需要足够多的数据来计算MA5, MA10, 过去60天平均额, 过去10天平均额
    # 获取至少61天的数据以计算MA10和过去60天均额，获取额外几天用于MA5上穿判断
    hist_data = stock.history(period="70d")  # 70天足够

    if (
        hist_data.empty or len(hist_data) < 11
    ):  # 至少需要11天数据来计算MA10和前一天的MA5
        print(f"警告: {ticker} 的历史数据不足，跳过。")
        return None, None, None, None, None, None

    # 计算移动平均线
    hist_data["MA5"] = hist_data["Close"].rolling(window=5).mean()
    hist_data["MA10"] = hist_data["Close"].rolling(window=10).mean()

    # 计算每日成交额 (Dollar Volume)
    hist_data["DollarVolume"] = hist_data["Close"] * hist_data["Volume"]

    # --- 条件1: 成交额异动 ---
    # 当日成交额 (最后一天)
    current_dollar_volume = hist_data["DollarVolume"].iloc[-1]
    # 过去60天平均成交额 (不包含今天)
    past_60_dollar_volumes = hist_data["DollarVolume"].iloc[-61:-1]
    if len(past_60_dollar_volumes) < 60:
        print(f"警告: {ticker} 的过去60天交易日数据不足，跳过成交额检查。")
        dollar_vol_condition_met = False
        avg_dollar_vol_60 = None
        ratio = None
    else:
        avg_dollar_vol_60 = past_60_dollar_volumes.mean()
        ratio = current_dollar_volume / avg_dollar_vol_60
        dollar_vol_condition_met = ratio > 2

    # --- 条件2: 均线金叉 + 高额交易 ---
    # 检查MA5是否上穿MA10
    # 当前MA5 > MA10，且前一天MA5 <= MA10
    current_ma5 = hist_data["MA5"].iloc[-1]
    current_ma10 = hist_data["MA10"].iloc[-1]
    prev_ma5 = hist_data["MA5"].iloc[-2]
    prev_ma10 = hist_data["MA10"].iloc[-2]

    ma_condition_met = current_ma5 > current_ma10 and prev_ma5 <= prev_ma10

    # 检查过去10天平均成交额是否大于5000万美元
    past_10_dollar_volumes = hist_data["DollarVolume"].iloc[-11:-1]  # 不包含今天
    if len(past_10_dollar_volumes) < 10:
        print(f"警告: {ticker} 的过去10天交易日数据不足，跳过均线交易额检查。")
        avg_dollar_vol_10 = None
        high_dollar_vol_condition_met = False
    else:
        avg_dollar_vol_10 = past_10_dollar_volumes.mean()
        if pd.isna(avg_dollar_vol_10) or avg_dollar_vol_10 == 0:
            print(
                f"警告: {ticker} 的过去10天平均成交额无效 ({avg_dollar_vol_10})，跳过均线交易额检查。"
            )
            high_dollar_vol_condition_met = False
        else:
            high_dollar_vol_condition_met = avg_dollar_vol_10 > 50_000_000  # 5000万美元

    # 综合均线和交易额条件
    ma_and_dollar_vol_condition_met = ma_condition_met and high_dollar_vol_condition_met

    return (
        dollar_vol_condition_met,
        avg_dollar_vol_60,
        ma_and_dollar_vol_condition_met,
        current_dollar_volume,
        avg_dollar_vol_10,
        ratio,
    )


def scan_stocks(stock_list):
    """
    遍历股票列表，查找符合条件的股票。
    """
    volume_results = []  # 成交额异动股
    ma_results = []  # 均线金叉股

    for ticker in stock_list:
        try:
            vol_met, avg_vol_60, ma_met, current_vol, avg_dollar_vol_10, ratio = (
                get_stock_data(ticker)
            )

            if vol_met and avg_vol_60 is not None and ratio is not None:
                volume_results.append(
                    {
                        "Symbol": ticker,
                        "Current Dollar Volume": f"${current_vol:,.2f}",  # 格式化为带美元符号和千分位的成交额
                        "60-Day Avg Dollar Volume": f"${avg_vol_60:,.2f}",  # 格式化为带美元符号和千分位的成交额
                        "Ratio (Current / 60-Day Avg)": f"{ratio:.2f}",  # 添加比例列
                    }
                )
                print(f"发现成交额异动股票: {ticker}")

            if ma_met and avg_dollar_vol_10 is not None:
                ma_results.append(
                    {
                        "Symbol": ticker,
                        "Avg Dollar Volume (10-day)": f"${avg_dollar_vol_10:,.2f}",
                    }
                )
                print(f"发现均线金叉+高额交易股票: {ticker}")

        except Exception as e:
            # 可能因网络、API限制或股票代码无效导致错误
            print(f"处理股票 {ticker} 时出错: {e}")

    # 转换为DataFrame并排序
    df_volumes = pd.DataFrame(volume_results)
    if not df_volumes.empty:
        # 为了排序，创建一个临时的数值列 (基于当前成交额)
        df_volumes["Current Dollar Volume Num"] = (
            df_volumes["Current Dollar Volume"]
            .str.replace("$", "")
            .str.replace(",", "")
            .astype(float)
        )
        df_volumes = (
            df_volumes.sort_values(by="Current Dollar Volume Num", ascending=False)
            .drop(columns=["Current Dollar Volume Num"])
            .reset_index(drop=True)
        )

    df_mas = pd.DataFrame(ma_results)
    if not df_mas.empty:
        # 按平均交易额从高到低排序
        df_mas["Avg Dollar Volume (10-day) Num"] = (
            df_mas["Avg Dollar Volume (10-day)"]
            .str.replace("$", "")
            .str.replace(",", "")
            .astype(float)
        )
        df_mas = (
            df_mas.sort_values(by="Avg Dollar Volume (10-day) Num", ascending=False)
            .drop(columns=["Avg Dollar Volume (10-day) Num"])
            .reset_index(drop=True)
        )

    return df_volumes, df_mas


def send_email(df_volume_results, df_ma_results):
    """
    将两种结果数据框发送到指定邮箱。
    """
    html_content = (
        f"<h2>美股盘后扫描结果 - {pd.Timestamp.now().strftime('%Y-%m-%d')}</h2>"
    )

    if df_volume_results.empty:
        html_content += "<h3>1. 成交额异动股</h3><p>今日没有找到成交额超过过去60天平均水平2倍的股票。</p>"
    else:
        html_content += "<h3>1. 成交额异动股 (按当日成交额排序)</h3>"
        html_table_volumes = df_volume_results.to_html(
            index=False, table_id="results_volumes"
        )
        html_content += html_table_volumes

    if df_ma_results.empty:
        html_content += "<h3>2. MA5上穿MA10且过去10天平均交易额>5000万美元</h3><p>今日没有找到符合条件的股票。</p>"
    else:
        html_content += (
            "<h3>2. MA5上穿MA10且过去10天平均交易额>5000万美元 (按平均交易额排序)</h3>"
        )
        html_table_mas = df_ma_results.to_html(index=False, table_id="results_mas")
        html_content += html_table_mas

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


def main():
    """
    主函数
    """
    # 从NASDAQ官网获取股票代码列表
    print("正在下载纳斯达克股票代码列表...")
    stock_symbols = get_nasdaq_symbols()

    if not stock_symbols:
        print("无法获取股票列表，脚本终止。")
        return

    print(f"开始扫描 {len(stock_symbols)} 只纳斯达克股票...")
    results_df_volumes, results_df_mas = scan_stocks(stock_symbols)

    print("扫描完成，正在发送邮件...")
    send_email(results_df_volumes, results_df_mas)
    print("脚本执行完毕。")


if __name__ == "__main__":
    main()
