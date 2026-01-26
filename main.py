from scanner.config.scan import *
from scanner.config.mail import *
from scanner.stock_universe import resolve_stock_universe
from scanner.datasource import YahooFinanceDataSource
from scanner.pipeline import StockScanner
from scanner.notifier import EmailNotifier
from scanner.formatter import format_results_text, format_results_for_email


def main() -> None:
    datasource = YahooFinanceDataSource(YF_MAX_CALLS_PER_SEC)
    stock_symbols = resolve_stock_universe(TARGET_STOCKS, datasource, PREFILTERS)
    if not stock_symbols:
        print("[ERROR] 股票池为空，程序终止")
        return

    datasource = YahooFinanceDataSource(YF_MAX_CALLS_PER_SEC)

    scanner = StockScanner(
        datasource=datasource,
        stock_strategy_map=STOCK_STRATEGY_MAP,
        default_strategies=EXECUTE_STRATEGIES,
    )

    results, strategy_metadata = scanner.run(stock_symbols, SCAN_MAX_WORKERS)

    # ===== 耗时统计 =====
    stats = datasource.stats.summary()
    if stats:
        print(
            f"Total Calls      : {stats['total_calls']}\n"
            f"Failed Calls    : {stats['failed_calls']} "
            f"({stats['failure_rate'] * 100:.1f}%)\n"
            f"Avg Wait Time   : {stats['avg_wait_ms']:.1f} ms\n"
            f"Avg Request Time: {stats['avg_request_ms']:.1f} ms\n"
            f"Avg Total Time  : {stats['avg_total_ms']:.1f} ms\n"
            f"Max Wait Time   : {stats['max_wait_ms']:.1f} ms\n"
            f"Max Request Time: {stats['max_request_ms']:.1f} ms\n"
            f"Max Total Time  : {stats['max_total_ms']:.1f} ms"
        )

    # ===== 控制台输出 =====
    text = format_results_text(results, strategy_metadata)
    print(text)

    # ===== 邮件通知 =====
    mail_text = format_results_for_email(results, strategy_metadata)
    if EMAIL_ENABLED:
        if not SENDER_EMAIL or not SENDER_PASSWORD:
            print("[WARN] 邮件未配置完整，跳过发送")
            return

        notifier = EmailNotifier(
            SMTP_SERVER,
            SMTP_PORT,
            SENDER_EMAIL,
            SENDER_PASSWORD,
            RECIPIENT_EMAIL,
        )

        notifier.send("📈 股票扫描结果", mail_text, "html")


if __name__ == "__main__":
    main()
