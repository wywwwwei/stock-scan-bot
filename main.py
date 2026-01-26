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

    # ===== Prefilter 耗时统计 =====
    datasource.stats.print_summary("Prefilter")
    # 重置 stats
    datasource.stats.reset()

    scanner = StockScanner(
        datasource=datasource,
        stock_strategy_map=STOCK_STRATEGY_MAP,
        default_strategies=EXECUTE_STRATEGIES,
    )
    results, strategy_metadata = scanner.run(stock_symbols, SCAN_MAX_WORKERS)

    # ===== Run 耗时统计 =====
    datasource.stats.print_summary("Run")

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
