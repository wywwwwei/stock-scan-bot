import time


class ProgressLogger:
    """
    用于股票扫描的轻量进度展示
    """

    def __init__(self, total: int):
        self.total = total
        self.start_time = time.time()
        self.last_log_time = 0

    def log(self, current: int) -> None:
        """
        根据进度决定是否输出日志
        """
        now = time.time()
        elapsed = now - self.start_time

        # 少量股票：每一只都打印
        if self.total < 20:
            print(f"[INFO] 正在获取历史数据 {current}/{self.total}")
            return

        # 大量股票：每 5 秒或 5% 打印一次
        progress = current / self.total
        if now - self.last_log_time < 5 and progress * 100 % 5 != 0:
            return

        self.last_log_time = now

        avg_time = elapsed / current if current else 0
        eta = avg_time * (self.total - current)

        print(
            f"[INFO] 历史数据进度: {current}/{self.total} "
            f"({progress:.1%}), "
            f"ETA {eta:.0f}s"
        )
