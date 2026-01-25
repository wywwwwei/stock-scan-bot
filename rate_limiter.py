import threading
import time


class RateLimiter:
    """
    线程安全的速率限制器（令牌桶的简化实现）

    用于限制在指定时间窗口内的最大调用次数，防止
    yfinance / Yahoo Finance 接口被高频访问触发反爬。
    """

    def __init__(self, max_calls: int, period: float):
        """
        :param max_calls: 在一个 period 时间窗口内允许的最大调用次数
        :param period: 时间窗口长度（秒）
        """
        self.max_calls = max_calls
        self.period = period
        self.lock = threading.Lock()
        self.calls = []

    def acquire(self):
        """
        获取一次调用许可。

        如果在当前时间窗口内调用次数已达上限，
        则阻塞当前线程直到可以安全调用。
        """
        with self.lock:
            now = time.time()
            self.calls = [t for t in self.calls if now - t < self.period]

            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)

            self.calls.append(time.time())
