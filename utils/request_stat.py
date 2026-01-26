from typing import Dict


class RequestStats:
    """
    简单的请求统计器（无锁版）

    适用于：
    - 单进程
    - 轻量并发
    - 调试 / 日志用途
    """

    def __init__(self) -> None:
        self.total_calls: int = 0
        self.failed_calls: int = 0

        self.total_wait_time: float = 0.0
        self.total_request_time: float = 0.0
        self.total_time: float = 0.0

        self.max_wait_time: float = 0.0
        self.max_request_time: float = 0.0
        self.max_total_time: float = 0.0

    def record(
        self,
        success: bool,
        wait_time: float,
        request_time: float,
        total_time: float,
    ) -> None:
        self.total_calls += 1

        if not success:
            self.failed_calls += 1

        self.total_wait_time += wait_time
        self.total_request_time += request_time
        self.total_time += total_time

        if wait_time > self.max_wait_time:
            self.max_wait_time = wait_time
        if request_time > self.max_request_time:
            self.max_request_time = request_time
        if total_time > self.max_total_time:
            self.max_total_time = total_time

    def summary(self) -> Dict[str, float]:
        """
        返回统计摘要
        """
        if self.total_calls == 0:
            return {}

        return {
            "total_calls": self.total_calls,
            "failed_calls": self.failed_calls,
            "failure_rate": self.failed_calls / self.total_calls,
            "avg_wait_ms": (self.total_wait_time / self.total_calls) * 1000,
            "avg_request_ms": (self.total_request_time / self.total_calls) * 1000,
            "avg_total_ms": (self.total_time / self.total_calls) * 1000,
            "max_wait_ms": self.max_wait_time * 1000,
            "max_request_ms": self.max_request_time * 1000,
            "max_total_ms": self.max_total_time * 1000,
        }
