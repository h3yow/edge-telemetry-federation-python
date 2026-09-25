"""时间来源：用可注入时钟替代真实系统时间，保证乱序/迟到判定可复现。"""

from __future__ import annotations

import time


class Clock:
    """时钟接口，返回当前参考时间（epoch 毫秒）。"""

    def now_ms(self) -> int:
        raise NotImplementedError


class SystemClock(Clock):
    """系统墙上时钟（毫秒）。"""

    def now_ms(self) -> int:
        return int(time.time() * 1000)


class FixedClock(Clock):
    """固定/可手动推进的时钟，供确定性测试使用。"""

    def __init__(self, start_ms: int = 0) -> None:
        self._now = start_ms

    def now_ms(self) -> int:
        return self._now

    def set(self, now_ms: int) -> None:
        self._now = now_ms

    def advance(self, delta_ms: int) -> int:
        self._now += delta_ms
        return self._now
