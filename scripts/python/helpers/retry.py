"""Retry helpers with exponential backoff."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry_with_exponential_backoff(
    operation: Callable[[], T],
    *,
    max_attempts: int,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] = Exception,
    base_sleep_seconds: int = 5,
    sleep_fn: Callable[[float], None] | None = None,
) -> T:
    """
    Run ``operation`` up to ``max_attempts`` times, retrying selected failures.

    On each retry, waits ``base_sleep_seconds * 2 ** (attempt - 1)`` seconds (5,
    10, 20, 40, ... by default). If a failure is not in ``retry_on``, it is
    raised immediately. If retries are exhausted, the last matching exception is
    raised.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    sleeper = time.sleep if sleep_fn is None else sleep_fn

    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except retry_on:
            if attempt >= max_attempts:
                raise
            sleeper(base_sleep_seconds * (2 ** (attempt - 1)))

    raise RuntimeError("unreachable")
