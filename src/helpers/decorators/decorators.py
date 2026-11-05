"""Utility decorators for common patterns."""

from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import Executor, Future
from functools import wraps
from typing import Any, Callable, Iterator


class ExecutorWrapper:
    """Provide an executor configured during application runtime."""

    def __init__(self, executor: Executor | None = None) -> None:
        """Initialize the wrapper with an optional executor."""
        self._executor = executor

    @contextmanager
    def use(self, executor: Executor) -> Iterator[None]:
        """Temporarily provide ``executor`` to decorated functions."""
        previous_executor = self._executor
        self._executor = executor
        try:
            yield
        finally:
            self._executor = previous_executor

    def get_executor(self) -> Executor:
        """Return the configured executor or raise when none has been provided."""
        if self._executor is None:
            raise RuntimeError("No executor has been configured")
        return self._executor


def async_in_executor(
    executor_wrapper: ExecutorWrapper,
) -> Callable[..., Callable[..., Future[Any]]]:
    """Decorate a function to run asynchronously with ``executor_wrapper``'s executor."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Future[Any]]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Future[Any]:
            return executor_wrapper.get_executor().submit(func, *args, **kwargs)

        return wrapper

    return decorator
