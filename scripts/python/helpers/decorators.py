"""Utility decorators for common patterns."""

from __future__ import annotations

from concurrent.futures import Executor, Future
from functools import wraps
from typing import Any, Callable


def async_in_executor(
    executor_instance: Executor,
) -> Callable[..., Callable[..., Future[Any]]]:
    """Decorate a function to run asynchronously in the given executor instance."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Future[Any]]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Future[Any]:
            return executor_instance.submit(func, *args, **kwargs)

        return wrapper

    return decorator
