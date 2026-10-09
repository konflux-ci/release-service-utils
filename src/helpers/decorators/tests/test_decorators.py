"""Tests for the ``decorators`` helper module."""

from __future__ import annotations

import unittest.mock as mock
from concurrent.futures import Future, ThreadPoolExecutor

from release_service_utils.helpers.decorators import async_in_executor


def test_async_in_executor_returns_future() -> None:
    """Decorated function returns Future when called."""
    executor = ThreadPoolExecutor(max_workers=1)

    @async_in_executor(executor)
    def sample_func(x: int) -> int:
        return x * 2

    result = sample_func(5)

    assert isinstance(result, Future)
    assert result.result() == 10
    executor.shutdown(wait=True)


def test_async_in_executor_submits_to_executor() -> None:
    """Decorated function submits work to the provided executor."""
    mock_executor = mock.Mock(spec=ThreadPoolExecutor)
    mock_future = mock.Mock(spec=Future)
    mock_executor.submit.return_value = mock_future

    @async_in_executor(mock_executor)
    def sample_func(x: int, y: int) -> int:
        return x + y

    result = sample_func(3, 4)

    mock_executor.submit.assert_called_once()
    call_args = mock_executor.submit.call_args
    assert call_args[0][1] == 3
    assert call_args[0][2] == 4
    assert result is mock_future


def test_async_in_executor_passes_args_and_kwargs() -> None:
    """Decorated function passes positional and keyword arguments correctly."""
    executor = ThreadPoolExecutor(max_workers=1)

    @async_in_executor(executor)
    def sample_func(a: int, b: int, c: int = 10) -> int:
        return a + b + c

    result = sample_func(1, 2, c=3)

    assert result.result() == 6
    executor.shutdown(wait=True)


def test_async_in_executor_preserves_function_metadata() -> None:
    """Decorator preserves original function name and docstring via wraps."""

    @async_in_executor(ThreadPoolExecutor(max_workers=1))
    def documented_func() -> str:
        """Sample docstring for testing."""
        return "test"

    assert documented_func.__name__ == "documented_func"
    assert documented_func.__doc__ == "Sample docstring for testing."


def test_async_in_executor_handles_exceptions() -> None:
    """Exceptions raised in decorated function are propagated via Future."""
    executor = ThreadPoolExecutor(max_workers=1)

    @async_in_executor(executor)
    def failing_func() -> None:
        raise ValueError("Test error")

    result = failing_func()

    try:
        result.result()
        assert False, "Expected ValueError to be raised"
    except ValueError as e:
        assert str(e) == "Test error"
    finally:
        executor.shutdown(wait=True)


def test_async_in_executor_with_no_args() -> None:
    """Decorated function works with no arguments."""
    executor = ThreadPoolExecutor(max_workers=1)

    @async_in_executor(executor)
    def no_args_func() -> str:
        return "success"

    result = no_args_func()

    assert result.result() == "success"
    executor.shutdown(wait=True)
