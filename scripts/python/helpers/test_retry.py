"""Tests for the ``retry`` helper module."""

from __future__ import annotations

import pytest
import retry


def test_retry_with_exponential_backoff_succeeds_after_retries() -> None:
    """Matching failures sleep with exponential delays until success."""
    calls = {"n": 0}
    sleeps: list[float] = []

    def _op() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("retry me")
        return "ok"

    out = retry.retry_with_exponential_backoff(
        _op,
        max_attempts=5,
        retry_on=ValueError,
        base_sleep_seconds=5,
        sleep_fn=sleeps.append,
    )
    assert out == "ok"
    assert calls["n"] == 3
    assert sleeps == [5, 10]


def test_retry_with_exponential_backoff_raises_after_last_attempt() -> None:
    """When all attempts fail with a retryable error, the last error is raised."""
    sleeps: list[float] = []

    def _op() -> None:
        raise ValueError("still failing")

    with pytest.raises(ValueError, match="still failing"):
        retry.retry_with_exponential_backoff(
            _op,
            max_attempts=3,
            retry_on=ValueError,
            base_sleep_seconds=5,
            sleep_fn=sleeps.append,
        )
    assert sleeps == [5, 10]


def test_retry_with_exponential_backoff_does_not_retry_other_exceptions() -> None:
    """Exceptions outside ``retry_on`` are raised immediately without sleeping."""
    sleeps: list[float] = []

    def _op() -> None:
        raise TypeError("no retry")

    with pytest.raises(TypeError, match="no retry"):
        retry.retry_with_exponential_backoff(
            _op,
            max_attempts=5,
            retry_on=ValueError,
            sleep_fn=sleeps.append,
        )
    assert sleeps == []


def test_retry_with_exponential_backoff_rejects_zero_attempts() -> None:
    """``max_attempts`` must be at least 1."""
    with pytest.raises(ValueError, match=">= 1"):
        retry.retry_with_exponential_backoff(lambda: None, max_attempts=0)
