"""Utility decorators for common patterns."""

from functools import wraps


def async_in_executor(executor_instance):
    """Decorate a function to run asynchronously in the given executor instance."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return executor_instance.submit(func, *args, **kwargs)

        return wrapper

    return decorator
