"""Framework base classes for helpers and tasks."""

from __future__ import annotations

from .helpers import BinaryHelper, Helper
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .tasks import InternalTask, ManagedTask, Task

__all__ = [
    "Helper",
    "BinaryHelper",
    "Task",
    "InternalTask",
    "ManagedTask",
]


def __getattr__(name: str) -> type[Task] | type[InternalTask] | type[ManagedTask]:
    """Load task classes only when callers request them.

    Helpers import :class:`Helper` from this package. Deferring the task-module
    import prevents helper imports from recursively loading helpers used by the
    task framework.
    """
    if name in {"Task", "InternalTask", "ManagedTask"}:
        from . import tasks

        return getattr(tasks, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
