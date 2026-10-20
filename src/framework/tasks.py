"""Base classes for task modules."""

from __future__ import annotations

from abc import ABC, abstractmethod
from types import SimpleNamespace
from typing import TYPE_CHECKING

from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

if TYPE_CHECKING:
    from .helpers import Helper


class Task(ABC):
    """Base class for Tekton task implementations.

    Tasks declare their helper dependencies via the `helpers` class attribute.
    This enables dependency tracking for test selection.

    Attributes:
        helpers: Dict mapping helper name to Helper class.
                 Accessed at runtime via self.helpers.<name>.<method>()

    """

    helpers: dict[str, type[Helper]] = {}

    def __init__(self, helpers: dict[str, Helper] | None = None):
        """Initialize task with helper instances.

        Args:
            helpers: Optional dict of instantiated helpers for dependency injection.
                     If None, instantiates from class-level helpers declaration.

        """
        if helpers is None:
            helpers = {name: cls() for name, cls in self.helpers.items()}
        self.helpers = SimpleNamespace(**helpers)

    @abstractmethod
    def run(self) -> None:
        """Execute task logic. Subclasses implement core functionality here."""
        pass


class InternalTask(Task):
    """Base class for internal Tekton tasks.

    Internal tasks write results to Tekton result files and must handle all
    exceptions gracefully (always exit 0, write failure to result file).

    Attributes:
        results: List of Tekton result env var names (e.g., ["RESULT_RESULT", ...]).
                 First result is the main success/failure indicator.

    """

    results: list[str] = []

    def __init__(self, helpers: dict[str, Helper] | None = None):
        """Initialize internal task and read result file paths from env."""
        super().__init__(helpers)
        if self.results:
            self.result_paths = tekton.result_paths_from_env(*self.results)
        else:
            self.result_paths = []

    def execute(self) -> int:
        """Execute task with exception handling and result writing.

        Returns:
            Always 0 (success written to result file, failures logged there too).

        """
        try:
            self.run()
            self.write_success()
            return 0
        except Exception as e:
            logger.error("%s failed: %s", self.__class__.__name__, e)
            self.write_failure(e)
            return 0

    def write_success(self) -> None:
        """Write success indicator to main result file.

        Override for custom success result format.
        """
        if self.result_paths:
            self.result_paths[0].write_text("Success\n", encoding="utf-8")

    def write_failure(self, exception: Exception) -> None:
        """Write failure to main result file.

        Override for custom failure result format.

        Args:
            exception: The exception that caused the failure.

        """
        if self.result_paths:
            tekton.write_failure_result(
                self.result_paths[0],
                self.__class__.__name__,
                exception,
            )


class ManagedTask(Task):
    """Base class for managed Tekton tasks.

    Managed tasks can spawn internal tasks via internal requests.
    The `internal_tasks` class attribute declares which internal tasks
    this managed task is allowed to create (validates at runtime).

    Attributes:
        internal_tasks: List of InternalTask classes this task can spawn.

    """

    internal_tasks: list[type[InternalTask]] = []

    def execute(self) -> int:
        """Execute managed task.

        Managed tasks don't catch exceptions - they propagate to caller.

        Returns:
            0 on success, non-zero on failure.

        """
        self.run()
        return 0

    def create_internal_task(self, task_class: type[InternalTask], params: dict) -> str:
        """Create an internal task request.

        Args:
            task_class: Internal task class to spawn.
            params: Parameters for the internal task.

        Returns:
            Internal request name.

        Raises:
            ValueError: If task_class not in declared internal_tasks.

        """
        if task_class not in self.internal_tasks:
            raise ValueError(
                f"{task_class.__name__} not in declared internal_tasks for "
                f"{self.__class__.__name__}"
            )
        # TODO: Call internal_request helper once it's converted to framework
        # For now, raise NotImplementedError
        raise NotImplementedError(
            "create_internal_task requires internal_request helper conversion"
        )

    def fetch_internal_task(self, task_run_name: str) -> dict:
        """Fetch status/results of an internal task request.

        Args:
            task_run_name: Name of the internal task run to fetch.

        Returns:
            Internal task status and results.

        Raises:
            NotImplementedError: Until internal_request helper is converted.

        """
        # TODO: Call internal_request helper once it's converted
        raise NotImplementedError(
            "fetch_internal_task requires internal_request helper conversion"
        )
