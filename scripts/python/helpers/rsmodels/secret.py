"""Secure handling of secrets in logs and debugging."""

from __future__ import annotations

from typing import Optional, Union


class Secret(str):
    """A str subclass that masks its value in logs but works normally in code.

    This class is picklable and can be used with ProcessPoolExecutor.

    Usage:
        # Create a secret
        token = Secret("my-secret-token")

        # Works directly in subprocess (no unveil needed)
        cmd = ["curl", "-H", f"Authorization: Bearer {token}"]
        subprocess.Popen(cmd)  # Uses actual value

        # Masks in debug logs
        logger.debug(f"Command: {cmd}")  # Shows: [..., 'Authorization: Bearer ***SECRET***']
        print(repr(token))  # Shows: ***SECRET***
        print(f"Token: {token!r}")  # Shows: Token: ***SECRET***

        # Access actual value when needed
        actual = str(token)  # or token.unveil()

        # Picklable for multiprocessing
        with ProcessPoolExecutor() as executor:
            executor.submit(some_func, token)  # Works!

    """

    _MASK = "***SECRET***"

    def __new__(cls, value: str, name: Optional[str] = None) -> Secret:
        """Create a new Secret instance."""
        instance = super().__new__(cls, value)
        instance._name = name
        return instance

    def __repr__(self) -> str:
        """Return masked value for repr() and logging."""
        if hasattr(self, "_name") and self._name:
            return f"***SECRET:{self._name}***"
        return self._MASK

    def __str__(self) -> str:
        """Return masked value for repr() and logging."""
        if hasattr(self, "_name") and self._name:
            return f"***SECRET:{self._name}***"
        return self._MASK

    def unveil(self) -> str:
        """Explicitly return the actual secret value.

        This is just an alias for str(self) for clarity.
        """
        return str.__str__(self)

    def __reduce__(self) -> tuple[type[Secret], tuple[str, Optional[str]]]:
        """Support pickling for ProcessPoolExecutor."""
        return (self.__class__, (str.__str__(self), getattr(self, "_name", None)))


def unveil(s: Union[Secret, str]) -> str:
    """Unveil a Secret or return a regular string."""
    if isinstance(s, Secret):
        return s.unveil()
    return s
