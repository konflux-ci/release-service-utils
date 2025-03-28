"""
This module contains the abstract class for SBOM handlers.
"""

from abc import ABC, abstractmethod
from typing import Union, Any

from sbom.sbomlib import Component, Image, IndexImage


class SBOMHandler(ABC):  # pylint: disable=too-few-public-methods
    """
    This class enforces an interface for SBOM handlers.
    """

    @classmethod
    @abstractmethod
    def update_sbom(
        cls, component: Component, image: Union[IndexImage, Image], sbom: dict[str, Any]
    ) -> None:
        """
        Update the specified SBOM in-place based on the provided component information.
        """
        raise NotImplementedError()
