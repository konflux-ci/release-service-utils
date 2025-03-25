from abc import ABC, abstractmethod
from typing import Union, Any

from sbom.sbomlib import Component, Image, IndexImage


class SBOMHandler(ABC):
    @classmethod
    @abstractmethod
    def update_sbom(
        cls, component: Component, image: Union[IndexImage, Image], sbom: dict[str, Any]
    ) -> None:
        raise NotImplementedError()
