from abc import ABC, abstractmethod
from typing import Union, Any, Optional

from sbom.handlers.abstract import SBOMHandler
from sbom.handlers.spdx_2_3 import SPDX_2_3
from sbom.sbomlib import Component, Image, IndexImage


def get_handler(sbom: dict[str, Any]) -> Optional[type[SBOMHandler]]:
    if sbom.get("spdxVersion") == "SPDX-2.3":
        return SPDX_2_3
    elif sbom.get("bomFormat") == "CycloneDX":
        # TODO: implement
        pass
