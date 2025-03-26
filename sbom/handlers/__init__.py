from typing import Any, Optional

from sbom.handlers.abstract import SBOMHandler
from sbom.handlers.spdx_2_3 import SPDX_2_3


def get_handler(sbom: dict[str, Any]) -> Optional[type[SBOMHandler]]:
    if sbom.get("spdxVersion") == "SPDX-2.3":
        return SPDX_2_3
    elif sbom.get("bomFormat") == "CycloneDX":
        if sbom.get("specVersion") == "1.6":
            raise NotImplementedError()
