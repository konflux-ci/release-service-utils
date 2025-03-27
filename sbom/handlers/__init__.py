"""
This module contains a function for picking a handler for an SBOM and exports
the handlers.
"""

from typing import Any, Optional

from sbom.handlers.abstract import SBOMHandler
from sbom.handlers.spdx_2_3 import SPDXVersion23


def get_handler(sbom: dict[str, Any]) -> Optional[type[SBOMHandler]]:
    """
    Get SBOM handler class based on the SBOM dict provided.
    """
    if sbom.get("spdxVersion") == "SPDX-2.3":
        return SPDXVersion23

    if sbom.get("bomFormat") == "CycloneDX":
        if sbom.get("specVersion") == "1.6":
            raise NotImplementedError()

    return None
