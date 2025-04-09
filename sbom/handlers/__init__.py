"""
This module contains a function for picking a handler for an SBOM and exports
the handlers.
"""

from typing import Any, Optional

from sbom.sbomlib import SBOMHandler
from sbom.handlers.spdx2 import SPDXVersion2


def get_handler(sbom: dict[str, Any]) -> Optional[SBOMHandler]:
    """
    Get SBOM handler class based on the SBOM dict provided.
    """
    if sbom.get("spdxVersion") in SPDXVersion2.supported_versions:
        return SPDXVersion2

    if sbom.get("bomFormat") == "CycloneDX":
        raise NotImplementedError()

    return None
