import json
from typing import IO
from pathlib import Path


def get_oci_auth_file(reference: str, auth: Path, fp: IO) -> bool:
    """
    Gets path to a temporary file containing the docker config JSON for <reference>.
    Returns True if a token was found, False otherwise.

    Args:
        reference (str): Reference to an image in the form registry/repo@sha256-deadbeef
        auth (Path): Existing docker config.json
        fp (IO): File object to write the new auth file to
    """
    if not auth.is_file():
        raise ValueError(f"No docker config file at {auth}")

    # Remove digest (e.g. @sha256:...)
    ref = reference.split("@", 1)[0]

    # Registry is up to the first slash
    registry = ref.split("/", 1)[0]

    with open(auth, "r") as f:
        config = json.load(f)
    auths = config.get("auths", {})

    current_ref = ref

    while True:
        token = auths.get(current_ref)
        if token is not None:
            json.dump({"auths": {registry: token}}, fp)
            return True

        if "/" not in current_ref:
            break
        current_ref = current_ref.rsplit("/", 1)[0]

    json.dump({"auths": {}}, fp)
    return False
