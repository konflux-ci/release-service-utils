from io import StringIO
import json
import tempfile
import pytest
from pathlib import Path

import sbomlib


@pytest.mark.parametrize(
    ["auths", "reference", "expected_auths"],
    [
        pytest.param(
            {"registry.local/repo": {"auth": "some_token"}},
            "registry.local/repo@sha256:deadbeef",
            {"registry.local": {"auth": "some_token"}},
            id="simple"
        ),
        pytest.param(
            {"registry.local/org/repo": {"auth": "some_token"}},
            "registry.local/org/repo@sha256:deadbeef",
            {"registry.local": {"auth": "some_token"}},
            id="nested"
        ),
    ],
)
def test_get_oci_auth_file(auths, reference, expected_auths):
    test_config = {"auths": auths}

    with tempfile.NamedTemporaryFile(mode="w") as config:
        json.dump(test_config, config)
        config.flush()

        fp = StringIO()

        assert sbomlib.get_oci_auth_file(
            reference, Path(config.name), fp
        ) is True

        fp.seek(0)

        data = json.loads(fp.read())
        assert data["auths"] == expected_auths
