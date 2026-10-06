"""Verify rh_sign_python_wheels signing and attestation logic."""

from __future__ import annotations

import json
import runpy
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from release_service_utils.tasks.managed.rh_sign_python_wheels import (
    build_slsa_predicate,
    convert_dsse_to_pep740,
    load_chains_predicate,
    main,
    run,
)

TASK = "release_service_utils.tasks.managed" ".rh_sign_python_wheels.rh_sign_python_wheels"

_DSSE_ENVELOPE = {
    "payload": "eyJzdWJqZWN0IjogW119",
    "payloadType": "application/vnd.in-toto+json",
    "signatures": [{"keyid": "", "sig": "MEUCIQD..."}],
}

_V1_PREDICATE: dict[str, Any] = {
    "buildDefinition": {
        "buildType": "https://slsa.dev/container-build/v1",
        "externalParameters": {"source": "git+https://example.com"},
        "resolvedDependencies": [{"uri": "oci://registry/image"}],
    },
    "runDetails": {
        "builder": {"id": "https://konflux-ci.dev/calunga"},
        "metadata": {
            "invocationId": "inv-123",
            "startedOn": "2024-01-01T00:00:00Z",
            "finishedOn": "2024-01-01T01:00:00Z",
        },
    },
}

_V02_PREDICATE: dict[str, Any] = {
    "buildType": "https://example.com/CustomBuild@v1",
    "invocation": {
        "parameters": {"param1": "value1"},
        "environment": {"env1": "val1"},
    },
    "materials": [{"uri": "git+https://example.com"}],
    "builder": {"id": "https://example.com/builder"},
    "metadata": {
        "buildInvocationId": "inv-456",
        "buildStartedOn": "2024-01-01T00:00:00Z",
        "buildFinishedOn": "2024-01-01T01:00:00Z",
    },
}


def _write_secrets(secrets_dir: Path) -> None:
    """Write minimal secret files to *secrets_dir*."""
    secrets_dir.mkdir(parents=True, exist_ok=True)
    (secrets_dir / "AWS_DEFAULT_REGION").write_text("us-east-1")
    (secrets_dir / "AWS_ACCESS_KEY_ID").write_text("AKID")
    (secrets_dir / "AWS_SECRET_ACCESS_KEY").write_text("secret")
    (secrets_dir / "SIGN_KEY").write_text("awskms:///key-id")


def _write_wheel(wheels_dir: Path, name: str = "pkg-1.0-py3-none-any.whl") -> Path:
    """Create a dummy wheel file."""
    wheels_dir.mkdir(parents=True, exist_ok=True)
    wheel = wheels_dir / name
    wheel.write_bytes(b"PK dummy wheel content")
    return wheel


def _write_provenance(
    wheels_dir: Path,
    predicate: dict[str, Any],
    filename: str = "sha256:abc123.json",
) -> Path:
    """Write a Chains provenance envelope under chains-provenance/."""
    prov_dir = wheels_dir / "chains-provenance"
    prov_dir.mkdir(parents=True, exist_ok=True)
    prov_file = prov_dir / filename
    prov_file.write_text(json.dumps({"predicate": predicate}), encoding="utf-8")
    return prov_file


# -- load_chains_predicate ----------------------------------------------------


def test_load_chains_predicate_no_dir(tmp_path: Path) -> None:
    """Return None when the directory does not exist."""
    assert load_chains_predicate(tmp_path / "missing") is None


def test_load_chains_predicate_empty_dir(tmp_path: Path) -> None:
    """Return None when no sha256:*.json files exist."""
    prov_dir = tmp_path / "prov"
    prov_dir.mkdir()
    assert load_chains_predicate(prov_dir) is None


def test_load_chains_predicate_valid(tmp_path: Path) -> None:
    """Return the predicate dict from the first matching file."""
    prov_dir = tmp_path / "prov"
    prov_dir.mkdir()
    (prov_dir / "sha256:aaa.json").write_text(
        json.dumps({"predicate": _V1_PREDICATE}), encoding="utf-8"
    )
    assert load_chains_predicate(prov_dir) == _V1_PREDICATE


def test_load_chains_predicate_not_dict(tmp_path: Path) -> None:
    """Return None when the predicate is not a dict."""
    prov_dir = tmp_path / "prov"
    prov_dir.mkdir()
    (prov_dir / "sha256:aaa.json").write_text(
        json.dumps({"predicate": "not-a-dict"}), encoding="utf-8"
    )
    assert load_chains_predicate(prov_dir) is None


def test_load_chains_predicate_missing_key(tmp_path: Path) -> None:
    """Return None when the envelope has no predicate key."""
    prov_dir = tmp_path / "prov"
    prov_dir.mkdir()
    (prov_dir / "sha256:aaa.json").write_text(json.dumps({"other": 1}), encoding="utf-8")
    assert load_chains_predicate(prov_dir) is None


def test_load_chains_predicate_uses_first_file(tmp_path: Path) -> None:
    """Only inspect the first sha256:*.json file (sorted)."""
    prov_dir = tmp_path / "prov"
    prov_dir.mkdir()
    (prov_dir / "sha256:aaa.json").write_text(
        json.dumps({"predicate": _V1_PREDICATE}), encoding="utf-8"
    )
    (prov_dir / "sha256:bbb.json").write_text(
        json.dumps({"predicate": _V02_PREDICATE}), encoding="utf-8"
    )
    assert load_chains_predicate(prov_dir) == _V1_PREDICATE


# -- build_slsa_predicate ----------------------------------------------------


def test_build_slsa_predicate_no_chains() -> None:
    """Return a minimal v1 predicate when no chains predicate exists."""
    result = build_slsa_predicate(None, "2024-01-01T00:00:00Z")
    assert result["buildDefinition"]["buildType"] == (
        "https://konflux-ci.dev/PythonWheelBuild@v1"
    )
    assert result["runDetails"]["builder"]["id"] == ("https://konflux-ci.dev/calunga")
    assert result["runDetails"]["metadata"]["finishedOn"] == "2024-01-01T00:00:00Z"


def test_build_slsa_predicate_v1_passthrough() -> None:
    """Use buildDefinition and runDetails as-is for v1 predicates."""
    result = build_slsa_predicate(_V1_PREDICATE, "2024-01-01T00:00:00Z")
    assert result["buildDefinition"] == _V1_PREDICATE["buildDefinition"]
    assert result["runDetails"] == _V1_PREDICATE["runDetails"]


def test_build_slsa_predicate_v02_conversion() -> None:
    """Convert v0.2 predicate fields to v1 format."""
    result = build_slsa_predicate(_V02_PREDICATE, "2024-06-01T00:00:00Z")
    bd = result["buildDefinition"]
    assert bd["buildType"] == "https://example.com/CustomBuild@v1"
    assert bd["externalParameters"] == {"param1": "value1"}
    assert bd["internalParameters"] == {"env1": "val1"}
    assert bd["resolvedDependencies"] == [{"uri": "git+https://example.com"}]
    rd = result["runDetails"]
    assert rd["builder"]["id"] == "https://example.com/builder"
    assert rd["metadata"]["invocationId"] == "inv-456"
    assert rd["metadata"]["startedOn"] == "2024-01-01T00:00:00Z"
    assert rd["metadata"]["finishedOn"] == "2024-01-01T01:00:00Z"


def test_build_slsa_predicate_v02_missing_fields_uses_defaults() -> None:
    """Use defaults when v0.2 predicate has missing fields."""
    result = build_slsa_predicate({"metadata": {}}, "2024-01-01T00:00:00Z")
    assert result["buildDefinition"]["buildType"] == (
        "https://konflux-ci.dev/PythonWheelBuild@v1"
    )
    assert result["runDetails"]["builder"]["id"] == ("https://konflux-ci.dev/calunga")
    assert result["runDetails"]["metadata"]["finishedOn"] == "2024-01-01T00:00:00Z"


# -- convert_dsse_to_pep740 --------------------------------------------------


def test_convert_dsse_to_pep740() -> None:
    """Convert DSSE envelope fields to PEP 740 format."""
    result = convert_dsse_to_pep740(_DSSE_ENVELOPE)
    assert result["version"] == 1
    assert result["verification_material"] is None
    assert result["envelope"]["statement"] == _DSSE_ENVELOPE["payload"]
    assert result["envelope"]["signature"] == "MEUCIQD..."


def test_convert_dsse_to_pep740_missing_payload() -> None:
    """Raise KeyError when payload is missing."""
    with pytest.raises(KeyError, match="payload"):
        convert_dsse_to_pep740({"signatures": [{"sig": "x"}]})


def test_convert_dsse_to_pep740_missing_signatures() -> None:
    """Raise KeyError when signatures is missing."""
    with pytest.raises(KeyError, match="signatures"):
        convert_dsse_to_pep740({"payload": "x"})


def test_convert_dsse_to_pep740_empty_signatures() -> None:
    """Raise IndexError when signatures list is empty."""
    with pytest.raises(IndexError):
        convert_dsse_to_pep740({"payload": "x", "signatures": []})


# -- _run_cosign --------------------------------------------------------------


def test_run_cosign_without_rekor(tmp_path: Path) -> None:
    """Use --tlog-upload=false when no rekor URL."""
    from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
        _run_cosign,
    )

    wheel = _write_wheel(tmp_path)
    pred = tmp_path / "pred.json"
    pred.write_text("{}")
    dsse = tmp_path / "dsse.json"

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run,
        patch(
            f"{TASK}.retry.retry_with_exponential_backoff", side_effect=lambda op, **kw: op()
        ),
    ):
        _run_cosign(wheel, pred, "awskms:///k", None, dsse, {}, 1)

    cmd = mock_run.call_args[0][0]
    assert "--tlog-upload=false" in cmd


def test_run_cosign_with_rekor(tmp_path: Path) -> None:
    """Use --rekor-url when rekor URL is provided."""
    from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
        _run_cosign,
    )

    wheel = _write_wheel(tmp_path)
    pred = tmp_path / "pred.json"
    pred.write_text("{}")
    dsse = tmp_path / "dsse.json"

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run,
        patch(
            f"{TASK}.retry.retry_with_exponential_backoff", side_effect=lambda op, **kw: op()
        ),
    ):
        _run_cosign(
            wheel,
            pred,
            "awskms:///k",
            "https://rekor.example.com",
            dsse,
            {},
            1,
        )

    cmd = mock_run.call_args[0][0]
    assert "--rekor-url=https://rekor.example.com" in cmd
    assert "--tlog-upload=false" not in cmd


def test_run_cosign_passes_max_attempts(tmp_path: Path) -> None:
    """Pass max_attempts to retry helper."""
    from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
        _run_cosign,
    )

    wheel = _write_wheel(tmp_path)
    pred = tmp_path / "pred.json"
    pred.write_text("{}")
    dsse = tmp_path / "dsse.json"

    with (
        patch(
            f"{TASK}.retry.retry_with_exponential_backoff", side_effect=lambda op, **kw: op()
        ) as mock_retry,
        patch(f"{TASK}.subprocess_cmd.run_cmd"),
    ):
        _run_cosign(wheel, pred, "k", None, dsse, {}, 5)

    assert mock_retry.call_args[1]["max_attempts"] == 5


def test_run_cosign_failure_logs_and_raises(tmp_path: Path) -> None:
    """Log and re-raise CalledProcessError on exhausted retries."""
    from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
        _run_cosign,
    )

    wheel = _write_wheel(tmp_path)
    pred = tmp_path / "pred.json"
    pred.write_text("{}")
    dsse = tmp_path / "dsse.json"

    with patch(
        f"{TASK}.retry.retry_with_exponential_backoff",
        side_effect=subprocess.CalledProcessError(1, "cosign"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            _run_cosign(wheel, pred, "k", None, dsse, {}, 2)


def test_run_cosign_passes_aws_env(tmp_path: Path) -> None:
    """Pass AWS env vars to run_cmd."""
    from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
        _run_cosign,
    )

    wheel = _write_wheel(tmp_path)
    pred = tmp_path / "pred.json"
    pred.write_text("{}")
    dsse = tmp_path / "dsse.json"
    aws_env = {"AWS_DEFAULT_REGION": "us-east-1"}

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd") as mock_run,
        patch(
            f"{TASK}.retry.retry_with_exponential_backoff", side_effect=lambda op, **kw: op()
        ),
    ):
        _run_cosign(wheel, pred, "k", None, dsse, aws_env, 1)

    assert mock_run.call_args[1]["env"] == aws_env


# -- _sign_artifact -----------------------------------------------------------


def test_sign_artifact_creates_attestation(tmp_path: Path) -> None:
    """Write a PEP 740 attestation file next to the wheel."""
    from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
        _sign_artifact,
    )

    wheels_dir = tmp_path / "files"
    wheel = _write_wheel(wheels_dir)

    with (
        patch(f"{TASK}._run_cosign"),
        patch(f"{TASK}.file.load_json_dict", return_value=_DSSE_ENVELOPE),
    ):
        _sign_artifact(
            wheel,
            wheels_dir,
            None,
            "2024-01-01T00:00:00Z",
            "key",
            None,
            {},
            1,
        )

    att_file = wheels_dir / f"{wheel.name}.attestation"
    assert att_file.is_file()
    att = json.loads(att_file.read_text(encoding="utf-8"))
    assert att["version"] == 1
    assert att["envelope"]["statement"] == _DSSE_ENVELOPE["payload"]


def test_sign_artifact_cleans_temp_files_on_success(tmp_path: Path) -> None:
    """Temp predicate and DSSE files are removed after success."""
    from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
        _sign_artifact,
    )

    wheels_dir = tmp_path / "files"
    wheel = _write_wheel(wheels_dir)
    created_temps: list[Path] = []

    def track_tempfile(prefix: str, data: bytes | None = None) -> Path:
        p = tmp_path / f"temp_{prefix}"
        p.write_bytes(data or b"")
        created_temps.append(p)
        return p

    with (
        patch(f"{TASK}._run_cosign"),
        patch(f"{TASK}.file.load_json_dict", return_value=_DSSE_ENVELOPE),
        patch(f"{TASK}.file.make_tempfile_path", side_effect=track_tempfile),
    ):
        _sign_artifact(
            wheel,
            wheels_dir,
            None,
            "2024-01-01T00:00:00Z",
            "key",
            None,
            {},
            1,
        )

    for p in created_temps:
        assert not p.exists()


def test_sign_artifact_cleans_temp_files_on_failure(tmp_path: Path) -> None:
    """Temp files are removed even when cosign fails."""
    from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
        _sign_artifact,
    )

    wheels_dir = tmp_path / "files"
    wheel = _write_wheel(wheels_dir)
    created_temps: list[Path] = []

    def track_tempfile(prefix: str, data: bytes | None = None) -> Path:
        p = tmp_path / f"temp_{prefix}"
        p.write_bytes(data or b"")
        created_temps.append(p)
        return p

    with (
        patch(f"{TASK}._run_cosign", side_effect=subprocess.CalledProcessError(1, "cosign")),
        patch(f"{TASK}.file.make_tempfile_path", side_effect=track_tempfile),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            _sign_artifact(
                wheel,
                wheels_dir,
                None,
                "2024-01-01T00:00:00Z",
                "key",
                None,
                {},
                1,
            )

    for p in created_temps:
        assert not p.exists()


# -- run ----------------------------------------------------------------------


def test_run_signs_whl_and_tar_gz(tmp_path: Path) -> None:
    """Process both .whl and .tar.gz files."""
    wheels_dir = tmp_path / "files"
    _write_wheel(wheels_dir, "a-1.0-py3-none-any.whl")
    _write_wheel(wheels_dir, "a-1.0.tar.gz")
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)
    result_path = tmp_path / "result"

    with patch(f"{TASK}._sign_artifact") as mock_sign:
        run(
            data_dir=tmp_path,
            files_dir="files",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=result_path,
        )

    assert mock_sign.call_count == 2
    assert result_path.read_text() == "2"


def test_run_no_files_writes_zero(tmp_path: Path) -> None:
    """Write 0 to result when no .whl or .tar.gz files exist."""
    (tmp_path / "files").mkdir()
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)
    result_path = tmp_path / "result"

    with patch(f"{TASK}._sign_artifact") as mock_sign:
        run(
            data_dir=tmp_path,
            files_dir="files",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=result_path,
        )

    mock_sign.assert_not_called()
    assert result_path.read_text() == "0"


def test_run_missing_files_dir_raises(tmp_path: Path) -> None:
    """Raise FileNotFoundError when files directory does not exist."""
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)

    with pytest.raises(FileNotFoundError, match="does not exist"):
        run(
            data_dir=tmp_path,
            files_dir="missing",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=tmp_path / "result",
        )


def test_run_path_traversal_rejected(tmp_path: Path) -> None:
    """Raise ValueError when files_dir escapes data_dir."""
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)

    with pytest.raises(ValueError, match="path must stay under"):
        run(
            data_dir=tmp_path,
            files_dir="../outside",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=tmp_path / "result",
        )


def test_run_loads_chains_predicate(tmp_path: Path) -> None:
    """Pass chains predicate to _sign_artifact when available."""
    wheels_dir = tmp_path / "files"
    _write_wheel(wheels_dir)
    _write_provenance(wheels_dir, _V1_PREDICATE)
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)

    with patch(f"{TASK}._sign_artifact") as mock_sign:
        run(
            data_dir=tmp_path,
            files_dir="files",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=tmp_path / "result",
        )

    assert mock_sign.call_args[0][2] == _V1_PREDICATE


def test_run_cleans_provenance_dir(tmp_path: Path) -> None:
    """Remove chains-provenance directory after processing."""
    wheels_dir = tmp_path / "files"
    _write_wheel(wheels_dir)
    _write_provenance(wheels_dir, _V1_PREDICATE)
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)
    prov_dir = wheels_dir / "chains-provenance"
    assert prov_dir.is_dir()

    with patch(f"{TASK}._sign_artifact"):
        run(
            data_dir=tmp_path,
            files_dir="files",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=tmp_path / "result",
        )

    assert not prov_dir.exists()


def test_run_rekor_url_read_when_present(tmp_path: Path) -> None:
    """Read REKOR_URL from secrets when file exists and is non-empty."""
    wheels_dir = tmp_path / "files"
    _write_wheel(wheels_dir)
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)
    (secrets_dir / "REKOR_URL").write_text("https://rekor.example.com\n")

    with patch(f"{TASK}._sign_artifact") as mock_sign:
        run(
            data_dir=tmp_path,
            files_dir="files",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=tmp_path / "result",
        )

    assert mock_sign.call_args[0][5] == "https://rekor.example.com"


def test_run_rekor_url_none_when_missing(tmp_path: Path) -> None:
    """Pass None for rekor_url when the file does not exist."""
    wheels_dir = tmp_path / "files"
    _write_wheel(wheels_dir)
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)

    with patch(f"{TASK}._sign_artifact") as mock_sign:
        run(
            data_dir=tmp_path,
            files_dir="files",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=tmp_path / "result",
        )

    assert mock_sign.call_args[0][5] is None


def test_run_rekor_url_none_when_empty(tmp_path: Path) -> None:
    """Pass None for rekor_url when the file is empty."""
    wheels_dir = tmp_path / "files"
    _write_wheel(wheels_dir)
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)
    (secrets_dir / "REKOR_URL").write_text("")

    with patch(f"{TASK}._sign_artifact") as mock_sign:
        run(
            data_dir=tmp_path,
            files_dir="files",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=tmp_path / "result",
        )

    assert mock_sign.call_args[0][5] is None


def test_run_skips_non_file_whl(tmp_path: Path) -> None:
    """Skip directories whose names end in .whl."""
    wheels_dir = tmp_path / "files"
    wheels_dir.mkdir()
    (wheels_dir / "fake.whl").mkdir()
    secrets_dir = tmp_path / "secrets"
    _write_secrets(secrets_dir)

    with patch(f"{TASK}._sign_artifact") as mock_sign:
        run(
            data_dir=tmp_path,
            files_dir="files",
            secrets_dir=secrets_dir,
            max_attempts=1,
            result_path=tmp_path / "result",
        )

    mock_sign.assert_not_called()


# -- main ---------------------------------------------------------------------


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return 0 on successful run."""
    result_file = tmp_path / "att_count"
    monkeypatch.setenv("RESULT_ATTESTATION_COUNT", str(result_file))

    with patch(f"{TASK}.run") as mock_run:
        assert (
            main(
                [
                    "--data-dir",
                    str(tmp_path),
                    "--files-dir",
                    "files",
                    "--secrets-dir",
                    str(tmp_path / "secrets"),
                    "--retries",
                    "2",
                ]
            )
            == 0
        )

    kw = mock_run.call_args[1]
    assert kw["data_dir"] == tmp_path
    assert kw["files_dir"] == "files"
    assert kw["secrets_dir"] == tmp_path / "secrets"
    assert kw["max_attempts"] == 3
    assert kw["result_path"] == result_file


def test_main_default_retries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Default --retries=3 means max_attempts=4."""
    monkeypatch.setenv("RESULT_ATTESTATION_COUNT", str(tmp_path / "r"))

    with patch(f"{TASK}.run") as mock_run:
        main(["--data-dir", str(tmp_path), "--files-dir", "f"])

    assert mock_run.call_args[1]["max_attempts"] == 4


def test_main_missing_result_env_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SystemExit when RESULT_ATTESTATION_COUNT is not set."""
    monkeypatch.delenv("RESULT_ATTESTATION_COUNT", raising=False)

    with pytest.raises(SystemExit):
        main(["--data-dir", "/d", "--files-dir", "f"])


def test_main_missing_required_arg_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SystemExit when required args are missing."""
    monkeypatch.setenv("RESULT_ATTESTATION_COUNT", str(tmp_path / "r"))

    with pytest.raises(SystemExit):
        main([])


def test_main_module_entry_point() -> None:
    """Running the package as a module calls main()."""
    module = "release_service_utils.tasks.managed.rh_sign_python_wheels"
    with (
        patch(f"{TASK}.main", return_value=0) as mock_main,
        pytest.raises(SystemExit) as exc,
    ):
        runpy.run_module(module, run_name="__main__")
    assert exc.value.code == 0
    mock_main.assert_called_once_with()
