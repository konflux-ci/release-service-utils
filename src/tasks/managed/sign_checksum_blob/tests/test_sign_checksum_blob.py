"""Unit tests for sign_checksum_blob."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.sign_checksum_blob.sign_checksum_blob import (
    SigningConfig,
    _signature_for_checksum,
    finalize,
    find_checksum_file,
    prepare,
    resolve_signing_config,
    sign,
    signature_is_valid,
)

MODULE = "release_service_utils.tasks.managed.sign_checksum_blob.sign_checksum_blob"


# --- find_checksum_file ---


def test_find_checksum_file_single(tmp_path) -> None:
    """A single SHA256SUMS file is returned."""
    (tmp_path / "v1.0_SHA256SUMS").write_text("sum")
    assert find_checksum_file(tmp_path).name == "v1.0_SHA256SUMS"


def test_find_checksum_file_excludes_sig(tmp_path) -> None:
    """An existing .sig sibling is ignored."""
    (tmp_path / "SHA256SUMS").write_text("sum")
    (tmp_path / "SHA256SUMS.sig").write_text("sig")
    assert find_checksum_file(tmp_path).name == "SHA256SUMS"


def test_find_checksum_file_excludes_directories(tmp_path) -> None:
    """A directory with a checksum-like name is ignored."""
    (tmp_path / "SHA256SUMS").write_text("sum")
    (tmp_path / "nested_SHA256SUMS").mkdir()
    assert find_checksum_file(tmp_path).name == "SHA256SUMS"


def test_find_checksum_file_none_raises(tmp_path) -> None:
    """No checksum file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        find_checksum_file(tmp_path)


def test_find_checksum_file_multiple_raises(tmp_path) -> None:
    """More than one checksum file raises ValueError."""
    (tmp_path / "a_SHA256SUMS").write_text("1")
    (tmp_path / "b_SHA256SUMS").write_text("2")
    with pytest.raises(ValueError, match="exactly one"):
        find_checksum_file(tmp_path)


# --- signature_is_valid ---


def test_signature_is_valid_missing(tmp_path) -> None:
    """A missing sig file is not valid."""
    assert signature_is_valid(tmp_path / "nope.sig") is False


def test_signature_is_valid_parseable(tmp_path) -> None:
    """A parseable sig file is valid."""
    sig = tmp_path / "s.sig"
    sig.write_bytes(b"data")
    with patch(f"{MODULE}.run_cmd", return_value=MagicMock(returncode=0)):
        assert signature_is_valid(sig) is True


# --- resolve_signing_config ---


def test_resolve_signing_config_uses_configmap(tmp_path) -> None:
    """resolve_signing_config reads the key and Kerberos overrides."""
    with patch(f"{MODULE}.file.load_json_dict", return_value={"sign": {}}):
        with patch(
            f"{MODULE}.kubectl.get_configmap",
            return_value={
                "data": {
                    "SIG_KEY_NAME": "prodkey",
                    "KERBEROS_KEYTAB_SECRET": "stage-keytab",
                    "KERBEROS_KEYTAB": "/etc/kerberos/keytab",
                    "KERBEROS_PRINCIPAL": "signer@EXAMPLE.COM",
                }
            },
        ) as cm:
            config = resolve_signing_config(tmp_path / "data.json")
    assert config.keyname == "prodkey"
    assert config.kerberos_keytab_secret == "stage-keytab"
    assert config.kerberos_keytab == "/etc/kerberos/keytab"
    assert config.kerberos_principal == "signer@EXAMPLE.COM"
    cm.assert_called_once_with("signing-config-map")


def test_resolve_signing_config_custom_config_map(tmp_path) -> None:
    """A custom configMapName is honoured."""
    data = {"sign": {"configMapName": "custom-cm"}}
    with patch(f"{MODULE}.file.load_json_dict", return_value=data):
        with patch(
            f"{MODULE}.kubectl.get_configmap",
            return_value={"data": {"SIG_KEY_NAME": "k"}},
        ) as cm:
            config = resolve_signing_config(tmp_path / "data.json")
    assert config.keyname == "k"
    assert config.kerberos_keytab_secret == ""
    cm.assert_called_once_with("custom-cm")


# --- prepare ---


def test_prepare_copies_checksum(tmp_path) -> None:
    """Prepare copies the checksum file into the isolated dir."""
    binaries = tmp_path / "binaries"
    binaries.mkdir()
    (binaries / "SHA256SUMS").write_text("sum")
    isolated = tmp_path / "isolated"

    with patch(f"{MODULE}.signature_is_valid", return_value=False):
        signature_valid, checksum_name = prepare(binaries_dir=binaries, isolated_dir=isolated)

    assert signature_valid is False
    assert checksum_name == "SHA256SUMS"

    assert (isolated / "SHA256SUMS").read_text() == "sum"


def test_prepare_skips_copy_for_valid_signature(tmp_path) -> None:
    """Prepare reports a valid signature without creating an input directory."""
    binaries = tmp_path / "binaries"
    binaries.mkdir()
    (binaries / "SHA256SUMS").write_text("sum")
    (binaries / "SHA256SUMS.sig").write_bytes(b"valid")
    isolated = tmp_path / "isolated"

    with patch(f"{MODULE}.signature_is_valid", return_value=True):
        signature_valid, checksum_name = prepare(binaries_dir=binaries, isolated_dir=isolated)

    assert signature_valid is True
    assert checksum_name == "SHA256SUMS"
    assert not isolated.exists()


def test_prepare_removes_invalid_signature(tmp_path) -> None:
    """Prepare removes an invalid signature before copying the checksum."""
    binaries = tmp_path / "binaries"
    binaries.mkdir()
    (binaries / "SHA256SUMS").write_text("sum")
    sig_path = binaries / "SHA256SUMS.sig"
    sig_path.write_bytes(b"invalid")
    isolated = tmp_path / "isolated"

    with patch(f"{MODULE}.signature_is_valid", return_value=False):
        signature_valid, checksum_name = prepare(binaries_dir=binaries, isolated_dir=isolated)

    assert signature_valid is False
    assert checksum_name == "SHA256SUMS"
    assert not sig_path.exists()
    assert (isolated / "SHA256SUMS").read_text() == "sum"


# --- _signature_for_checksum ---


def test_signature_for_checksum_match(tmp_path) -> None:
    """The signature file matching the checksum basename is returned."""
    results = {
        "results": [
            {"file": "other", "signature_file": "signatures/0_a.sig"},
            {"file": "dir/SHA256SUMS", "signature_file": "signatures/1_b.sig"},
        ]
    }
    path = _signature_for_checksum(results, tmp_path, "SHA256SUMS")
    assert path == tmp_path / "signatures/1_b.sig"


def test_signature_for_checksum_missing_raises(tmp_path) -> None:
    """No matching entry raises RuntimeError."""
    with pytest.raises(RuntimeError, match="No signature found"):
        _signature_for_checksum({"results": []}, tmp_path, "SHA256SUMS")


def test_signature_for_checksum_empty_file_raises(tmp_path) -> None:
    """A matching entry with no signature_file raises RuntimeError."""
    results = {"results": [{"file": "SHA256SUMS", "signature_file": ""}]}
    with pytest.raises(RuntimeError, match="no signature_file"):
        _signature_for_checksum(results, tmp_path, "SHA256SUMS")


# --- sign ---


def _make_sign_env(tmp_path) -> tuple[Path, Path]:
    output_file = tmp_path / "output-artifact.txt"
    return tmp_path / "data.json", output_file


def _sign_kwargs(data_file, output_file) -> dict:
    return dict(
        data_file=data_file,
        input_artifact_uri="oci://in",
        output_artifact_file=output_file,
        requester="alice",
        pipelinerun_uid="uid",
        task_id="tid",
        signing_repo="repo",
        signing_revision="rev",
        ta_task_git_url="cat",
        ta_task_git_revision="prod",
        oci_storage="",
        oras_options="",
        request_timeout=1800,
    )


def test_sign_submits_and_records_output(tmp_path) -> None:
    """Sign submits a detachsign request and records the output TA URI."""
    data_file, output_file = _make_sign_env(tmp_path)

    with (
        patch(
            f"{MODULE}.resolve_signing_config",
            return_value=SigningConfig(
                keyname="key",
                kerberos_keytab_secret="stage-keytab",
                kerberos_keytab="/etc/kerberos/keytab",
                kerberos_principal="signer@EXAMPLE.COM",
            ),
        ),
        patch(
            f"{MODULE}.direct_sign_generic.submit",
            return_value={"sourceDataArtifact": "oci://out"},
        ) as submit_mock,
    ):
        sign(**_sign_kwargs(data_file, output_file))

    request = submit_mock.call_args.args[0]
    assert request.sign_method == "detachsign"
    assert request.onbehalfof == "alice"
    assert request.keyname == "key"
    assert request.kerberos_keytab_secret == "stage-keytab"
    assert request.kerberos_keytab == "/etc/kerberos/keytab"
    assert request.kerberos_principal == "signer@EXAMPLE.COM"
    assert output_file.read_text() == "oci://out"


def test_sign_empty_result_raises(tmp_path) -> None:
    """An empty sourceDataArtifact result raises RuntimeError."""
    data_file, output_file = _make_sign_env(tmp_path)

    with (
        patch(
            f"{MODULE}.resolve_signing_config",
            return_value=SigningConfig(keyname="key"),
        ),
        patch(f"{MODULE}.direct_sign_generic.submit", return_value={}),
    ):
        with pytest.raises(RuntimeError, match="empty sourceDataArtifact"):
            sign(**_sign_kwargs(data_file, output_file))


# --- finalize ---


@pytest.mark.skipif(shutil.which("gpg") is None, reason="gpg is not installed")
def test_finalize_dearmors_with_real_gpg(tmp_path) -> None:
    """Finalize dearmors an ASCII-armored signature with the real gpg binary."""
    binaries = tmp_path / "binaries"
    binaries.mkdir()

    signed_output = tmp_path / "signed"
    signatures = signed_output / "signatures"
    signatures.mkdir(parents=True)
    fixture = Path(__file__).parent / "data" / "armored-signature.asc"
    armored_signature = signatures / "0_a.sig"
    armored_signature.write_text(fixture.read_text(encoding="ascii"), encoding="ascii")
    (signed_output / "results.json").write_text(
        json.dumps(
            {"results": [{"file": "SHA256SUMS", "signature_file": "signatures/0_a.sig"}]}
        )
    )

    finalize(
        binaries_dir=binaries,
        signed_output_dir=signed_output,
        checksum_name="SHA256SUMS",
    )

    output = binaries / "SHA256SUMS.sig"
    assert output.stat().st_size > 0
    subprocess.run(["gpg", "--list-packets", str(output)], check=True, capture_output=True)


def test_finalize_rejects_path_checksum_name(tmp_path) -> None:
    """Finalize rejects a checksum result that is not a plain filename."""
    with pytest.raises(ValueError, match="checksum filename"):
        finalize(
            binaries_dir=tmp_path,
            signed_output_dir=tmp_path,
            checksum_name="nested/SHA256SUMS",
        )
