"""Unit tests for sign_image_cosign_keyless."""

from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.sign_image_cosign_keyless import (
    KeylessConfig,
    SignItem,
    certificate_identity_from_oidc_token,
    check_existing_cosign_signature,
    collect_component_sign_items,
    get_manifest_digests,
    initialize_tuf,
    run_cosign_with_retry,
    setup_argparser,
    sign_all,
    sign_item,
)

TASK = (
    "release_service_utils.tasks.managed.sign_image_cosign_keyless.sign_image_cosign_keyless"
)

EXPECTED_IDENTITY = "https://kubernetes.io/namespaces/default/serviceaccounts/default"

CONFIG = KeylessConfig(
    oidc_issuer="https://kubernetes.default.svc",
    fulcio_url="fake-fulcio-server",
    rekor_url="fake-rekor-server",
    tuf_url="https://tuf.example.com",
    oidc_token_path=Path("/var/run/secrets/tokens/oidc-token"),
    certificate_identity=EXPECTED_IDENTITY,
)

COMPONENT = {
    "name": "comp0",
    "containerImage": "quay.io/internal/test-image@sha256:top",
    "repositories": [
        {
            "url": "quay.io/pending/test-image",
            "rh-registry-repo": "registry.redhat.io/test-product/test-image",
            "tags": ["t1", "t2"],
        }
    ],
}


def _jwt_token(namespace: str, sa_name: str) -> str:
    """Return a JWT whose payload carries Kubernetes SA claims."""
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = json.dumps(
        {
            "kubernetes.io": {
                "namespace": namespace,
                "serviceaccount": {"name": sa_name, "uid": "abc"},
            }
        }
    ).encode()
    payload_b64 = base64.urlsafe_b64encode(payload).rstrip(b"=").decode()
    return f"{header}.{payload_b64}.sig"


# --- certificate_identity_from_oidc_token ---


def test_certificate_identity_from_oidc_token(tmp_path: Path) -> None:
    """Decode the Kubernetes SA identity from a projected OIDC JWT."""
    token_path = tmp_path / "oidc-token"
    token_path.write_text(_jwt_token("default", "default"))

    identity = certificate_identity_from_oidc_token(token_path)

    assert identity == EXPECTED_IDENTITY


def test_certificate_identity_from_unpadded_payload(tmp_path: Path) -> None:
    """Payloads whose base64url length is not a multiple of 4 still decode."""
    token_path = tmp_path / "oidc-token"
    token_path.write_text(_jwt_token("release", "pipeline"))

    identity = certificate_identity_from_oidc_token(token_path)

    assert identity == ("https://kubernetes.io/namespaces/release/serviceaccounts/pipeline")


def test_certificate_identity_missing_claims_raises(tmp_path: Path) -> None:
    """Raise when the JWT payload has no kubernetes.io claims."""
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(b'{"sub":"x"}').rstrip(b"=").decode()
    token_path = tmp_path / "oidc-token"
    token_path.write_text(f"{header}.{payload}.sig")

    with pytest.raises(ValueError, match="kubernetes.io"):
        certificate_identity_from_oidc_token(token_path)


def test_certificate_identity_not_jwt_raises(tmp_path: Path) -> None:
    """Raise when the token is not a dotted JWT."""
    token_path = tmp_path / "oidc-token"
    token_path.write_text("not-a-jwt")

    with pytest.raises(ValueError, match="not a JWT"):
        certificate_identity_from_oidc_token(token_path)


# --- initialize_tuf ---


@patch(f"{TASK}.run_cmd")
def test_initialize_tuf_calls_cosign_initialize(mock_run_cmd: MagicMock) -> None:
    """Cosign initialize is invoked with --mirror and --root from the TUF URL."""
    initialize_tuf("https://tuf.example.com")

    mock_run_cmd.assert_called_once_with(
        [
            "cosign",
            "initialize",
            "--mirror=https://tuf.example.com",
            "--root=https://tuf.example.com/root.json",
        ]
    )


# --- get_manifest_digests ---


def test_get_manifest_digests_uses_image_digests_field() -> None:
    """ImageDigests in the snapshot is preferred over skopeo inspect."""
    component = {
        "name": "c",
        "containerImage": "reg/repo@sha256:top",
        "imageDigests": ["sha256:arm", "sha256:amd"],
    }
    is_list, digests = get_manifest_digests(component)

    assert is_list is True
    assert digests == ["sha256:top", "sha256:arm", "sha256:amd"]


@patch(f"{TASK}.skopeo")
def test_get_manifest_digests_single_manifest(mock_skopeo: MagicMock) -> None:
    """Single-manifest images return is_list=False with only the top-level digest."""
    mock_skopeo.inspect.return_value = MagicMock(
        returncode=0,
        stdout=json.dumps(
            {"mediaType": "application/vnd.docker.distribution.manifest.v2+json"}
        ),
    )

    component = {"name": "c", "containerImage": "reg/repo@sha256:abc"}
    is_list, digests = get_manifest_digests(component)

    assert is_list is False
    assert digests == ["sha256:abc"]
    mock_skopeo.inspect.assert_called_once_with("reg/repo@sha256:abc", raw=True)


@patch(f"{TASK}.skopeo")
def test_get_manifest_digests_oci_index(mock_skopeo: MagicMock) -> None:
    """OCI image indexes return nested digests via skopeo inspect --raw."""
    raw = {
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{"digest": "sha256:arm"}, {"digest": "sha256:amd"}],
    }
    mock_skopeo.inspect.return_value = MagicMock(returncode=0, stdout=json.dumps(raw))

    component = {"name": "c", "containerImage": "reg/repo@sha256:top"}
    is_list, digests = get_manifest_digests(component)

    assert is_list is True
    assert digests == ["sha256:top", "sha256:arm", "sha256:amd"]


@patch(f"{TASK}.skopeo")
def test_get_manifest_digests_docker_manifest_list(mock_skopeo: MagicMock) -> None:
    """Docker manifest lists return nested digests via skopeo inspect --raw."""
    raw = {
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "manifests": [{"digest": "sha256:1111-1"}, {"digest": "sha256:1111-2"}],
    }
    mock_skopeo.inspect.return_value = MagicMock(returncode=0, stdout=json.dumps(raw))

    is_list, digests = get_manifest_digests(
        {"name": "c", "containerImage": "reg/repo@sha256:1111"}
    )

    assert is_list is True
    assert digests == ["sha256:1111", "sha256:1111-1", "sha256:1111-2"]


@patch(f"{TASK}.skopeo")
def test_get_manifest_digests_skopeo_failure_raises(mock_skopeo: MagicMock) -> None:
    """RuntimeError is raised when skopeo inspect fails."""
    mock_skopeo.inspect.return_value = MagicMock(returncode=1, stderr="not found")

    with pytest.raises(RuntimeError, match="skopeo inspect failed"):
        get_manifest_digests({"name": "c", "containerImage": "reg/repo@sha256:x"})


# --- collect_component_sign_items ---


@patch(f"{TASK}.get_manifest_digests", return_value=(False, ["sha256:top"]))
def test_collect_component_single_manifest(_mock_digests: MagicMock) -> None:
    """Single-manifest component produces one item per tag using repository url."""
    items = collect_component_sign_items(COMPONENT)

    assert len(items) == 2
    identities = {i.identity for i in items}
    assert identities == {
        "quay.io/pending/test-image:t1",
        "quay.io/pending/test-image:t2",
    }
    for item in items:
        assert item.source == "quay.io/pending/test-image"
        assert item.digest == "sha256:top"


@patch(
    f"{TASK}.get_manifest_digests",
    return_value=(True, ["sha256:top", "sha256:arm", "sha256:amd"]),
)
def test_collect_component_manifest_list_produces_nested_items(
    _mock_digests: MagicMock,
) -> None:
    """Manifest-list components produce items for nested digests plus the index."""
    items = collect_component_sign_items(COMPONENT)

    digests_seen = {i.digest for i in items}
    assert digests_seen == {"sha256:top", "sha256:arm", "sha256:amd"}
    # 3 digests * 2 tags
    assert len(items) == 6


@patch(f"{TASK}.get_manifest_digests", return_value=(False, ["sha256:top"]))
def test_collect_component_no_tags_skipped(_mock_digests: MagicMock) -> None:
    """Repositories without tags produce no signing items."""
    component = {
        "name": "c",
        "containerImage": "reg/repo@sha256:top",
        "repositories": [{"url": "reg/repo", "tags": []}],
    }
    items = collect_component_sign_items(component)
    assert items == []


@patch(f"{TASK}.get_manifest_digests", return_value=(False, ["sha256:top"]))
def test_collect_component_ignores_rh_registry_repo(_mock_digests: MagicMock) -> None:
    """Keyless signing uses repository url, not rh-registry-repo, as identity."""
    items = collect_component_sign_items(COMPONENT)

    assert all(item.identity.startswith("quay.io/pending/") for item in items)
    assert all("registry.redhat.io" not in item.identity for item in items)


# --- run_cosign_with_retry ---


@patch("subprocess.run")
def test_run_cosign_with_retry_success_on_first_attempt(mock_run: MagicMock) -> None:
    """Success on the first attempt returns the CompletedProcess immediately."""
    mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")

    result = run_cosign_with_retry(["cosign", "sign", "ref"], retries=3)

    assert mock_run.call_count == 1
    assert result.returncode == 0


@patch("time.sleep")
@patch("subprocess.run")
def test_run_cosign_with_retry_retries_on_failure(
    mock_run: MagicMock, mock_sleep: MagicMock
) -> None:
    """Failed cosign is retried up to the configured limit with Fibonacci delays."""
    error = subprocess.CalledProcessError(1, "cosign", stderr="error")
    mock_run.side_effect = [error, error, MagicMock(returncode=0, stdout="", stderr="")]

    run_cosign_with_retry(["cosign", "sign"], retries=3)

    assert mock_run.call_count == 3
    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(3)
    mock_sleep.assert_any_call(5)


@patch("time.sleep")
@patch("subprocess.run")
def test_run_cosign_with_retry_raises_after_all_attempts(
    mock_run: MagicMock, mock_sleep: MagicMock
) -> None:
    """CalledProcessError is raised when all attempts are exhausted."""
    error = subprocess.CalledProcessError(1, "cosign", stderr="error")
    mock_run.side_effect = error

    with pytest.raises(subprocess.CalledProcessError):
        run_cosign_with_retry(["cosign", "sign"], retries=2)

    assert mock_run.call_count == 3  # 1 initial + 2 retries


# --- check_existing_cosign_signature ---


@patch(f"{TASK}.run_cosign_with_retry")
def test_check_existing_signature_found(mock_cosign: MagicMock) -> None:
    """Return True when a matching signature record is found in verify output."""
    sig_output = json.dumps(
        [
            {
                "critical": {
                    "image": {"docker-manifest-digest": "sha256:abc"},
                    "identity": {"docker-reference": "quay.io/pending/repo:t1"},
                }
            }
        ]
    )
    mock_cosign.return_value = MagicMock(stdout=sig_output)

    found = check_existing_cosign_signature(
        "quay.io/pending/repo:t1",
        "quay.io/pending/repo",
        "sha256:abc",
        CONFIG,
        retries=3,
        env={},
    )

    assert found is True
    verify_args = mock_cosign.call_args[0][0]
    assert verify_args[0:2] == ["cosign", "verify"]
    assert "--rekor-url=fake-rekor-server" in verify_args
    assert f"--certificate-identity={EXPECTED_IDENTITY}" in verify_args
    assert "--certificate-oidc-issuer=https://kubernetes.default.svc" in verify_args
    assert "quay.io/pending/repo@sha256:abc" in verify_args


@patch(f"{TASK}.run_cosign_with_retry")
def test_check_existing_signature_not_found(mock_cosign: MagicMock) -> None:
    """Return False when verify output contains no matching signature."""
    mock_cosign.return_value = MagicMock(stdout="[]")

    found = check_existing_cosign_signature(
        "quay.io/pending/repo:t1",
        "quay.io/pending/repo",
        "sha256:abc",
        CONFIG,
        retries=3,
        env={},
    )
    assert found is False


@patch(f"{TASK}.run_cosign_with_retry")
def test_check_existing_signature_empty_stdout_treated_as_unsigned(
    mock_cosign: MagicMock,
) -> None:
    """Treat empty cosign verify stdout as an empty JSON array."""
    mock_cosign.return_value = MagicMock(stdout="")

    found = check_existing_cosign_signature(
        "quay.io/pending/repo:t1",
        "quay.io/pending/repo",
        "sha256:abc",
        CONFIG,
        retries=3,
        env={},
    )
    assert found is False


@patch(f"{TASK}.run_cosign_with_retry")
def test_check_existing_signature_verify_failure_raises(mock_cosign: MagicMock) -> None:
    """Propagate the error when cosign verify fails.

    A verification failure must not be treated as "no signature found": doing
    so would let the task add a duplicate signature instead of failing loudly,
    matching the original bash task's ``set -e`` behavior.
    """
    mock_cosign.side_effect = subprocess.CalledProcessError(1, "cosign", stderr="no sig")

    with pytest.raises(subprocess.CalledProcessError):
        check_existing_cosign_signature(
            "quay.io/pending/repo:t1",
            "quay.io/pending/repo",
            "sha256:abc",
            CONFIG,
            retries=3,
            env={},
        )


@patch(f"{TASK}.run_cosign_with_retry")
def test_check_existing_signature_invalid_json_raises(mock_cosign: MagicMock) -> None:
    """Raise ValueError when cosign verify exits 0 but prints invalid JSON."""
    mock_cosign.return_value = MagicMock(stdout="not valid json {")

    with pytest.raises(ValueError, match="not valid JSON"):
        check_existing_cosign_signature(
            "quay.io/pending/repo:t1",
            "quay.io/pending/repo",
            "sha256:abc",
            CONFIG,
            retries=3,
            env={},
        )


# --- sign_item ---


@patch(f"{TASK}.run_cosign_with_retry")
@patch(f"{TASK}.check_existing_cosign_signature", return_value=True)
@patch(f"{TASK}.run_cmd")
def test_sign_item_skips_when_already_signed(
    mock_run_cmd: MagicMock,
    _mock_check: MagicMock,
    mock_cosign: MagicMock,
) -> None:
    """Do not call cosign sign when a signature is already present."""
    mock_run_cmd.return_value = MagicMock(stdout='{"auths":{}}')

    sign_item(
        SignItem("quay.io/pending/repo:t1", "quay.io/pending/repo", "sha256:abc"),
        CONFIG,
        retries=3,
    )

    mock_cosign.assert_not_called()


@patch(f"{TASK}.run_cosign_with_retry")
@patch(f"{TASK}.check_existing_cosign_signature", return_value=False)
@patch(f"{TASK}.run_cmd")
def test_sign_item_calls_cosign_when_not_signed(
    mock_run_cmd: MagicMock,
    _mock_check: MagicMock,
    mock_cosign: MagicMock,
) -> None:
    """Call cosign sign with keyless flags when the image is unsigned."""
    mock_run_cmd.return_value = MagicMock(stdout='{"auths":{}}')
    mock_cosign.return_value = MagicMock(returncode=0, stdout="", stderr="")

    sign_item(
        SignItem("quay.io/pending/repo:t1", "quay.io/pending/repo", "sha256:abc"),
        CONFIG,
        retries=3,
    )

    mock_cosign.assert_called_once()
    sign_args = mock_cosign.call_args[0][0]
    assert sign_args[:4] == ["cosign", "-t", "3m0s", "sign"]
    assert "-y" in sign_args
    assert "--rekor-url=fake-rekor-server" in sign_args
    assert "--identity-token" in sign_args
    token_idx = sign_args.index("--identity-token")
    assert sign_args[token_idx + 1] == str(CONFIG.oidc_token_path)
    assert "--fulcio-url" in sign_args
    fulcio_idx = sign_args.index("--fulcio-url")
    assert sign_args[fulcio_idx + 1] == "fake-fulcio-server"
    assert "--sign-container-identity" in sign_args
    assert "quay.io/pending/repo:t1" in sign_args
    assert "quay.io/pending/repo@sha256:abc" in sign_args


@patch(f"{TASK}.run_cosign_with_retry")
@patch(f"{TASK}.check_existing_cosign_signature", return_value=False)
@patch(f"{TASK}.run_cmd")
def test_sign_item_passes_docker_config_to_verify(
    mock_run_cmd: MagicMock,
    mock_check: MagicMock,
    mock_cosign: MagicMock,
) -> None:
    """Pass the per-source DOCKER_CONFIG dir to verify, not just sign."""
    mock_run_cmd.return_value = MagicMock(stdout='{"auths":{}}')
    mock_cosign.return_value = MagicMock(returncode=0, stdout="", stderr="")

    sign_item(
        SignItem("quay.io/pending/repo:t1", "quay.io/pending/repo", "sha256:abc"),
        CONFIG,
        retries=3,
    )

    mock_run_cmd.assert_called_once_with(["select-oci-auth", "quay.io/pending/repo"])
    verify_call_env = mock_check.call_args.kwargs["env"]
    assert "DOCKER_CONFIG" in verify_call_env
    sign_call_env = mock_cosign.call_args.kwargs["env"]
    assert sign_call_env["DOCKER_CONFIG"] == verify_call_env["DOCKER_CONFIG"]
    assert sign_call_env["SIGSTORE_ID_TOKEN"] == str(CONFIG.oidc_token_path)


# --- sign_all ---


@patch(f"{TASK}.sign_item")
@patch(f"{TASK}.memory_throttle")
def test_sign_all_calls_sign_item_for_each(
    _mock_throttle: MagicMock, mock_sign: MagicMock
) -> None:
    """Call sign_item once for each signing candidate."""
    items = [
        SignItem("reg/repo:t1", "quay.io/src", "sha256:a"),
        SignItem("reg/repo:t2", "quay.io/src", "sha256:a"),
    ]

    sign_all(items, CONFIG, retries=3, concurrent_limit=4)

    assert mock_sign.call_count == 2


@patch(f"{TASK}.sign_item")
@patch(f"{TASK}.memory_throttle")
def test_sign_all_raises_on_failure(_mock_throttle: MagicMock, mock_sign: MagicMock) -> None:
    """Raise RuntimeError when any signing job fails."""
    mock_sign.side_effect = RuntimeError("cosign exploded")

    with pytest.raises(RuntimeError, match="signing job"):
        sign_all(
            [SignItem("reg/repo:t1", "quay.io/src", "sha256:a")],
            CONFIG,
            retries=3,
            concurrent_limit=4,
        )


@patch(f"{TASK}.sign_item")
@patch(f"{TASK}.memory_throttle")
def test_sign_all_empty_items_succeeds(
    _mock_throttle: MagicMock, mock_sign: MagicMock
) -> None:
    """Complete an empty candidate list without calling sign_item."""
    sign_all([], CONFIG, retries=3, concurrent_limit=4)

    mock_sign.assert_not_called()


# --- setup_argparser ---


def test_setup_argparser_defaults(tmp_path: Path) -> None:
    """Default values match the bash task's original defaults."""
    snap = tmp_path / "snap.json"
    snap.write_text("{}")

    args = setup_argparser().parse_args(
        [
            "--snapshot",
            str(snap),
            "--oidc-issuer",
            "https://kubernetes.default.svc",
            "--fulcio-url",
            "https://fulcio.example.com",
            "--rekor-url",
            "https://rekor.example.com",
            "--tuf-url",
            "https://tuf.example.com",
        ]
    )

    assert args.snapshot == snap
    assert args.retries == 3
    assert args.concurrent_limit == 90
    assert str(args.oidc_token_path) == "/var/run/secrets/tokens/oidc-token"
    assert args.ca_cert_path == "/mnt/trusted-ca/ca-bundle.crt"


def test_setup_argparser_all_args(tmp_path: Path) -> None:
    """Parse all flags correctly."""
    snap = tmp_path / "snap.json"
    snap.write_text("{}")
    token = tmp_path / "oidc-token"
    token.write_text("x")

    args = setup_argparser().parse_args(
        [
            "--snapshot",
            str(snap),
            "--retries",
            "5",
            "--concurrent-limit",
            "20",
            "--oidc-issuer",
            "https://oidc.example.com",
            "--fulcio-url",
            "https://fulcio.example.com",
            "--rekor-url",
            "https://rekor.example.com",
            "--tuf-url",
            "https://tuf.example.com",
            "--oidc-token-path",
            str(token),
            "--ca-cert-path",
            "/custom/ca.crt",
        ]
    )

    assert args.retries == 5
    assert args.concurrent_limit == 20
    assert args.oidc_token_path == token
    assert args.ca_cert_path == "/custom/ca.crt"


# --- main ---


@patch(f"{TASK}.sign_all")
@patch(f"{TASK}.collect_component_sign_items", return_value=[])
@patch(f"{TASK}.initialize_tuf")
@patch(f"{TASK}.authentication.setup_ca_cert")
def test_main_runs_without_error(
    mock_ca: MagicMock,
    mock_init: MagicMock,
    mock_collect: MagicMock,
    mock_sign_all: MagicMock,
    tmp_path: Path,
) -> None:
    """Return 0 for a snapshot with no components."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": []}))
    token = tmp_path / "oidc-token"
    token.write_text(_jwt_token("default", "default"))

    from release_service_utils.tasks.managed.sign_image_cosign_keyless.sign_image_cosign_keyless import (  # noqa: E501
        main,
    )

    rc = main(
        [
            "--snapshot",
            str(snap),
            "--oidc-issuer",
            "https://kubernetes.default.svc",
            "--fulcio-url",
            "fake-fulcio-server",
            "--rekor-url",
            "fake-rekor-server",
            "--tuf-url",
            "https://tuf.example.com",
            "--oidc-token-path",
            str(token),
        ]
    )

    assert rc == 0
    mock_ca.assert_called_once()
    mock_init.assert_called_once_with("https://tuf.example.com")
    mock_collect.assert_not_called()
    mock_sign_all.assert_called_once()
    signed_items = mock_sign_all.call_args[0][0]
    assert signed_items == []
    config = mock_sign_all.call_args[0][1]
    assert config.certificate_identity == EXPECTED_IDENTITY


@patch(f"{TASK}.sign_all")
@patch(f"{TASK}.initialize_tuf")
@patch(f"{TASK}.authentication.setup_ca_cert")
@patch(f"{TASK}.skopeo")
def test_main_collects_components_from_snapshot(
    mock_skopeo: MagicMock,
    _mock_ca: MagicMock,
    _mock_init: MagicMock,
    mock_sign_all: MagicMock,
    tmp_path: Path,
) -> None:
    """Collect signing items from snapshot components."""
    mock_skopeo.inspect.return_value = MagicMock(
        returncode=0,
        stdout=json.dumps(
            {"mediaType": "application/vnd.docker.distribution.manifest.v2+json"}
        ),
    )
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": [COMPONENT]}))
    token = tmp_path / "oidc-token"
    token.write_text(_jwt_token("default", "default"))

    from release_service_utils.tasks.managed.sign_image_cosign_keyless.sign_image_cosign_keyless import (  # noqa: E501
        main,
    )

    assert (
        main(
            [
                "--snapshot",
                str(snap),
                "--oidc-issuer",
                "https://kubernetes.default.svc",
                "--fulcio-url",
                "fake-fulcio-server",
                "--rekor-url",
                "fake-rekor-server",
                "--tuf-url",
                "https://tuf.example.com",
                "--oidc-token-path",
                str(token),
                "--retries",
                "3",
                "--concurrent-limit",
                "2",
            ]
        )
        == 0
    )

    items = mock_sign_all.call_args[0][0]
    assert len(items) == 2
    assert mock_sign_all.call_args.kwargs["retries"] == 3
    assert mock_sign_all.call_args.kwargs["concurrent_limit"] == 2
