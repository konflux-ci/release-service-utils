"""Unit tests for rh_sign_image_cosign."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.rh_sign_image_cosign import (
    SignItem,
    SigningSecrets,
    check_existing_cosign_signature,
    collect_component_sign_items,
    get_manifest_digests,
    load_signing_secrets,
    run_cosign_with_retry,
    setup_argparser,
    sign_all,
    sign_item,
)

TASK = "release_service_utils.tasks.managed.rh_sign_image_cosign.rh_sign_image_cosign"

SECRETS = SigningSecrets(
    sign_key="awskms:///my-key-arn",
    public_key="-----BEGIN PUBLIC KEY-----\nfake\n-----END PUBLIC KEY-----\n",
    rekor_public_key="-----BEGIN PUBLIC KEY-----\nfake-rekor\n-----END PUBLIC KEY-----\n",
    rekor_url="https://rekor.example.com",
    aws_default_region="us-east-1",
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)

SECRETS_NO_REKOR = SigningSecrets(
    sign_key="awskms:///my-key-arn",
    public_key="-----BEGIN PUBLIC KEY-----\nfake\n-----END PUBLIC KEY-----\n",
    rekor_public_key="",
    rekor_url=None,
    aws_default_region="us-east-1",
    aws_access_key_id="AKID",
    aws_secret_access_key="secret",
)

COMPONENT_RH_REGISTRY = {
    "name": "mycomp",
    "containerImage": "quay.io/internal/myrepo@sha256:top",
    "repositories": [
        {
            "url": "quay.io/internal/myrepo",
            "rh-registry-repo": "registry.redhat.io/myproduct/myrepo",
            "registry-access-repo": "registry.access.redhat.com/myproduct/myrepo",
            "tags": ["v1.0", "latest"],
        }
    ],
}

COMPONENT_EXTERNAL = {
    "name": "extcomp",
    "containerImage": "quay.io/ext/repo@sha256:ext",
    "repositories": [
        {
            "url": "quay.io/ext/repo",
            "tags": ["v2.0"],
        }
    ],
}


# --- load_signing_secrets ---


def test_load_signing_secrets_reads_all_files(tmp_path: Path) -> None:
    """All required secret files are read and the struct is populated."""
    (tmp_path / "SIGN_KEY").write_text("awskms:///key")
    (tmp_path / "PUBLIC_KEY").write_text("pubkey")
    (tmp_path / "REKOR_PUBLIC_KEY").write_text("rekorpubkey")
    (tmp_path / "REKOR_URL").write_text("https://rekor.example.com\n")
    (tmp_path / "AWS_DEFAULT_REGION").write_text("us-east-1")
    (tmp_path / "AWS_ACCESS_KEY_ID").write_text("AKID")
    (tmp_path / "AWS_SECRET_ACCESS_KEY").write_text("secret")

    s = load_signing_secrets(tmp_path)

    assert s.sign_key == "awskms:///key"
    assert s.public_key == "pubkey"
    assert s.rekor_public_key == "rekorpubkey"
    assert s.rekor_url == "https://rekor.example.com"
    assert s.aws_default_region == "us-east-1"
    assert s.aws_access_key_id == "AKID"
    assert s.aws_secret_access_key == "secret"


def test_load_signing_secrets_no_rekor_url(tmp_path: Path) -> None:
    """rekor_url is None when REKOR_URL file is absent."""
    (tmp_path / "SIGN_KEY").write_text("key")
    (tmp_path / "PUBLIC_KEY").write_text("pub")
    (tmp_path / "REKOR_PUBLIC_KEY").write_text("rpub")
    (tmp_path / "AWS_DEFAULT_REGION").write_text("us-east-1")
    (tmp_path / "AWS_ACCESS_KEY_ID").write_text("AKID")
    (tmp_path / "AWS_SECRET_ACCESS_KEY").write_text("sec")

    s = load_signing_secrets(tmp_path)

    assert s.rekor_url is None


def test_load_signing_secrets_empty_rekor_url(tmp_path: Path) -> None:
    """rekor_url is None when REKOR_URL file exists but is empty."""
    for name in (
        "SIGN_KEY",
        "PUBLIC_KEY",
        "REKOR_PUBLIC_KEY",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ):
        (tmp_path / name).write_text("x")
    (tmp_path / "REKOR_URL").write_text("   \n")

    s = load_signing_secrets(tmp_path)

    assert s.rekor_url is None


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


@patch(f"{TASK}.skopeo")
def test_get_manifest_digests_manifest_list(mock_skopeo: MagicMock) -> None:
    """Manifest-list images return nested digests via skopeo inspect --raw."""
    raw = {
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "manifests": [{"digest": "sha256:arm"}, {"digest": "sha256:amd"}],
    }
    mock_skopeo.inspect.return_value = MagicMock(returncode=0, stdout=json.dumps(raw))

    component = {"name": "c", "containerImage": "reg/repo@sha256:top"}
    is_list, digests = get_manifest_digests(component)

    assert is_list is True
    assert digests == ["sha256:top", "sha256:arm", "sha256:amd"]


@patch(f"{TASK}.skopeo")
def test_get_manifest_digests_skopeo_failure_raises(mock_skopeo: MagicMock) -> None:
    """RuntimeError is raised when skopeo inspect fails."""
    mock_skopeo.inspect.return_value = MagicMock(returncode=1, stderr="not found")

    with pytest.raises(RuntimeError, match="skopeo inspect failed"):
        get_manifest_digests({"name": "c", "containerImage": "reg/repo@sha256:x"})


# --- collect_component_sign_items ---


@patch(f"{TASK}.get_manifest_digests", return_value=(False, ["sha256:top"]))
def test_collect_component_single_manifest_rh_registry(mock_digests: MagicMock) -> None:
    """Single-manifest component produces one item per tag for rh-registry-repo."""
    items = collect_component_sign_items(
        COMPONENT_RH_REGISTRY, sign_registry_access_repos=set(), sign_external_registries=False
    )

    assert len(items) == 2
    identities = {i.identity for i in items}
    assert "registry.redhat.io/myproduct/myrepo:v1.0" in identities
    assert "registry.redhat.io/myproduct/myrepo:latest" in identities
    for item in items:
        assert item.source == "quay.io/internal/myrepo"
        assert item.digest == "sha256:top"


@patch(f"{TASK}.get_manifest_digests", return_value=(True, ["sha256:top", "sha256:arm"]))
def test_collect_component_manifest_list_produces_nested_items(
    mock_digests: MagicMock,
) -> None:
    """Manifest-list components produce items for nested digests plus the index."""
    items = collect_component_sign_items(
        COMPONENT_RH_REGISTRY, sign_registry_access_repos=set(), sign_external_registries=False
    )

    digests_seen = {i.digest for i in items}
    assert "sha256:top" in digests_seen
    assert "sha256:arm" in digests_seen


@patch(f"{TASK}.get_manifest_digests", return_value=(False, ["sha256:ext"]))
def test_collect_component_external_registry_skipped_by_default(
    mock_digests: MagicMock,
) -> None:
    """External-registry-only components are skipped when sign_external_registries is False."""
    items = collect_component_sign_items(
        COMPONENT_EXTERNAL, sign_registry_access_repos=set(), sign_external_registries=False
    )
    assert items == []


@patch(f"{TASK}.get_manifest_digests", return_value=(False, ["sha256:ext"]))
def test_collect_component_external_registry_signed_when_enabled(
    mock_digests: MagicMock,
) -> None:
    """External-registry components are signed when sign_external_registries=True."""
    items = collect_component_sign_items(
        COMPONENT_EXTERNAL, sign_registry_access_repos=set(), sign_external_registries=True
    )

    assert len(items) == 1
    assert items[0].identity == "quay.io/ext/repo:v2.0"
    assert items[0].source == "quay.io/ext/repo"


@patch(f"{TASK}.get_manifest_digests", return_value=(False, ["sha256:top"]))
def test_collect_component_registry_access_added_when_in_file(
    mock_digests: MagicMock,
) -> None:
    """registry-access-repo ref is added when the repo path is in sign_registry_access_repos."""  # noqa: E501
    items = collect_component_sign_items(
        COMPONENT_RH_REGISTRY,
        sign_registry_access_repos={"myproduct/myrepo"},
        sign_external_registries=False,
    )

    identities = {i.identity for i in items}
    assert any("registry.access.redhat.com" in id_ for id_ in identities)


@patch(f"{TASK}.get_manifest_digests", return_value=(False, ["sha256:top"]))
def test_collect_component_no_tags_skipped(mock_digests: MagicMock) -> None:
    """Repositories without tags produce no signing items."""
    component = {
        "name": "c",
        "containerImage": "reg/repo@sha256:top",
        "repositories": [{"url": "reg/repo", "rh-registry-repo": "reg/repo", "tags": []}],
    }
    items = collect_component_sign_items(
        component, sign_registry_access_repos=set(), sign_external_registries=False
    )
    assert items == []


# --- run_cosign_with_retry ---


@patch("subprocess.run")
def test_run_cosign_with_retry_success_on_first_attempt(mock_run: MagicMock) -> None:
    """Success on the first attempt returns the CompletedProcess immediately."""
    mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")

    result = run_cosign_with_retry(["cosign", "sign", "--key", "k", "ref"], retries=3)

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
    # Fibonacci: first delay is 3, second is 5
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
def test_check_existing_signature_found(mock_cosign: MagicMock, tmp_path: Path) -> None:
    """Returns True when a matching signature record is found in cosign verify output."""
    sig_output = json.dumps(
        [
            {
                "critical": {
                    "image": {"docker-manifest-digest": "sha256:abc"},
                    "identity": {"docker-reference": "registry.redhat.io/myrepo:v1.0"},
                }
            }
        ]
    )
    mock_cosign.return_value = MagicMock(stdout=sig_output)

    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    found = check_existing_cosign_signature(
        "registry.redhat.io/myrepo:v1.0",
        "quay.io/internal/myrepo",
        "sha256:abc",
        SECRETS_NO_REKOR,
        public_key_path=pub_key,
        rekor_key_path=None,
        retries=3,
        aws_env={},
    )
    assert found is True


@patch(f"{TASK}.run_cosign_with_retry")
def test_check_existing_signature_not_found(mock_cosign: MagicMock, tmp_path: Path) -> None:
    """Returns False when verify output contains no matching signature."""
    mock_cosign.return_value = MagicMock(stdout="[]")

    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    found = check_existing_cosign_signature(
        "registry.redhat.io/myrepo:v1.0",
        "quay.io/internal/myrepo",
        "sha256:abc",
        SECRETS_NO_REKOR,
        public_key_path=pub_key,
        rekor_key_path=None,
        retries=3,
        aws_env={},
    )
    assert found is False


@patch(f"{TASK}.run_cosign_with_retry")
def test_check_existing_signature_verify_failure_raises(
    mock_cosign: MagicMock, tmp_path: Path
) -> None:
    """Propagates the error (does not return False) when cosign verify fails.

    A verification failure must not be treated as "no signature found": doing
    so would let the task add a duplicate signature instead of failing loudly,
    matching the original bash task's ``set -e`` behavior.
    """
    mock_cosign.side_effect = subprocess.CalledProcessError(1, "cosign", stderr="no sig")

    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    with pytest.raises(subprocess.CalledProcessError):
        check_existing_cosign_signature(
            "registry.redhat.io/myrepo:v1.0",
            "quay.io/internal/myrepo",
            "sha256:abc",
            SECRETS_NO_REKOR,
            public_key_path=pub_key,
            rekor_key_path=None,
            retries=3,
            aws_env={},
        )


@patch(f"{TASK}.run_cosign_with_retry")
def test_check_existing_signature_invalid_json_raises(
    mock_cosign: MagicMock, tmp_path: Path
) -> None:
    """Raises ValueError when cosign verify exits 0 but prints invalid JSON.

    Mirrors the release-service-catalog regression test asserting that this
    condition fails the task rather than being silently treated as unsigned.
    """
    mock_cosign.return_value = MagicMock(stdout="not valid json {")

    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    with pytest.raises(ValueError, match="not valid JSON"):
        check_existing_cosign_signature(
            "registry.redhat.io/myrepo:v1.0",
            "quay.io/internal/myrepo",
            "sha256:abc",
            SECRETS_NO_REKOR,
            public_key_path=pub_key,
            rekor_key_path=None,
            retries=3,
            aws_env={},
        )


# --- sign_item ---


@patch(f"{TASK}.run_cosign_with_retry")
@patch(f"{TASK}.check_existing_cosign_signature", return_value=True)
@patch(f"{TASK}.run_cmd")
def test_sign_item_skips_when_already_signed(
    mock_run_cmd: MagicMock,
    mock_check: MagicMock,
    mock_cosign: MagicMock,
    tmp_path: Path,
) -> None:
    """Cosign sign is not called when a signature is already present."""
    mock_run_cmd.return_value = MagicMock(stdout='{"auths":{}}')
    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    sign_item(
        SignItem("reg/repo:v1", "quay.io/repo", "sha256:abc"),
        SECRETS_NO_REKOR,
        public_key_path=pub_key,
        rekor_key_path=None,
        retries=3,
        aws_env={},
    )

    mock_cosign.assert_not_called()


@patch(f"{TASK}.run_cosign_with_retry")
@patch(f"{TASK}.check_existing_cosign_signature", return_value=False)
@patch(f"{TASK}.run_cmd")
def test_sign_item_calls_cosign_when_not_signed(
    mock_run_cmd: MagicMock,
    mock_check: MagicMock,
    mock_cosign: MagicMock,
    tmp_path: Path,
) -> None:
    """Cosign sign is called with the correct identity and reference when unsigned."""
    mock_run_cmd.return_value = MagicMock(stdout='{"auths":{}}')
    mock_cosign.return_value = MagicMock(returncode=0, stdout="", stderr="")
    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    sign_item(
        SignItem("reg/repo:v1", "quay.io/repo", "sha256:abc"),
        SECRETS_NO_REKOR,
        public_key_path=pub_key,
        rekor_key_path=None,
        retries=3,
        aws_env={},
    )

    mock_cosign.assert_called_once()
    sign_args = mock_cosign.call_args[0][0]
    assert "sign" in sign_args
    assert "--sign-container-identity" in sign_args
    assert "reg/repo:v1" in sign_args
    assert "quay.io/repo@sha256:abc" in sign_args


@patch(f"{TASK}.run_cosign_with_retry")
@patch(f"{TASK}.check_existing_cosign_signature", return_value=False)
@patch(f"{TASK}.run_cmd")
def test_sign_item_passes_docker_config_to_verify(
    mock_run_cmd: MagicMock,
    mock_check: MagicMock,
    mock_cosign: MagicMock,
    tmp_path: Path,
) -> None:
    """The per-source DOCKER_CONFIG dir is passed to verify, not just sign.

    cosign has no way to select the right auth entry for a source registry,
    so both the verify and sign calls need the single-entry auth file created
    from select-oci-auth for that source.
    """
    mock_run_cmd.return_value = MagicMock(stdout='{"auths":{}}')
    mock_cosign.return_value = MagicMock(returncode=0, stdout="", stderr="")
    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    sign_item(
        SignItem("reg/repo:v1", "quay.io/repo", "sha256:abc"),
        SECRETS_NO_REKOR,
        public_key_path=pub_key,
        rekor_key_path=None,
        retries=3,
        aws_env={},
    )

    verify_call_env = mock_check.call_args.kwargs["aws_env"]
    assert "DOCKER_CONFIG" in verify_call_env
    sign_call_env = mock_cosign.call_args.kwargs["env"]
    assert sign_call_env["DOCKER_CONFIG"] == verify_call_env["DOCKER_CONFIG"]


@patch(f"{TASK}.run_cosign_with_retry")
@patch(f"{TASK}.check_existing_cosign_signature", return_value=False)
@patch(f"{TASK}.run_cmd")
def test_sign_item_with_rekor(
    mock_run_cmd: MagicMock,
    mock_check: MagicMock,
    mock_cosign: MagicMock,
    tmp_path: Path,
) -> None:
    """Cosign sign includes --rekor-url and sets SIGSTORE_REKOR_PUBLIC_KEY when available."""
    mock_run_cmd.return_value = MagicMock(stdout='{"auths":{}}')
    mock_cosign.return_value = MagicMock(returncode=0, stdout="", stderr="")
    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")
    rekor_key = tmp_path / "rekor.key"
    rekor_key.write_text("rekorkey")

    sign_item(
        SignItem("reg/repo:v1", "quay.io/repo", "sha256:abc"),
        SECRETS,
        public_key_path=pub_key,
        rekor_key_path=rekor_key,
        retries=3,
        aws_env={},
    )

    sign_kwargs = mock_cosign.call_args[1]
    env = sign_kwargs.get("env", {})
    assert "SIGSTORE_REKOR_PUBLIC_KEY" in env
    sign_args = mock_cosign.call_args[0][0]
    assert f"--rekor-url={SECRETS.rekor_url}" in sign_args


# --- sign_all ---


@patch(f"{TASK}.sign_item")
@patch(f"{TASK}.memory_throttle")
def test_sign_all_calls_sign_item_for_each(
    mock_throttle: MagicMock, mock_sign: MagicMock, tmp_path: Path
) -> None:
    """sign_item is called once for each signing candidate."""
    items = [
        SignItem("reg/repo:v1.0", "quay.io/src", "sha256:a"),
        SignItem("reg/repo:latest", "quay.io/src", "sha256:a"),
    ]
    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    sign_all(
        items,
        SECRETS_NO_REKOR,
        public_key_path=pub_key,
        rekor_key_path=None,
        retries=3,
        concurrent_limit=4,
        aws_env={},
    )

    assert mock_sign.call_count == 2


@patch(f"{TASK}.sign_item")
@patch(f"{TASK}.memory_throttle")
def test_sign_all_raises_on_failure(
    mock_throttle: MagicMock, mock_sign: MagicMock, tmp_path: Path
) -> None:
    """RuntimeError is raised when any signing job fails."""
    mock_sign.side_effect = RuntimeError("cosign exploded")
    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    with pytest.raises(RuntimeError, match="signing job"):
        sign_all(
            [SignItem("reg/repo:v1", "quay.io/src", "sha256:a")],
            SECRETS_NO_REKOR,
            public_key_path=pub_key,
            rekor_key_path=None,
            retries=3,
            concurrent_limit=4,
            aws_env={},
        )


@patch(f"{TASK}.sign_item")
@patch(f"{TASK}.memory_throttle")
def test_sign_all_groups_same_source_digest(
    mock_throttle: MagicMock, mock_sign: MagicMock, tmp_path: Path
) -> None:
    """Items with identical source+digest are placed in the same digest group."""
    items = [
        SignItem("reg/repo:v1.0", "quay.io/src", "sha256:a"),
        SignItem("reg/repo:latest", "quay.io/src", "sha256:a"),
        SignItem("reg/other:v1.0", "quay.io/src2", "sha256:b"),
    ]
    pub_key = tmp_path / "pub.key"
    pub_key.write_text("key")

    sign_all(
        items,
        SECRETS_NO_REKOR,
        public_key_path=pub_key,
        rekor_key_path=None,
        retries=3,
        concurrent_limit=8,
        aws_env={},
    )

    assert mock_sign.call_count == 3


# --- setup_argparser ---


def test_setup_argparser_defaults(tmp_path: Path) -> None:
    """Default values match the bash task's original defaults."""
    snap = tmp_path / "snap.json"
    snap.write_text("{}")

    args = setup_argparser().parse_args(["--snapshot", str(snap)])

    assert args.snapshot == snap
    assert args.sign_registry_access_file == ""
    assert args.sign_external_registries == "false"
    assert args.retries == 3
    assert args.concurrent_limit == 90


def test_setup_argparser_all_args(tmp_path: Path) -> None:
    """All flags are parsed correctly."""
    snap = tmp_path / "snap.json"
    snap.write_text("{}")
    access_file = tmp_path / "access.txt"
    access_file.write_text("repo/a\nrepo/b\n")

    args = setup_argparser().parse_args(
        [
            "--snapshot",
            str(snap),
            "--sign-registry-access-file",
            str(access_file),
            "--sign-external-registries",
            "true",
            "--retries",
            "5",
            "--concurrent-limit",
            "20",
        ]
    )

    assert args.sign_registry_access_file == str(access_file)
    assert args.sign_external_registries == "true"
    assert args.retries == 5
    assert args.concurrent_limit == 20


# --- main ---


@patch(f"{TASK}.sign_all")
@patch(f"{TASK}.collect_component_sign_items", return_value=[])
@patch(f"{TASK}.load_signing_secrets", return_value=SECRETS_NO_REKOR)
def test_main_runs_without_error(
    mock_secrets: MagicMock,
    mock_collect: MagicMock,
    mock_sign_all: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() returns 0 for a snapshot with no components."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": []}))
    monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
    import sys

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "rh_sign_image_cosign.py",
            "--snapshot",
            str(snap),
            "--sign-registry-access-file",
            "",
            "--sign-external-registries",
            "false",
        ],
    )

    from release_service_utils.tasks.managed.rh_sign_image_cosign.rh_sign_image_cosign import (
        main,
    )

    assert main() == 0
    mock_sign_all.assert_called_once()
