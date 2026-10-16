"""Test cdn helpers."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest
from release_service_utils.helpers.cdn import cdn


def test_cdn_env_secrets_production() -> None:
    """Production environment returns the public Exodus/Pulp/CGW config."""
    config = cdn.cdn_env_secrets("production")
    assert config["exodusGwSecret"] == "exodus-prod-secret"
    assert config["exodusGwEnv"] == "live"
    assert config["pulpSecret"] == "rhsm-pulp-prod-secret"
    assert config["udcacheSecret"] == "udcache-prod-secret"
    assert "developers.redhat.com" in config["cgwHostname"]
    assert config["cgwSecret"] == "cgw-service-account-prod-secret"


def test_cdn_env_secrets_stage() -> None:
    """Stage environment returns the preprod Exodus/Pulp/CGW config."""
    config = cdn.cdn_env_secrets("stage")
    assert config["exodusGwSecret"] == "exodus-prod-secret"
    assert config["exodusGwEnv"] == "pre"
    assert config["pulpSecret"] == "rhsm-pulp-stage-secret"
    assert config["udcacheSecret"] == "udcache-stage-secret"
    assert "developers.qa.redhat.com" in config["cgwHostname"]
    assert config["cgwSecret"] == "cgw-service-account-stage-secret"


def test_cdn_env_secrets_qa() -> None:
    """QA environment returns the stage-Exodus, qa-Pulp/CGW config."""
    config = cdn.cdn_env_secrets("qa")
    assert config["exodusGwSecret"] == "exodus-stage-secret"
    assert config["exodusGwEnv"] == "live"
    assert config["pulpSecret"] == "rhsm-pulp-qa-secret"
    assert config["udcacheSecret"] == "udcache-qa-secret"
    assert "developers.qa.redhat.com" in config["cgwHostname"]
    assert config["cgwSecret"] == "cgw-service-account-stage-secret"


def test_cdn_env_secrets_invalid_raises() -> None:
    """An unrecognized environment raises ValueError."""
    with pytest.raises(ValueError, match="cdn.env.*must be one of"):
        cdn.cdn_env_secrets("invalid")


def test_cdn_env_secrets_empty_raises() -> None:
    """An empty environment string raises ValueError."""
    with pytest.raises(ValueError, match="cdn.env.*must be one of"):
        cdn.cdn_env_secrets("")


def test_cdn_env_secrets_returns_copy() -> None:
    """Mutating a returned config does not affect subsequent lookups."""
    config1 = cdn.cdn_env_secrets("production")
    config2 = cdn.cdn_env_secrets("production")
    config1["exodusGwSecret"] = "modified"
    assert config2["exodusGwSecret"] == "exodus-prod-secret"


# ---------------------------------------------------------------------------
# create_exodus_conf
# ---------------------------------------------------------------------------


def test_create_exodus_conf_contains_required_fields(tmp_path: Path) -> None:
    """Generated exodus-rsync.conf includes gwcert, gwkey, gwurl, gwenv, and profile."""
    conf = tmp_path / "exodus-rsync.conf"
    cdn.create_exodus_conf(
        conf,
        Path("/tmp/exodus.crt"),
        Path("/tmp/exodus.key"),
        "https://exodus.example.com",
        "live",
    )
    text = conf.read_text()
    assert "gwcert" in text
    assert "gwkey" in text
    assert "gwurl" in text
    assert "gwenv" in text
    assert "live" in text
    assert "exodus" in text


# ---------------------------------------------------------------------------
# push_files_to_cdn
# ---------------------------------------------------------------------------


def test_push_files_to_cdn_calls_rsync(tmp_path: Path) -> None:
    """The rsync command is invoked with a CDN sha256-based destination path per file."""
    content_dir = tmp_path / "content"
    content_dir.mkdir()
    (content_dir / "file.tar.gz").write_bytes(b"archive")

    rsync_calls: list[list[str]] = []

    def fake_check_call(cmd: list[str], **kwargs: object) -> None:
        rsync_calls.append(cmd)

    with mock.patch(
        "release_service_utils.helpers.cdn.cdn.subprocess.check_call",
        side_effect=fake_check_call,
    ):
        cdn.push_files_to_cdn(content_dir, tmp_path / "exodus.conf")

    assert len(rsync_calls) == 1
    dest = rsync_calls[0][-1]
    assert dest.startswith("exodus:/content/origin/files/sha256/")


def test_push_files_to_cdn_excludes_files(tmp_path: Path) -> None:
    """Files listed in the exclude set are skipped."""
    content_dir = tmp_path / "content"
    content_dir.mkdir()
    (content_dir / "keep.tar.gz").write_bytes(b"keep")
    (content_dir / "skip.tar.gz").write_bytes(b"skip")

    rsync_calls: list[list[str]] = []

    def fake_check_call(cmd: list[str], **kwargs: object) -> None:
        rsync_calls.append(cmd)

    with mock.patch(
        "release_service_utils.helpers.cdn.cdn.subprocess.check_call",
        side_effect=fake_check_call,
    ):
        cdn.push_files_to_cdn(content_dir, tmp_path / "exodus.conf", exclude={"skip.tar.gz"})

    assert len(rsync_calls) == 1
    assert "keep.tar.gz" in rsync_calls[0][-1]
