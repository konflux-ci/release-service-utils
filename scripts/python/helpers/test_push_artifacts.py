"""Tests for push_artifacts.py."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest import mock

import pytest

import push_artifacts

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

SNAPSHOT_STAGED = {
    "components": [
        {
            "name": "testproduct",
            "staged": {
                "destination": "testproduct-amd64",
                "version": "1.3",
                "files": [
                    {
                        "source": "/releases/binary-linux-amd64.tar.gz",
                        "os": "linux",
                        "arch": "amd64",
                    }
                ],
            },
        }
    ]
}

SNAPSHOT_CGW_ONLY = {
    "components": [
        {
            "name": "testproduct",
            "contentGateway": {
                "productCode": "Code",
                "productName": "MyName",
                "productVersionName": "1.3-staging",
            },
        }
    ]
}

SNAPSHOT_BOTH = {
    "components": [
        {
            "name": "testproduct",
            "staged": {
                "destination": "testproduct-amd64",
                "version": "1.3",
                "files": [
                    {
                        "source": "/releases/binary-linux-amd64.tar.gz",
                        "os": "linux",
                        "arch": "amd64",
                    }
                ],
            },
            "contentGateway": {
                "productCode": "Code",
                "productName": "MyName",
                "productVersionName": "1.3-staging",
            },
        }
    ]
}


def _setup_secrets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for secret, files in {
        "exodus": {"cert": "CERT", "key": "KEY", "url": "https://exodus.example.com"},
        "pulp": {
            "pulp_url": "https://pulp.example.com",
            "konflux-release-rhsm-pulp.crt": "PULPCERT",
            "konflux-release-rhsm-pulp.key": "PULPKEY",
        },
        "udc": {"url": "https://udc.example.com", "cert": "UDCCERT", "key": "UDCKEY"},
        "cgw": {"username": "cgwuser", "token": "cgwtoken"},
    }.items():
        d = tmp_path / secret
        d.mkdir()
        for fname, content in files.items():
            (d / fname).write_text(content)

    monkeypatch.setattr(push_artifacts, "EXODUS_GW_SECRET_MOUNT", tmp_path / "exodus")
    monkeypatch.setattr(push_artifacts, "PULP_SECRET_MOUNT", tmp_path / "pulp")
    monkeypatch.setattr(push_artifacts, "UDCACHE_SECRET_MOUNT", tmp_path / "udc")
    monkeypatch.setattr(push_artifacts, "CGW_SECRET_MOUNT", tmp_path / "cgw")


def _setup_published_files_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    pub = tmp_path / "published"
    monkeypatch.setenv("RESULT_PUBLISHED_FILES", str(pub))
    return pub


# ---------------------------------------------------------------------------
# _check_cert_expiration
# ---------------------------------------------------------------------------


def test_check_cert_expiration_success() -> None:
    """No exception is raised when openssl reports the certificate has not expired."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.Mock(returncode=0)
        push_artifacts._check_cert_expiration("/mnt/cert", 7)
    mock_run.assert_called_once()


def test_check_cert_expiration_raises_on_failure() -> None:
    """RuntimeError is raised when openssl reports the certificate has expired."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.Mock(returncode=1, stdout="expired", stderr="")
        with pytest.raises(RuntimeError, match="expired"):
            push_artifacts._check_cert_expiration("/mnt/cert", 7)


# ---------------------------------------------------------------------------
# _write_cert_files
# ---------------------------------------------------------------------------


def test_write_cert_files_creates_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All six certificate files are written to disk with the provided PEM content."""
    monkeypatch.chdir(tmp_path)
    with mock.patch("pathlib.Path", wraps=Path):
        ec, ek, pc, pk, uc, uk = push_artifacts._write_cert_files(
            "ECERT", "EKEY", "PCERT", "PKEY", "UCERT", "UKEY"
        )
    assert ec.read_text().strip() == "ECERT"
    assert ek.read_text().strip() == "EKEY"
    assert pc.read_text().strip() == "PCERT"
    assert pk.read_text().strip() == "PKEY"
    assert uc.read_text().strip() == "UCERT"
    assert uk.read_text().strip() == "UKEY"


# ---------------------------------------------------------------------------
# _create_exodus_conf
# ---------------------------------------------------------------------------


def test_create_exodus_conf_contains_required_fields(tmp_path: Path) -> None:
    """Generated exodus-rsync.conf includes gwcert, gwkey, gwurl, gwenv, and profile."""
    conf = tmp_path / "exodus-rsync.conf"
    push_artifacts._create_exodus_conf(
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
# _push_component_to_pulp
# ---------------------------------------------------------------------------


def test_push_component_to_pulp_calls_pulp_push_wrapper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """pulp_push_wrapper.main is called once for a component with staged files."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    comp_dir = tmp_path / "artifacts" / "testproduct" / "ready_for_distribution"
    comp_dir.mkdir(parents=True)
    (comp_dir / "binary-linux-amd64.tar.gz").write_bytes(b"archive")

    with mock.patch("pulp_push_wrapper.main") as mock_pulp:
        push_artifacts._push_component_to_pulp(
            "testproduct",
            SNAPSHOT_STAGED,
            "https://pulp.example.com",
            Path("/tmp/pulp.crt"),
            Path("/tmp/pulp.key"),
            "https://udc.example.com",
        )

    mock_pulp.assert_called_once()


def test_push_component_to_pulp_raises_on_missing_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RuntimeError is raised when staged destination exists but version is missing."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    snapshot = {
        "components": [
            {
                "name": "prod",
                "staged": {"destination": "dest"},  # missing version
            }
        ]
    }
    with pytest.raises(RuntimeError, match="staged.version"):
        push_artifacts._push_component_to_pulp(
            "prod",
            snapshot,
            "https://pulp.example.com",
            Path("/tmp/p.crt"),
            Path("/tmp/p.key"),
            "https://udc.example.com",
        )


def test_push_component_to_pulp_skips_no_staged_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A warning is logged and pulp is skipped when staged has no files list."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    snapshot = {
        "components": [
            {
                "name": "prod",
                "staged": {"destination": "dest", "version": "1.0"},
            }
        ]
    }
    with (
        caplog.at_level(logging.WARNING, logger="push_artifacts"),
        mock.patch("pulp_push_wrapper.main"),
    ):
        push_artifacts._push_component_to_pulp(
            "prod",
            snapshot,
            "https://pulp.example.com",
            Path("/tmp/p.crt"),
            Path("/tmp/p.key"),
            "https://udc.example.com",
        )
    assert "No staged.files" in caplog.text


def test_push_component_to_pulp_handles_windows_zip_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A .tar.gz source with a matching .zip on disk is renamed before the Pulp push."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    comp_dir = tmp_path / "artifacts" / "prod" / "ready_for_distribution"
    comp_dir.mkdir(parents=True)
    (comp_dir / "binary-windows-amd64.zip").write_bytes(b"data")

    snapshot = {
        "components": [
            {
                "name": "prod",
                "staged": {
                    "destination": "dest",
                    "version": "1.0",
                    "files": [
                        {
                            "source": "/releases/binary-windows-amd64.tar.gz",
                            "filename": "binary-windows-amd64.tar.gz",
                        }
                    ],
                },
            }
        ]
    }

    with mock.patch("pulp_push_wrapper.main") as mock_pulp:
        push_artifacts._push_component_to_pulp(
            "prod",
            snapshot,
            "https://pulp.example.com",
            Path("/tmp/p.crt"),
            Path("/tmp/p.key"),
            "https://udc.example.com",
        )

    mock_pulp.assert_called_once()


# ---------------------------------------------------------------------------
# _push_component_to_cdn
# ---------------------------------------------------------------------------


def test_push_component_to_cdn_calls_rsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rsync command is invoked with a CDN sha256-based destination path per archive."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    comp_dir = tmp_path / "artifacts" / "prod" / "ready_for_distribution"
    comp_dir.mkdir(parents=True)
    (comp_dir / "file.tar.gz").write_bytes(b"archive")

    rsync_calls = []

    def fake_check_call(cmd, **kwargs):
        if cmd[0] == "rsync":
            rsync_calls.append(cmd)

    with mock.patch("subprocess.check_call", side_effect=fake_check_call):
        push_artifacts._push_component_to_cdn("prod", tmp_path / "exodus.conf")

    assert len(rsync_calls) == 1
    # Verify CDN path structure: exodus:/content/origin/files/sha256/xx/sha256hash/filename
    dest = rsync_calls[0][-1]
    assert dest.startswith("exodus:/content/origin/files/sha256/")


# ---------------------------------------------------------------------------
# run - routing logic
# ---------------------------------------------------------------------------


def test_run_staged_calls_pulp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() invokes pulp_push_wrapper for a snapshot with staged-only components."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(push_artifacts, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_STAGED))
    _setup_secrets(tmp_path, monkeypatch)
    _setup_published_files_env(tmp_path, monkeypatch)

    comp_dir = tmp_path / "artifacts" / "testproduct" / "ready_for_distribution"
    comp_dir.mkdir(parents=True)
    (comp_dir / "binary-linux-amd64.tar.gz").write_bytes(b"data")

    def fake_check_cert(_path, _days):
        pass

    with (
        mock.patch.object(
            push_artifacts, "_check_cert_expiration", side_effect=fake_check_cert
        ),
        mock.patch("pulp_push_wrapper.main") as mock_pulp,
    ):
        push_artifacts.run("pre", "https://cgw.example.com", 7)

    mock_pulp.assert_called_once()


def test_run_cgw_only_calls_rsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() invokes rsync for a snapshot with CGW-only (no staged) components."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(push_artifacts, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_CGW_ONLY))
    _setup_secrets(tmp_path, monkeypatch)
    _setup_published_files_env(tmp_path, monkeypatch)

    comp_dir = tmp_path / "artifacts" / "testproduct" / "ready_for_distribution"
    comp_dir.mkdir(parents=True)
    (comp_dir / "file.tar.gz").write_bytes(b"data")

    rsync_calls = []

    def fake_check_cert(_path, _days):
        pass

    def fake_check_call(cmd, **kwargs):
        if cmd[0] == "rsync":
            rsync_calls.append(cmd)

    with (
        mock.patch.object(
            push_artifacts, "_check_cert_expiration", side_effect=fake_check_cert
        ),
        mock.patch("subprocess.check_call", side_effect=fake_check_call),
        mock.patch("publish_to_cgw_wrapper.main"),
    ):
        push_artifacts.run("pre", "https://cgw.example.com", 7)

    assert any(c[0] == "rsync" for c in rsync_calls)


def test_run_both_calls_pulp_and_cgw(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() invokes both pulp and CGW wrappers for a snapshot that has both destinations."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(push_artifacts, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_BOTH))
    _setup_secrets(tmp_path, monkeypatch)
    _setup_published_files_env(tmp_path, monkeypatch)

    comp_dir = tmp_path / "artifacts" / "testproduct" / "ready_for_distribution"
    comp_dir.mkdir(parents=True)
    (comp_dir / "binary-linux-amd64.tar.gz").write_bytes(b"data")

    def fake_check_cert(_path, _days):
        pass

    with (
        mock.patch.object(
            push_artifacts, "_check_cert_expiration", side_effect=fake_check_cert
        ),
        mock.patch("pulp_push_wrapper.main") as mock_pulp,
        mock.patch("publish_to_cgw_wrapper.main") as mock_cgw,
        mock.patch("subprocess.check_call"),
    ):
        push_artifacts.run("pre", "https://cgw.example.com", 7)

    mock_pulp.assert_called_once()
    mock_cgw.assert_called_once()


def test_run_uses_shared_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() reads from shared snapshot.json instead of SNAPSHOT_JSON env when present."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(push_artifacts, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "snapshot.json").write_text(json.dumps({"components": []}))
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_STAGED))  # should be ignored
    _setup_secrets(tmp_path, monkeypatch)
    _setup_published_files_env(tmp_path, monkeypatch)

    def fake_check_cert(_path, _days):
        pass

    with mock.patch.object(
        push_artifacts, "_check_cert_expiration", side_effect=fake_check_cert
    ):
        push_artifacts.run("pre", "https://cgw.example.com", 7)
    # No pulp call expected when components list is empty
    # (just verifying no crash)


def test_run_cert_failure_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """RuntimeError from cert expiration check propagates out of run()."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(push_artifacts, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_STAGED))
    _setup_secrets(tmp_path, monkeypatch)
    _setup_published_files_env(tmp_path, monkeypatch)

    with mock.patch.object(
        push_artifacts,
        "_check_cert_expiration",
        side_effect=RuntimeError("cert expired"),
    ):
        with pytest.raises(RuntimeError, match="cert expired"):
            push_artifacts.run("pre", "https://cgw.example.com", 7)


def test_run_preprod_sets_squid_proxy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """HTTP_PROXY is set to the Squid proxy URL when the CGW hostname is a pre-prod URL."""
    monkeypatch.setattr(push_artifacts, "CONTENT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(push_artifacts, "SHARED_DIR", tmp_path / "shared")
    (tmp_path / "shared").mkdir()
    monkeypatch.setenv("SNAPSHOT_JSON", json.dumps(SNAPSHOT_CGW_ONLY))
    _setup_secrets(tmp_path, monkeypatch)
    _setup_published_files_env(tmp_path, monkeypatch)

    comp_dir = tmp_path / "artifacts" / "testproduct" / "ready_for_distribution"
    comp_dir.mkdir(parents=True)
    (comp_dir / "file.tar.gz").write_bytes(b"data")

    def fake_check_cert(_path, _days):
        pass

    env_captured = {}

    def fake_cgw_main(argv):
        import os

        env_captured["HTTP_PROXY"] = os.environ.get("HTTP_PROXY", "")

    with (
        mock.patch.object(
            push_artifacts, "_check_cert_expiration", side_effect=fake_check_cert
        ),
        mock.patch("subprocess.check_call"),
        mock.patch("publish_to_cgw_wrapper.main", side_effect=fake_cgw_main),
    ):
        push_artifacts.run("pre", "https://developers.qa.redhat.com", 7)

    assert "squid" in env_captured.get("HTTP_PROXY", "")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def test_main_success() -> None:
    """main() returns 0 and calls run() with the correct arguments."""
    with mock.patch.object(push_artifacts, "run") as mock_run:
        rc = push_artifacts.main(
            [
                "push_artifacts.py",
                "--exodus-gw-env",
                "pre",
                "--cgw-hostname",
                "https://cgw.example.com",
                "--cert-expiration-warn-days",
                "7",
            ]
        )
    assert rc == 0
    mock_run.assert_called_once_with("pre", "https://cgw.example.com", 7)


def test_main_exception_returns_error() -> None:
    """main() returns 1 when run() raises an exception."""
    with mock.patch.object(push_artifacts, "run", side_effect=RuntimeError("pulp down")):
        rc = push_artifacts.main(
            [
                "push_artifacts.py",
                "--exodus-gw-env",
                "pre",
                "--cgw-hostname",
                "https://cgw.example.com",
            ]
        )
    assert rc == 1
