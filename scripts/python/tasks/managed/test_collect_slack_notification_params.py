"""Tests for collect_slack_notification_params."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from collect_slack_notification_params import (
    ReleaseMetadata,
    _write_empty,
    build_slack_message,
    build_urls,
    collect_params,
    extract_release_metadata,
    extract_slack_keyname,
    extract_slack_secret,
    main,
)


def _write_json(path: Path, data: dict) -> None:
    """Write a dict as JSON to path, creating parent dirs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _sample_data() -> dict:
    """Return a data dict with full slack configuration."""
    return {
        "slack": {
            "slack-notification-secret": "my-slack-secret",
            "slack-webhook-notification-secret-keyname": "webhook-key",
        },
    }


def _sample_release() -> dict:
    """Return a release dict with standard metadata."""
    return {
        "metadata": {
            "namespace": "my-workspace-tenant",
            "name": "my-release",
        },
        "status": {
            "target": "target-workspace-tenant",
            "managedProcessing": {
                "pipelineRun": "my-pipeline-run",
            },
        },
    }


def _sample_snapshot() -> dict:
    """Return a snapshot dict with componentGroup."""
    return {"componentGroup": "my-app"}


# -- extract_slack_secret --


def test_extract_slack_secret_present() -> None:
    """Return the secret value when present."""
    data = {"slack": {"slack-notification-secret": "my-secret"}}
    assert extract_slack_secret(data) == "my-secret"


def test_extract_slack_secret_missing_key() -> None:
    """Return None when the key is absent from the slack block."""
    assert extract_slack_secret({"slack": {}}) is None


def test_extract_slack_secret_no_slack_block() -> None:
    """Return None when there is no slack block."""
    assert extract_slack_secret({"other": "data"}) is None


def test_extract_slack_secret_slack_not_dict() -> None:
    """Return None when slack is not a dict."""
    assert extract_slack_secret({"slack": "invalid"}) is None


# -- extract_slack_keyname --


def test_extract_slack_keyname_present() -> None:
    """Return the keyname value when present."""
    data = {
        "slack": {
            "slack-webhook-notification-secret-keyname": "wh-key",
        }
    }
    assert extract_slack_keyname(data) == "wh-key"


def test_extract_slack_keyname_missing() -> None:
    """Return None when the key is absent."""
    assert extract_slack_keyname({"slack": {}}) is None


def test_extract_slack_keyname_no_slack_block() -> None:
    """Return None when there is no slack block."""
    assert extract_slack_keyname({}) is None


# -- extract_release_metadata --


def test_extract_release_metadata_full() -> None:
    """Extract all fields and remove -tenant suffix."""
    meta = extract_release_metadata(_sample_release())

    assert meta.origin_workspace == "my-workspace"
    assert meta.target_workspace == "target-workspace"
    assert meta.release_name == "my-release"
    assert meta.release_pipeline_name == "my-pipeline-run"


def test_extract_release_metadata_missing_fields() -> None:
    """Return empty strings when nested fields are absent."""
    meta = extract_release_metadata({})

    assert meta.origin_workspace == ""
    assert meta.target_workspace == ""
    assert meta.release_name == ""
    assert meta.release_pipeline_name == ""


# -- build_urls --


def test_build_urls() -> None:
    """Build correct release and pipeline-run URLs."""
    meta = ReleaseMetadata(
        origin_workspace="origin-ws",
        target_workspace="target-ws",
        release_name="rel-1",
        release_pipeline_name="plr-1",
    )
    release_url, plr_url = build_urls("https://hac.example", meta, "app")

    assert release_url == ("https://hac.example/origin-ws/applications" "/app/releases/rel-1")
    assert plr_url == ("https://hac.example/target-ws/applications" "/app/pipelineruns/plr-1")


# -- build_slack_message --


def test_build_slack_message_valid_json() -> None:
    """Output is valid JSON with expected block structure."""
    meta = ReleaseMetadata("ws", "tws", "rel", "plr")
    raw = build_slack_message(meta, "app", "http://r", "http://p")
    parsed = json.loads(raw)

    assert "blocks" in parsed
    types = [b["type"] for b in parsed["blocks"]]
    assert "header" in types
    assert "divider" in types
    assert "section" in types
    assert "rich_text" in types


def test_build_slack_message_contains_placeholders() -> None:
    """Output contains status placeholders for downstream replacement."""
    meta = ReleaseMetadata("ws", "tws", "rel", "plr")
    raw = build_slack_message(meta, "app", "http://r", "http://p")

    assert "@@CIRCLE_TYPE@@" in raw
    assert "@@STATUS_TEXT@@" in raw


def test_build_slack_message_contains_release_info() -> None:
    """Output contains workspace, component group, and release name."""
    meta = ReleaseMetadata("my-ws", "tgt-ws", "rel-42", "plr-7")
    raw = build_slack_message(meta, "my-comp", "http://release", "http://plr")

    assert "my-ws/my-comp/rel-42" in raw
    assert "http://release" in raw
    assert "http://plr" in raw


# -- _write_empty --


def test_write_empty(tmp_path: Path) -> None:
    """Write empty strings to all three result files."""
    msg = tmp_path / "msg"
    secret = tmp_path / "secret"
    keyname = tmp_path / "keyname"

    _write_empty(msg, secret, keyname)

    assert msg.read_text() == ""
    assert secret.read_text() == ""
    assert keyname.read_text() == ""


# -- collect_params --


def test_collect_params_happy_path(tmp_path: Path) -> None:
    """Write secret, keyname, and message when all config is present."""
    data_file = tmp_path / "data.json"
    snapshot_file = tmp_path / "snapshot.json"
    release_file = tmp_path / "release.json"
    _write_json(data_file, _sample_data())
    _write_json(snapshot_file, _sample_snapshot())
    _write_json(release_file, _sample_release())

    r_msg = tmp_path / "r_msg"
    r_secret = tmp_path / "r_secret"
    r_keyname = tmp_path / "r_keyname"

    rc = collect_params(
        data_file=data_file,
        snapshot_file=snapshot_file,
        release_file=release_file,
        hac_url="https://hac.example",
        result_message=r_msg,
        result_secret=r_secret,
        result_keyname=r_keyname,
    )

    assert rc == 0
    assert r_secret.read_text() == "my-slack-secret"
    assert r_keyname.read_text() == "webhook-key"

    message = json.loads(r_msg.read_text())
    assert "blocks" in message


def test_collect_params_no_slack_secret(tmp_path: Path) -> None:
    """Write empty results when slack secret is absent."""
    data_file = tmp_path / "data.json"
    snapshot_file = tmp_path / "snapshot.json"
    release_file = tmp_path / "release.json"
    _write_json(data_file, {"other": "data"})
    _write_json(snapshot_file, _sample_snapshot())
    _write_json(release_file, _sample_release())

    r_msg = tmp_path / "r_msg"
    r_secret = tmp_path / "r_secret"
    r_keyname = tmp_path / "r_keyname"

    rc = collect_params(
        data_file=data_file,
        snapshot_file=snapshot_file,
        release_file=release_file,
        hac_url="https://hac.example",
        result_message=r_msg,
        result_secret=r_secret,
        result_keyname=r_keyname,
    )

    assert rc == 0
    assert r_msg.read_text() == ""
    assert r_secret.read_text() == ""
    assert r_keyname.read_text() == ""


def test_collect_params_no_slack_keyname(tmp_path: Path) -> None:
    """Write secret but empty message and keyname when keyname is absent."""
    data = {
        "slack": {
            "slack-notification-secret": "the-secret",
        }
    }
    data_file = tmp_path / "data.json"
    snapshot_file = tmp_path / "snapshot.json"
    release_file = tmp_path / "release.json"
    _write_json(data_file, data)
    _write_json(snapshot_file, _sample_snapshot())
    _write_json(release_file, _sample_release())

    r_msg = tmp_path / "r_msg"
    r_secret = tmp_path / "r_secret"
    r_keyname = tmp_path / "r_keyname"

    rc = collect_params(
        data_file=data_file,
        snapshot_file=snapshot_file,
        release_file=release_file,
        hac_url="https://hac.example",
        result_message=r_msg,
        result_secret=r_secret,
        result_keyname=r_keyname,
    )

    assert rc == 0
    assert r_msg.read_text() == ""
    assert r_keyname.read_text() == ""


def test_collect_params_missing_input_file(tmp_path: Path) -> None:
    """Raise FileNotFoundError when an input file is missing."""
    r_msg = tmp_path / "r_msg"
    r_secret = tmp_path / "r_secret"
    r_keyname = tmp_path / "r_keyname"

    with pytest.raises(FileNotFoundError):
        collect_params(
            data_file=tmp_path / "missing.json",
            snapshot_file=tmp_path / "snapshot.json",
            release_file=tmp_path / "release.json",
            hac_url="https://hac.example",
            result_message=r_msg,
            result_secret=r_secret,
            result_keyname=r_keyname,
        )


# -- main --


def test_main_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return 0 and write all result files when fully configured."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_json(data_dir / "data.json", _sample_data())
    _write_json(data_dir / "snapshot.json", _sample_snapshot())
    _write_json(data_dir / "release.json", _sample_release())

    r_msg = tmp_path / "r_msg"
    r_secret = tmp_path / "r_secret"
    r_keyname = tmp_path / "r_keyname"

    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("DATA_PATH", "data.json")
    monkeypatch.setenv("SNAPSHOT_PATH", "snapshot.json")
    monkeypatch.setenv("RELEASE_PATH", "release.json")
    monkeypatch.setenv("HAC_URL", "https://hac.example")
    monkeypatch.setenv("RESULT_MESSAGE", str(r_msg))
    monkeypatch.setenv("RESULT_SLACK_NOTIFICATION_SECRET", str(r_secret))
    monkeypatch.setenv("RESULT_SLACK_NOTIFICATION_SECRET_KEYNAME", str(r_keyname))

    rc = main()

    assert rc == 0
    assert r_secret.read_text() == "my-slack-secret"
    assert r_keyname.read_text() == "webhook-key"
    assert json.loads(r_msg.read_text())["blocks"]


def test_main_missing_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exit with SystemExit when a required env var is missing."""
    monkeypatch.delenv("DATA_DIR", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_main_missing_result_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit with SystemExit when a result path env var is missing."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATA_PATH", "data.json")
    monkeypatch.setenv("SNAPSHOT_PATH", "snapshot.json")
    monkeypatch.setenv("RELEASE_PATH", "release.json")
    monkeypatch.setenv("HAC_URL", "https://hac.example")
    monkeypatch.delenv("RESULT_MESSAGE", raising=False)
    monkeypatch.delenv("RESULT_SLACK_NOTIFICATION_SECRET", raising=False)
    monkeypatch.delenv("RESULT_SLACK_NOTIFICATION_SECRET_KEYNAME", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_main_missing_hac_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit with SystemExit when HAC_URL is not set."""
    monkeypatch.delenv("HAC_URL", raising=False)
    monkeypatch.delenv("DATA_DIR", raising=False)

    with pytest.raises(SystemExit):
        main()
