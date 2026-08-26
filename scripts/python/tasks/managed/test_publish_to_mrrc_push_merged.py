"""Tests for publish_to_mrrc_push_merged."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import publish_to_mrrc_push_merged
import pytest


def _make_result_paths(tmp_path: Path) -> tuple[Path, Path]:
    results = tmp_path / "results"
    results.mkdir()
    return results / "IMAGE_DIGEST", results / "IMAGE_TAG"


def _set_push_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    image: str = "quay.io/konflux-mrrc/mrrc-merge",
    image_expires_after: str | None = "1d",
    work_dir: str = "/workdir/mrrc",
    result_digest: Path,
    result_tag: Path,
) -> None:
    """Set env vars for main()."""
    monkeypatch.setenv("IMAGE", image)
    if image_expires_after is not None:
        monkeypatch.setenv("IMAGE_EXPIRES_AFTER", image_expires_after)
    else:
        monkeypatch.delenv("IMAGE_EXPIRES_AFTER", raising=False)
    monkeypatch.setenv("WORK_DIR", work_dir)
    monkeypatch.setenv("RESULT_IMAGE_DIGEST", str(result_digest))
    monkeypatch.setenv("RESULT_IMAGE_TAG", str(result_tag))


def test_push_merged_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Merged zip is pushed and result files are written."""
    work_dir = tmp_path / "mrrc"
    merge_dir = work_dir / "merged"
    merge_dir.mkdir(parents=True)
    (merge_dir / "merged.zip").write_bytes(b"fake-zip")

    result_digest, result_tag = _make_result_paths(tmp_path)

    calls: list[list[str]] = []

    def fake_check_output(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append([str(x) for x in cmd])
        if cmd[0] == "select-oci-auth":
            return '{"auths":{}}'
        return ""

    monkeypatch.setattr(
        publish_to_mrrc_push_merged.subprocess,
        "check_output",
        fake_check_output,
    )
    monkeypatch.setattr(
        publish_to_mrrc_push_merged.oras_utils,
        "oras_resolve",
        lambda ref: "sha256:abcdef1234567890",
    )
    monkeypatch.setattr(
        publish_to_mrrc_push_merged,
        "generate_tag",
        lambda: "20260716-120000-test-uuid",
    )

    publish_to_mrrc_push_merged.push_merged_maven_repo(
        work_dir=work_dir,
        image="quay.io/konflux-mrrc/mrrc-merge",
        image_expires_after="1d",
        result_image_digest=result_digest,
        result_image_tag=result_tag,
    )

    assert result_digest.read_text(encoding="utf-8") == "sha256:abcdef1234567890"
    assert result_tag.read_text(encoding="utf-8") == "20260716-120000-test-uuid"

    assert calls[0] == ["select-oci-auth", "quay.io/konflux-mrrc/mrrc-merge"]
    push_cmd = calls[1]
    assert push_cmd[0] == "oras"
    assert push_cmd[1] == "push"
    assert "--artifact-type" in push_cmd
    assert "application/vnd.maven+zip" in push_cmd
    assert "quay.expires-after=1d" in " ".join(push_cmd)
    assert push_cmd[-1] == "merged.zip"


def test_push_merged_no_merged_zip(tmp_path: Path) -> None:
    """No-op when merged.zip does not exist."""
    work_dir = tmp_path / "mrrc"
    merge_dir = work_dir / "merged"
    merge_dir.mkdir(parents=True)

    result_digest, result_tag = _make_result_paths(tmp_path)

    publish_to_mrrc_push_merged.push_merged_maven_repo(
        work_dir=work_dir,
        image="quay.io/konflux-mrrc/mrrc-merge",
        image_expires_after="1d",
        result_image_digest=result_digest,
        result_image_tag=result_tag,
    )

    assert not result_digest.exists()
    assert not result_tag.exists()


def test_push_merged_no_expire_annotation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Empty image_expires_after skips the annotation."""
    work_dir = tmp_path / "mrrc"
    merge_dir = work_dir / "merged"
    merge_dir.mkdir(parents=True)
    (merge_dir / "merged.zip").write_bytes(b"fake-zip")

    result_digest, result_tag = _make_result_paths(tmp_path)

    calls: list[list[str]] = []

    def fake_check_output(cmd, **kwargs):  # type: ignore[no-untyped-def]
        calls.append([str(x) for x in cmd])
        if cmd[0] == "select-oci-auth":
            return '{"auths":{}}'
        return ""

    monkeypatch.setattr(
        publish_to_mrrc_push_merged.subprocess,
        "check_output",
        fake_check_output,
    )
    monkeypatch.setattr(
        publish_to_mrrc_push_merged.oras_utils,
        "oras_resolve",
        lambda ref: "sha256:abcdef",
    )
    monkeypatch.setattr(
        publish_to_mrrc_push_merged,
        "generate_tag",
        lambda: "20260716-120000-test-uuid",
    )

    publish_to_mrrc_push_merged.push_merged_maven_repo(
        work_dir=work_dir,
        image="quay.io/konflux-mrrc/mrrc-merge",
        image_expires_after="",
        result_image_digest=result_digest,
        result_image_tag=result_tag,
    )

    push_cmd = calls[1]
    assert "--annotation" not in push_cmd


def test_push_merged_oras_push_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Push failure from oras propagates."""
    work_dir = tmp_path / "mrrc"
    merge_dir = work_dir / "merged"
    merge_dir.mkdir(parents=True)
    (merge_dir / "merged.zip").write_bytes(b"fake-zip")

    result_digest, result_tag = _make_result_paths(tmp_path)

    def fake_check_output(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return '{"auths":{}}'
        raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(
        publish_to_mrrc_push_merged.subprocess,
        "check_output",
        fake_check_output,
    )
    monkeypatch.setattr(
        publish_to_mrrc_push_merged,
        "generate_tag",
        lambda: "20260716-120000-test-uuid",
    )

    with pytest.raises(subprocess.CalledProcessError):
        publish_to_mrrc_push_merged.push_merged_maven_repo(
            work_dir=work_dir,
            image="quay.io/konflux-mrrc/mrrc-merge",
            image_expires_after="1d",
            result_image_digest=result_digest,
            result_image_tag=result_tag,
        )


def test_push_merged_oras_resolve_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolve failure from oras propagates."""
    work_dir = tmp_path / "mrrc"
    merge_dir = work_dir / "merged"
    merge_dir.mkdir(parents=True)
    (merge_dir / "merged.zip").write_bytes(b"fake-zip")

    result_digest, result_tag = _make_result_paths(tmp_path)

    def fake_check_output(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[0] == "select-oci-auth":
            return '{"auths":{}}'
        return ""

    monkeypatch.setattr(
        publish_to_mrrc_push_merged.subprocess,
        "check_output",
        fake_check_output,
    )

    def fail_resolve(ref: str) -> str:
        raise RuntimeError("oras resolve failed")

    monkeypatch.setattr(publish_to_mrrc_push_merged.oras_utils, "oras_resolve", fail_resolve)
    monkeypatch.setattr(
        publish_to_mrrc_push_merged,
        "generate_tag",
        lambda: "20260716-120000-test-uuid",
    )

    with pytest.raises(RuntimeError, match="oras resolve failed"):
        publish_to_mrrc_push_merged.push_merged_maven_repo(
            work_dir=work_dir,
            image="quay.io/konflux-mrrc/mrrc-merge",
            image_expires_after="1d",
            result_image_digest=result_digest,
            result_image_tag=result_tag,
        )


def test_generate_tag_format() -> None:
    """Tag looks like YYYYMMDD-HHMMSS-uuid4."""
    tag = publish_to_mrrc_push_merged.generate_tag()
    pattern = r"^\d{8}-\d{6}-" r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    assert re.match(pattern, tag), f"Tag {tag!r} does not match expected format"


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() returns 0 on success."""
    result_digest, result_tag = _make_result_paths(tmp_path)
    _set_push_env(
        monkeypatch,
        work_dir=str(tmp_path / "mrrc"),
        result_digest=result_digest,
        result_tag=result_tag,
    )
    monkeypatch.setattr(
        publish_to_mrrc_push_merged,
        "push_merged_maven_repo",
        lambda **_: None,
    )
    assert publish_to_mrrc_push_merged.main() == 0


def test_main_missing_image_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing IMAGE exits with code 1."""
    monkeypatch.delenv("IMAGE", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        publish_to_mrrc_push_merged.main()
    assert exc_info.value.code == 1


def test_main_missing_result_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing result env vars exits with code 1."""
    monkeypatch.setenv("IMAGE", "quay.io/konflux-mrrc/mrrc-merge")
    monkeypatch.delenv("RESULT_IMAGE_DIGEST", raising=False)
    monkeypatch.delenv("RESULT_IMAGE_TAG", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        publish_to_mrrc_push_merged.main()
    assert exc_info.value.code == 1


def test_main_image_expires_after_defaults_to_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unset IMAGE_EXPIRES_AFTER defaults to empty string."""
    result_digest, result_tag = _make_result_paths(tmp_path)
    _set_push_env(
        monkeypatch,
        image_expires_after=None,
        work_dir=str(tmp_path / "mrrc"),
        result_digest=result_digest,
        result_tag=result_tag,
    )
    captured_kwargs: dict[str, object] = {}

    def capture(**kwargs: object) -> None:
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(
        publish_to_mrrc_push_merged,
        "push_merged_maven_repo",
        capture,
    )
    publish_to_mrrc_push_merged.main()
    assert captured_kwargs["image_expires_after"] == ""


def test_main_image_expires_after_strips_whitespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Whitespace-only IMAGE_EXPIRES_AFTER is stripped to empty."""
    result_digest, result_tag = _make_result_paths(tmp_path)
    _set_push_env(
        monkeypatch,
        image_expires_after="   ",
        work_dir=str(tmp_path / "mrrc"),
        result_digest=result_digest,
        result_tag=result_tag,
    )
    captured_kwargs: dict[str, object] = {}

    def capture(**kwargs: object) -> None:
        captured_kwargs.update(kwargs)

    monkeypatch.setattr(
        publish_to_mrrc_push_merged,
        "push_merged_maven_repo",
        capture,
    )
    publish_to_mrrc_push_merged.main()
    assert captured_kwargs["image_expires_after"] == ""
