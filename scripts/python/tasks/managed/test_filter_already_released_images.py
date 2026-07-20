"""Test filtering of already-released images from a snapshot."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import filter_already_released_images
import pytest


def _write_snapshot(path: Path, components: list[dict]) -> None:
    """Write a minimal snapshot JSON file."""
    path.write_text(
        json.dumps({"application": "test", "components": components}),
        encoding="utf-8",
    )


def _component(
    name: str,
    image: str,
    repositories: list[dict] | None = None,
) -> dict:
    """Build a component dict."""
    c: dict = {"name": name, "containerImage": image}
    if repositories is not None:
        c["repositories"] = repositories
    return c


class TestCheckTag:
    """Test _check_tag which verifies a single tag in a target repo."""

    def test_matching_digest_returns_true(self) -> None:
        """Tag resolving to expected digest returns True."""
        with patch(
            "filter_already_released_images.oras_resolve",
            return_value="sha256:abc",
        ):
            assert filter_already_released_images._check_tag("reg.io/repo", "v1", "sha256:abc")

    def test_mismatched_digest_returns_false(self) -> None:
        """Tag resolving to wrong digest returns False."""
        with patch(
            "filter_already_released_images.oras_resolve",
            return_value="sha256:wrong",
        ):
            assert not filter_already_released_images._check_tag(
                "reg.io/repo", "v1", "sha256:abc"
            )

    def test_resolve_failure_returns_false(self) -> None:
        """Tag that cannot be resolved returns False."""
        with patch(
            "filter_already_released_images.oras_resolve",
            return_value=None,
        ):
            assert not filter_already_released_images._check_tag(
                "reg.io/repo", "v1", "sha256:abc"
            )

    def test_passes_repo_url_as_auth_ref(self) -> None:
        """_oras_resolve is called with repo_url as auth_ref."""
        with patch(
            "filter_already_released_images.oras_resolve",
            return_value="sha256:abc",
        ) as mock_resolve:
            filter_already_released_images._check_tag("reg.io/repo", "v1", "sha256:abc")
            mock_resolve.assert_called_once_with(
                "reg.io/repo:v1", auth_ref="reg.io/repo", check=False
            )


class TestIsComponentReleased:
    """Test is_component_released logic."""

    def test_all_tags_match_returns_true(self) -> None:
        """Component is released when all tags in a repo match."""
        comp = _component(
            "c1",
            "reg.io/img@sha256:abc",
            [{"url": "reg.io/target", "tags": ["v1", "latest"]}],
        )
        with patch(
            "filter_already_released_images._check_tag",
            return_value=True,
        ):
            assert filter_already_released_images.is_component_released(comp, "sha256:abc")

    def test_partial_tags_returns_false(self) -> None:
        """Component is not released when some tags are missing."""
        comp = _component(
            "c1",
            "reg.io/img@sha256:abc",
            [{"url": "reg.io/target", "tags": ["v1", "latest"]}],
        )
        with patch(
            "filter_already_released_images._check_tag",
            side_effect=[True, False],
        ):
            assert not filter_already_released_images.is_component_released(comp, "sha256:abc")

    def test_no_repositories_returns_false(self) -> None:
        """Component with no repositories is not considered released."""
        comp = _component("c1", "reg.io/img@sha256:abc", [])
        assert not filter_already_released_images.is_component_released(comp, "sha256:abc")

    def test_none_repositories_returns_false(self) -> None:
        """Component without repositories key returns False."""
        comp = _component("c1", "reg.io/img@sha256:abc")
        assert not filter_already_released_images.is_component_released(comp, "sha256:abc")

    def test_empty_url_skipped(self) -> None:
        """Repository with empty URL is skipped."""
        comp = _component(
            "c1",
            "reg.io/img@sha256:abc",
            [{"url": "", "tags": ["v1"]}],
        )
        assert not filter_already_released_images.is_component_released(comp, "sha256:abc")

    def test_no_tags_skipped(self) -> None:
        """Repository with no tags is skipped."""
        comp = _component(
            "c1",
            "reg.io/img@sha256:abc",
            [{"url": "reg.io/target", "tags": []}],
        )
        assert not filter_already_released_images.is_component_released(comp, "sha256:abc")

    def test_any_repo_logic_first_incomplete_second_complete(self) -> None:
        """Component released if ANY repository has all tags complete."""
        comp = _component(
            "c1",
            "reg.io/img@sha256:abc",
            [
                {"url": "staging.io/target", "tags": ["v1"]},
                {"url": "prod.io/target", "tags": ["v1"]},
            ],
        )
        with patch(
            "filter_already_released_images._check_tag",
            side_effect=[False, True],
        ):
            assert filter_already_released_images.is_component_released(comp, "sha256:abc")

    def test_first_complete_skips_remaining_repos(self) -> None:
        """Early return when first repository has all tags complete."""
        comp = _component(
            "c1",
            "reg.io/img@sha256:abc",
            [
                {"url": "first.io/target", "tags": ["v1"]},
                {"url": "second.io/target", "tags": ["v1"]},
            ],
        )
        with patch(
            "filter_already_released_images._check_tag",
            return_value=True,
        ) as mock_check:
            assert filter_already_released_images.is_component_released(comp, "sha256:abc")
        mock_check.assert_called_once_with("first.io/target", "v1", "sha256:abc")

    def test_all_repos_incomplete_returns_false(self) -> None:
        """Component not released if no repository has all tags."""
        comp = _component(
            "c1",
            "reg.io/img@sha256:abc",
            [
                {"url": "staging.io/t1", "tags": ["v1"]},
                {"url": "prod.io/t2", "tags": ["v1"]},
            ],
        )
        with patch(
            "filter_already_released_images._check_tag",
            return_value=False,
        ):
            assert not filter_already_released_images.is_component_released(comp, "sha256:abc")


class TestPartitionComponents:
    """Test _partition_components logic (core filtering without I/O)."""

    def test_all_released(self) -> None:
        """All components filtered when fully released."""
        components = [
            _component("c1", "r.io/i@sha256:a1", [{"url": "r.io/t1", "tags": ["v1"]}]),
        ]
        with (
            patch(
                "filter_already_released_images.oras_resolve",
                return_value="sha256:a1",
            ),
            patch(
                "filter_already_released_images.is_component_released",
                return_value=True,
            ),
        ):
            kept, filtered = filter_already_released_images._partition_components(components)
        assert filtered == 1
        assert kept == []

    def test_none_released(self) -> None:
        """No components filtered when none are released."""
        components = [
            _component("c1", "r.io/i@sha256:a1", [{"url": "r.io/t1", "tags": ["v1"]}]),
        ]
        with (
            patch(
                "filter_already_released_images.oras_resolve",
                return_value="sha256:a1",
            ),
            patch(
                "filter_already_released_images.is_component_released",
                return_value=False,
            ),
        ):
            kept, filtered = filter_already_released_images._partition_components(components)
        assert filtered == 0
        assert len(kept) == 1

    def test_some_released(self) -> None:
        """Only released components are filtered out."""
        components = [
            _component("released", "r.io/i@sha256:r1", [{"url": "r.io/t1", "tags": ["v1"]}]),
            _component("kept", "r.io/i@sha256:k1", [{"url": "r.io/t2", "tags": ["v1"]}]),
        ]
        with (
            patch(
                "filter_already_released_images.oras_resolve",
                side_effect=["sha256:r1", "sha256:k1"],
            ),
            patch(
                "filter_already_released_images.is_component_released",
                side_effect=[True, False],
            ),
        ):
            kept, filtered = filter_already_released_images._partition_components(components)
        assert filtered == 1
        assert len(kept) == 1
        assert kept[0]["name"] == "kept"

    def test_unresolvable_image_kept(self) -> None:
        """Component whose image cannot be resolved is kept."""
        components = [
            _component("c1", "r.io/i@sha256:bad", [{"url": "r.io/t1", "tags": ["v1"]}])
        ]
        with patch(
            "filter_already_released_images.oras_resolve",
            return_value=None,
        ):
            kept, filtered = filter_already_released_images._partition_components(components)
        assert filtered == 0
        assert len(kept) == 1

    def test_no_repositories_kept(self) -> None:
        """Component with no repositories is kept (is_component_released returns False)."""
        components = [_component("c1", "r.io/i@sha256:a1", [])]
        with patch(
            "filter_already_released_images.oras_resolve",
            return_value="sha256:a1",
        ):
            kept, filtered = filter_already_released_images._partition_components(components)
        assert filtered == 0
        assert len(kept) == 1

    def test_empty_components(self) -> None:
        """Empty components list produces zero counts."""
        kept, filtered = filter_already_released_images._partition_components([])
        assert filtered == 0
        assert kept == []

    def test_missing_repositories_key_kept(self) -> None:
        """Component without a repositories key is kept."""
        components = [_component("c1", "r.io/i@sha256:a1")]
        with patch(
            "filter_already_released_images.oras_resolve",
            return_value="sha256:a1",
        ):
            kept, filtered = filter_already_released_images._partition_components(components)
        assert filtered == 0
        assert len(kept) == 1


class TestFilterSnapshot:
    """Test filter_snapshot file I/O and integration with _partition_components."""

    def test_overwrites_snapshot_in_place(self, tmp_path: Path) -> None:
        """Snapshot file is overwritten with only kept components."""
        snap = tmp_path / "snap.json"
        _write_snapshot(
            snap,
            [
                _component(
                    "released",
                    "r.io/i@sha256:r1",
                    [{"url": "r.io/t1", "tags": ["v1"]}],
                ),
                _component(
                    "kept",
                    "r.io/i@sha256:k1",
                    [{"url": "r.io/t2", "tags": ["v1"]}],
                ),
            ],
        )
        kept_comps = [
            _component("kept", "r.io/i@sha256:k1", [{"url": "r.io/t2", "tags": ["v1"]}])
        ]
        with patch(
            "filter_already_released_images._partition_components",
            return_value=(kept_comps, 1),
        ):
            total, filtered = filter_already_released_images.filter_snapshot(snap)

        assert total == 2
        assert filtered == 1
        result = json.loads(snap.read_text(encoding="utf-8"))
        assert len(result["components"]) == 1
        assert result["components"][0]["name"] == "kept"

    def test_empty_snapshot(self, tmp_path: Path) -> None:
        """Empty components list produces zero counts."""
        snap = tmp_path / "snap.json"
        _write_snapshot(snap, [])
        with patch(
            "filter_already_released_images._partition_components",
            return_value=([], 0),
        ):
            total, filtered = filter_already_released_images.filter_snapshot(snap)
        assert total == 0
        assert filtered == 0

    def test_preserves_non_component_keys(self, tmp_path: Path) -> None:
        """Non-component keys in the snapshot survive the round-trip."""
        snap = tmp_path / "snap.json"
        snap.write_text(
            json.dumps(
                {
                    "application": "my-app",
                    "customField": {"nested": True},
                    "components": [
                        _component(
                            "c1", "r.io/i@sha256:a1", [{"url": "r.io/t", "tags": ["v1"]}]
                        )
                    ],
                }
            ),
            encoding="utf-8",
        )
        kept_comps = [
            _component("c1", "r.io/i@sha256:a1", [{"url": "r.io/t", "tags": ["v1"]}])
        ]
        with patch(
            "filter_already_released_images._partition_components",
            return_value=(kept_comps, 0),
        ):
            filter_already_released_images.filter_snapshot(snap)

        result = json.loads(snap.read_text(encoding="utf-8"))
        assert result["application"] == "my-app"
        assert result["customField"] == {"nested": True}
        assert len(result["components"]) == 1


class TestRun:
    """Test the run() orchestration."""

    def test_success(self, tmp_path: Path) -> None:
        """Run writes 'false' when not all components are filtered."""
        snap = tmp_path / "snap.json"
        result_file = tmp_path / "skip_release"
        _write_snapshot(
            snap,
            [
                _component(
                    "c1",
                    "r.io/i@sha256:a1",
                    [{"url": "r.io/t1", "tags": ["v1"]}],
                ),
            ],
        )
        with patch(
            "filter_already_released_images.filter_snapshot",
            return_value=(1, 0),
        ):
            filter_already_released_images.run(snap, result_file)
        assert result_file.read_text(encoding="utf-8") == "false"

    def test_all_filtered_writes_true(self, tmp_path: Path) -> None:
        """Run writes 'true' when all components are filtered."""
        snap = tmp_path / "snap.json"
        result_file = tmp_path / "skip_release"
        _write_snapshot(snap, [_component("c1", "r.io/i@sha256:a1")])
        with patch(
            "filter_already_released_images.filter_snapshot",
            return_value=(2, 2),
        ):
            filter_already_released_images.run(snap, result_file)
        assert result_file.read_text(encoding="utf-8") == "true"

    def test_empty_snapshot_writes_false(self, tmp_path: Path) -> None:
        """Run writes 'false' when snapshot has zero components."""
        snap = tmp_path / "snap.json"
        result_file = tmp_path / "skip_release"
        _write_snapshot(snap, [])
        with patch(
            "filter_already_released_images.filter_snapshot",
            return_value=(0, 0),
        ):
            filter_already_released_images.run(snap, result_file)
        assert result_file.read_text(encoding="utf-8") == "false"

    def test_missing_snapshot_raises(self, tmp_path: Path) -> None:
        """RuntimeError when snapshot file does not exist."""
        result_file = tmp_path / "skip_release"
        with pytest.raises(RuntimeError, match="Snapshot file not found"):
            filter_already_released_images.run(tmp_path / "missing.json", result_file)


class TestParseArgs:
    """Test CLI argument parsing."""

    def test_snapshot_path_required(self) -> None:
        """--snapshot-path is required."""
        with pytest.raises(SystemExit):
            filter_already_released_images._parse_args([])

    def test_parses_snapshot_path(self) -> None:
        """--snapshot-path is correctly parsed."""
        args = filter_already_released_images._parse_args(
            ["--snapshot-path", "/data/snap.json"]
        )
        assert args.snapshot_path == "/data/snap.json"


class TestMain:
    """Test the main() entry point."""

    def test_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return 0 on successful run."""
        snap = tmp_path / "snap.json"
        result_file = tmp_path / "skip_release"
        _write_snapshot(snap, [])
        monkeypatch.setenv("RESULT_SKIP_RELEASE", str(result_file))
        with patch(
            "filter_already_released_images.filter_snapshot",
            return_value=(0, 0),
        ):
            assert filter_already_released_images.main(["--snapshot-path", str(snap)]) == 0

    def test_missing_env_var_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SystemExit when RESULT_SKIP_RELEASE is not set."""
        monkeypatch.delenv("RESULT_SKIP_RELEASE", raising=False)
        with pytest.raises(SystemExit):
            filter_already_released_images.main(["--snapshot-path", "/tmp/s.json"])

    def test_runtime_error_propagates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """RuntimeError from run() propagates."""
        result_file = tmp_path / "skip_release"
        monkeypatch.setenv("RESULT_SKIP_RELEASE", str(result_file))
        with pytest.raises(RuntimeError, match="Snapshot file not found"):
            filter_already_released_images.main(
                ["--snapshot-path", str(tmp_path / "missing.json")]
            )
