"""Test the prepare_fbc_snapshot task."""

from __future__ import annotations

import json
import unittest.mock as mock
from pathlib import Path
from typing import Any

import pytest

from release_service_utils.tasks.managed.prepare_fbc_snapshot import (
    prepare_fbc_snapshot,
)

TASK = "release_service_utils.tasks.managed" ".prepare_fbc_snapshot.prepare_fbc_snapshot"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_json(path: Path, obj: Any) -> Path:
    """Write a JSON file and return its path."""
    path.write_text(json.dumps(obj), encoding="utf-8")
    return path


def _make_snapshot(
    tmp_path: Path,
    components: list[dict[str, Any]] | None = None,
) -> Path:
    """Create a minimal snapshot JSON file."""
    if components is None:
        components = [
            {
                "name": "comp-a",
                "containerImage": "quay.io/org/comp-a@sha256:abc",
                "ocpVersion": ["v4.14"],
            }
        ]
    return _write_json(tmp_path / "snapshot.json", {"components": components})


def _make_data(
    tmp_path: Path,
    *,
    from_index: str = "registry.example.com/index:{{ OCP_VERSION }}",
    target_index: str = "registry.example.com/target:{{ OCP_VERSION }}",
    hotfix: bool = False,
    pre_ga: bool = False,
    staged_index: bool = False,
    extra_fbc: dict[str, Any] | None = None,
) -> Path:
    """Create a minimal data JSON file."""
    fbc: dict[str, Any] = {
        "fromIndex": from_index,
        "targetIndex": target_index,
        "hotfix": hotfix,
        "preGA": pre_ga,
        "stagedIndex": staged_index,
    }
    if extra_fbc:
        fbc.update(extra_fbc)
    return _write_json(tmp_path / "data.json", {"fbc": fbc})


# ---------------------------------------------------------------------------
# sanitize_tag_component
# ---------------------------------------------------------------------------


class TestSanitizeTagComponent:
    """Tests for sanitize_tag_component."""

    def test_passthrough_clean_value(self) -> None:
        """Clean values pass through unchanged."""
        assert prepare_fbc_snapshot.sanitize_tag_component("my-tag", "field", 128) == "my-tag"

    def test_replaces_invalid_chars(self) -> None:
        """Invalid characters are replaced with hyphens."""
        result = prepare_fbc_snapshot.sanitize_tag_component("my tag/here", "field", 128)
        assert result == "my-tag-here"

    def test_collapses_consecutive_specials(self) -> None:
        """Consecutive special characters collapse to a single hyphen."""
        result = prepare_fbc_snapshot.sanitize_tag_component("a..--b", "field", 128)
        assert result == "a-b"

    def test_strips_leading_trailing_specials(self) -> None:
        """Leading/trailing special characters are stripped."""
        result = prepare_fbc_snapshot.sanitize_tag_component("---value---", "field", 128)
        assert result == "value"

    def test_empty_value_raises(self) -> None:
        """Empty input raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            prepare_fbc_snapshot.sanitize_tag_component("", "field", 128)

    def test_all_special_chars_raises(self) -> None:
        """Input that sanitizes to empty string raises ValueError."""
        with pytest.raises(ValueError, match="sanitization resulted in empty"):
            prepare_fbc_snapshot.sanitize_tag_component("---", "field", 128)

    @pytest.mark.parametrize("name", ["latest", "main", "master", "HEAD"])
    def test_reserved_names_raise(self, name: str) -> None:
        """Reserved tag names raise ValueError."""
        with pytest.raises(ValueError, match="reserved name"):
            prepare_fbc_snapshot.sanitize_tag_component(name, "field", 128)

    def test_truncation(self) -> None:
        """Values exceeding max_length are truncated."""
        result = prepare_fbc_snapshot.sanitize_tag_component("a" * 200, "field", 10)
        assert len(result) == 10

    def test_truncation_strips_trailing_specials(self) -> None:
        """Truncation removes trailing special characters."""
        result = prepare_fbc_snapshot.sanitize_tag_component("abcde-fgh", "field", 6)
        assert result == "abcde"


# ---------------------------------------------------------------------------
# replace_ocp_version
# ---------------------------------------------------------------------------


class TestReplaceOcpVersion:
    """Tests for replace_ocp_version."""

    def test_replaces_placeholder(self) -> None:
        """Standard placeholder is replaced."""
        result = prepare_fbc_snapshot.replace_ocp_version(
            "registry/idx:{{ OCP_VERSION }}", "v4.14"
        )
        assert result == "registry/idx:v4.14"

    def test_replaces_without_spaces(self) -> None:
        """Placeholder without spaces is replaced."""
        result = prepare_fbc_snapshot.replace_ocp_version(
            "registry/idx:{{OCP_VERSION}}", "v4.14"
        )
        assert result == "registry/idx:v4.14"

    def test_no_placeholder(self) -> None:
        """Template without placeholder passes through."""
        result = prepare_fbc_snapshot.replace_ocp_version("registry/idx:v4.14", "v4.14")
        assert result == "registry/idx:v4.14"

    def test_multiple_placeholders(self) -> None:
        """All occurrences are replaced."""
        result = prepare_fbc_snapshot.replace_ocp_version(
            "{{ OCP_VERSION }}-{{OCP_VERSION}}", "v4.14"
        )
        assert result == "v4.14-v4.14"


# ---------------------------------------------------------------------------
# validate_ocp_version
# ---------------------------------------------------------------------------


class TestValidateOcpVersion:
    """Tests for validate_ocp_version."""

    def test_exact_match_passes(self) -> None:
        """Exact version match passes validation."""
        prepare_fbc_snapshot.validate_ocp_version("registry/idx:v4.14", "v4.14")

    def test_version_with_suffix_passes(self) -> None:
        """Version with suffix passes validation."""
        prepare_fbc_snapshot.validate_ocp_version("registry/idx:v4.14-suffix", "v4.14")

    def test_mismatch_raises(self) -> None:
        """Version mismatch raises ValueError."""
        with pytest.raises(ValueError, match="does not match"):
            prepare_fbc_snapshot.validate_ocp_version("registry/idx:v4.13", "v4.14")

    def test_prefix_overlap_rejects(self) -> None:
        """Version that shares a prefix but differs rejects."""
        with pytest.raises(ValueError, match="does not match"):
            prepare_fbc_snapshot.validate_ocp_version("registry/idx:v4.10", "v4.1")

    def test_version_with_extra_chars_rejects(self) -> None:
        """Version with non-delimited trailing chars rejects."""
        with pytest.raises(ValueError, match="does not match"):
            prepare_fbc_snapshot.validate_ocp_version("registry/idx:v4.14foo", "v4.14")


# ---------------------------------------------------------------------------
# generate_target_index
# ---------------------------------------------------------------------------


class TestGenerateTargetIndex:
    """Tests for generate_target_index."""

    def test_with_placeholder_and_suffix(self) -> None:
        """Placeholder is replaced and suffix appended."""
        result = prepare_fbc_snapshot.generate_target_index(
            "v4.14",
            "registry/target:{{ OCP_VERSION }}",
            "ISSUE-123-1234567890",
        )
        assert result == "registry/target:v4.14-ISSUE-123-1234567890"

    def test_no_suffix(self) -> None:
        """Without suffix, only placeholder is replaced."""
        result = prepare_fbc_snapshot.generate_target_index(
            "v4.14", "registry/target:{{ OCP_VERSION }}", ""
        )
        assert result == "registry/target:v4.14"

    def test_empty_target_index(self) -> None:
        """Empty raw_target_index returns empty string."""
        result = prepare_fbc_snapshot.generate_target_index("v4.14", "", "suffix")
        assert result == ""


# ---------------------------------------------------------------------------
# build_suffix
# ---------------------------------------------------------------------------


class TestBuildSuffix:
    """Tests for build_suffix."""

    def test_no_hotfix_no_prega(self) -> None:
        """Normal release returns empty suffix."""
        result = prepare_fbc_snapshot.build_suffix(
            {"fbc": {}}, hotfix=False, pre_ga=False, timestamp=100
        )
        assert result == ""

    def test_hotfix_suffix(self) -> None:
        """Hotfix suffix contains sanitized issueId and timestamp."""
        result = prepare_fbc_snapshot.build_suffix(
            {"fbc": {"issueId": "ISSUE-123"}},
            hotfix=True,
            pre_ga=False,
            timestamp=1234567890,
        )
        assert result == "ISSUE-123-1234567890"

    def test_hotfix_missing_issue_id_raises(self) -> None:
        """Hotfix without issueId raises ValueError."""
        with pytest.raises(ValueError, match="issue id"):
            prepare_fbc_snapshot.build_suffix(
                {"fbc": {}},
                hotfix=True,
                pre_ga=False,
                timestamp=100,
            )

    def test_hotfix_min_issue_id_length(self) -> None:
        """Enforce minimum issue id length of 15 when budget is tight."""
        long_ts = 10**120
        result = prepare_fbc_snapshot.build_suffix(
            {"fbc": {"issueId": "A" * 100}},
            hotfix=True,
            pre_ga=False,
            timestamp=long_ts,
        )
        issue_part = result.removesuffix(f"-{long_ts}")
        assert len(issue_part) == 15

    def test_prega_suffix(self) -> None:
        """Pre-GA suffix contains product name, version, and timestamp."""
        result = prepare_fbc_snapshot.build_suffix(
            {
                "fbc": {
                    "productName": "my-product",
                    "productVersion": "1.0",
                }
            },
            hotfix=False,
            pre_ga=True,
            timestamp=1234567890,
        )
        assert result == "my-product-1.0-1234567890"

    def test_prega_missing_product_name_raises(self) -> None:
        """Pre-GA without productName raises ValueError."""
        with pytest.raises(ValueError, match="productName"):
            prepare_fbc_snapshot.build_suffix(
                {"fbc": {"productVersion": "1.0"}},
                hotfix=False,
                pre_ga=True,
                timestamp=100,
            )

    def test_prega_missing_product_version_raises(self) -> None:
        """Pre-GA without productVersion raises ValueError."""
        with pytest.raises(ValueError, match="productVersion"):
            prepare_fbc_snapshot.build_suffix(
                {"fbc": {"productName": "prod"}},
                hotfix=False,
                pre_ga=True,
                timestamp=100,
            )


# ---------------------------------------------------------------------------
# _parse_bool
# ---------------------------------------------------------------------------


class TestParseBool:
    """Tests for _parse_bool."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("True", True),
            ("false", False),
            ("False", False),
            ("", False),
            ("other", False),
            (0, False),
            (1, False),
        ],
    )
    def test_values(self, value: Any, expected: bool) -> None:
        """Various input types are correctly interpreted as booleans."""
        assert prepare_fbc_snapshot._parse_bool(value) is expected


# ---------------------------------------------------------------------------
# run_prepare
# ---------------------------------------------------------------------------


class TestRunPrepare:
    """Tests for run_prepare."""

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_single_version_normal_release(
        self, _mock_time: mock.MagicMock, tmp_path: Path
    ) -> None:
        """Single OCP version with normal release updates snapshot."""
        snapshot_path = _make_snapshot(tmp_path)
        data_path = _make_data(tmp_path)

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        meta = result["components"][0]["ocpVersionMetadata"]
        assert len(meta) == 1
        assert meta[0]["version"] == "v4.14"
        assert meta[0]["updatedFromIndex"] == ("registry.example.com/index:v4.14")
        assert meta[0]["targetIndex"] == ("registry.example.com/target:v4.14")

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_multi_version_component(self, _mock_time: mock.MagicMock, tmp_path: Path) -> None:
        """Multi-OCP-version component gets metadata for each version."""
        snapshot_path = _make_snapshot(
            tmp_path,
            components=[
                {
                    "name": "multi-v",
                    "containerImage": "img@sha256:abc",
                    "ocpVersion": ["v4.14", "v4.15"],
                }
            ],
        )
        data_path = _make_data(tmp_path)

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        meta = result["components"][0]["ocpVersionMetadata"]
        assert len(meta) == 2
        assert meta[0]["version"] == "v4.14"
        assert meta[1]["version"] == "v4.15"
        assert "v4.15" in meta[1]["updatedFromIndex"]

    @mock.patch(f"{TASK}.time.time", return_value=9999)
    def test_hotfix_release(self, _mock_time: mock.MagicMock, tmp_path: Path) -> None:
        """Hotfix release appends issueId-timestamp suffix."""
        snapshot_path = _make_snapshot(tmp_path)
        data_path = _make_data(
            tmp_path,
            hotfix=True,
            extra_fbc={"issueId": "BUG-42"},
        )

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        target = result["components"][0]["ocpVersionMetadata"][0]["targetIndex"]
        assert target == "registry.example.com/target:v4.14-BUG-42-9999"

    @mock.patch(f"{TASK}.time.time", return_value=9999)
    def test_prega_release(self, _mock_time: mock.MagicMock, tmp_path: Path) -> None:
        """Pre-GA release appends productName-productVersion-timestamp."""
        snapshot_path = _make_snapshot(tmp_path)
        data_path = _make_data(
            tmp_path,
            pre_ga=True,
            extra_fbc={
                "productName": "MyProduct",
                "productVersion": "2.1",
            },
        )

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        target = result["components"][0]["ocpVersionMetadata"][0]["targetIndex"]
        assert target == ("registry.example.com/target:v4.14-MyProduct-2.1-9999")

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_staged_release_empty_target(
        self, _mock_time: mock.MagicMock, tmp_path: Path
    ) -> None:
        """Staged release allows empty targetIndex."""
        snapshot_path = _make_snapshot(tmp_path)
        data_path = _make_data(
            tmp_path,
            target_index="",
            staged_index=True,
        )

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        meta = result["components"][0]["ocpVersionMetadata"][0]
        assert meta["targetIndex"] == ""

    def test_missing_from_index_raises(self, tmp_path: Path) -> None:
        """Missing fbc.fromIndex raises ValueError."""
        snapshot_path = _make_snapshot(tmp_path)
        data_path = _make_data(tmp_path, from_index="")

        with pytest.raises(ValueError, match="fbc.fromIndex"):
            prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

    def test_missing_target_index_non_staged_raises(self, tmp_path: Path) -> None:
        """Non-staged release without targetIndex raises ValueError."""
        snapshot_path = _make_snapshot(tmp_path)
        data_path = _make_data(tmp_path, target_index="", staged_index=False)

        with pytest.raises(ValueError, match="fbc.targetIndex"):
            prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

    def test_no_components_raises(self, tmp_path: Path) -> None:
        """Empty components array raises ValueError."""
        snapshot_path = _make_snapshot(tmp_path, components=[])
        data_path = _make_data(tmp_path)

        with pytest.raises(ValueError, match="No components"):
            prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

    def test_missing_ocp_version_raises(self, tmp_path: Path) -> None:
        """Component without ocpVersion raises ValueError."""
        snapshot_path = _make_snapshot(tmp_path, components=[{"name": "no-ocp"}])
        data_path = _make_data(tmp_path)

        with pytest.raises(ValueError, match="ocpVersion not found"):
            prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_ocp_version_mismatch_raises(
        self, _mock_time: mock.MagicMock, tmp_path: Path
    ) -> None:
        """OCP version mismatch between index and component raises."""
        snapshot_path = _make_snapshot(
            tmp_path,
            components=[
                {
                    "name": "comp",
                    "ocpVersion": ["v4.14"],
                }
            ],
        )
        data_path = _make_data(
            tmp_path,
            from_index="registry/idx:v4.13",
        )

        with pytest.raises(ValueError, match="does not match"):
            prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_fixed_index_no_placeholder(
        self, _mock_time: mock.MagicMock, tmp_path: Path
    ) -> None:
        """Fixed indexes without placeholder pass when versions match."""
        snapshot_path = _make_snapshot(tmp_path)
        data_path = _make_data(
            tmp_path,
            from_index="registry/idx:v4.14",
            target_index="registry/target:v4.14",
        )

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        meta = result["components"][0]["ocpVersionMetadata"][0]
        assert meta["updatedFromIndex"] == "registry/idx:v4.14"
        assert meta["targetIndex"] == "registry/target:v4.14"

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_multiple_components(self, _mock_time: mock.MagicMock, tmp_path: Path) -> None:
        """All components in the snapshot are processed."""
        snapshot_path = _make_snapshot(
            tmp_path,
            components=[
                {"name": "a", "ocpVersion": ["v4.14"]},
                {"name": "b", "ocpVersion": ["v4.14", "v4.15"]},
            ],
        )
        data_path = _make_data(tmp_path)

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        assert len(result["components"][0]["ocpVersionMetadata"]) == 1
        assert len(result["components"][1]["ocpVersionMetadata"]) == 2

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_scalar_ocp_version(self, _mock_time: mock.MagicMock, tmp_path: Path) -> None:
        """Scalar string ocpVersion is normalized to a one-element list."""
        snapshot_path = _make_snapshot(
            tmp_path,
            components=[
                {
                    "name": "scalar-comp",
                    "containerImage": "img@sha256:abc",
                    "ocpVersion": "v4.14",
                }
            ],
        )
        data_path = _make_data(tmp_path)

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_path)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        meta = result["components"][0]["ocpVersionMetadata"]
        assert len(meta) == 1
        assert meta[0]["version"] == "v4.14"

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_bool_string_flags(self, _mock_time: mock.MagicMock, tmp_path: Path) -> None:
        """String 'true'/'false' flags are handled correctly."""
        snapshot_path = _make_snapshot(tmp_path)
        data_file = tmp_path / "data.json"
        data_file.write_text(
            json.dumps(
                {
                    "fbc": {
                        "fromIndex": "reg/idx:{{ OCP_VERSION }}",
                        "targetIndex": "reg/tgt:{{ OCP_VERSION }}",
                        "hotfix": "false",
                        "preGA": "false",
                        "stagedIndex": "false",
                    }
                }
            ),
            encoding="utf-8",
        )

        prepare_fbc_snapshot.run_prepare(snapshot_path=snapshot_path, data_path=data_file)

        result = json.loads(snapshot_path.read_text(encoding="utf-8"))
        meta = result["components"][0]["ocpVersionMetadata"][0]
        assert meta["targetIndex"] == "reg/tgt:v4.14"


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    """Tests for main."""

    @mock.patch(f"{TASK}.time.time", return_value=1234567890)
    def test_main_wiring(
        self,
        _mock_time: mock.MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify main reads env vars and calls run_prepare."""
        _make_snapshot(tmp_path)
        _make_data(tmp_path)

        monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot.json")
        monkeypatch.setenv("PARAM_DATA_PATH", "data.json")

        assert prepare_fbc_snapshot.main() == 0

        result = json.loads((tmp_path / "snapshot.json").read_text(encoding="utf-8"))
        assert "ocpVersionMetadata" in result["components"][0]
