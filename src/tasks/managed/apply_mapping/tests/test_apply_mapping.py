"""Tests for the ``apply_mapping`` task script."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from release_service_utils.tasks.managed import apply_mapping as apply_mapping_pkg

apply_mapping = apply_mapping_pkg.apply_mapping

TASK = "release_service_utils.tasks.managed.apply_mapping.apply_mapping"

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


def _completed(stdout: str = "", returncode: int = 0, stderr: str = "") -> Any:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr
    )


def _fake_inspect(
    raw_response: dict, standard_response: dict | None = None
) -> apply_mapping.InspectFn:
    """Build an inspect_fn stub keyed on the ``raw`` flag."""

    def _inspect(ref: str, **kwargs: Any) -> Any:
        if kwargs.get("raw"):
            return _completed(json.dumps(raw_response))
        return _completed(json.dumps(standard_response or {}))

    return _inspect


def _fake_list_tags(tags_by_repo: dict[str, list[str]]) -> apply_mapping.ListTagsFn:
    def _list_tags(repo: str) -> list[str]:
        return tags_by_repo.get(repo, [])

    return _list_tags


def _fake_format_date(value: str, fmt: str) -> str:
    return f"{value}|{fmt}"


def _fake_get_arch(
    architecture: str = "amd64", os_name: str = "linux", digest: str = "sha256:aaa"
) -> apply_mapping.GetArchFn:
    def _get_arch(image_ref: str) -> list[dict]:
        return [{"platform": {"architecture": architecture, "os": os_name}, "digest": digest}]

    return _get_arch


# ---------------------------------------------------------------------------
# _git_revision_str
# ---------------------------------------------------------------------------


def test_git_revision_str_present() -> None:
    """A component with a git revision returns it as a string."""
    component = {"source": {"git": {"revision": "abc123"}}}
    assert apply_mapping._git_revision_str(component) == "abc123"


def test_git_revision_str_missing_source() -> None:
    """A component with no ``source`` key returns the literal string 'null'."""
    assert apply_mapping._git_revision_str({}) == "null"


def test_git_revision_str_missing_git() -> None:
    """A component with ``source`` but no ``git`` key returns 'null'."""
    assert apply_mapping._git_revision_str({"source": {}}) == "null"


def test_git_revision_str_null_revision() -> None:
    """A ``revision`` explicitly set to ``None`` returns 'null'."""
    component = {"source": {"git": {"revision": None}}}
    assert apply_mapping._git_revision_str(component) == "null"


def test_git_revision_str_non_dict_source() -> None:
    """A non-dict ``source`` value returns 'null' rather than raising."""
    assert apply_mapping._git_revision_str({"source": "not-a-dict"}) == "null"


# ---------------------------------------------------------------------------
# _digest_sha
# ---------------------------------------------------------------------------


def test_digest_sha_extracts_hex_only() -> None:
    """Only the hex portion after the last colon is returned."""
    assert apply_mapping._digest_sha("registry.io/repo@sha256:abc123") == "abc123"


# ---------------------------------------------------------------------------
# _set_metadata_field
# ---------------------------------------------------------------------------


def test_set_metadata_field_creates_metadata_dict() -> None:
    """A component with no metadata gets one created."""
    component: dict = {}
    apply_mapping._set_metadata_field(component, "media_type", "application/json")
    assert component["metadata"] == {"media_type": "application/json"}


def test_set_metadata_field_preserves_existing_keys() -> None:
    """Existing metadata keys are preserved when adding a new one."""
    component = {"metadata": {"existing": "value"}}
    apply_mapping._set_metadata_field(component, "media_type", "application/json")
    assert component["metadata"] == {"existing": "value", "media_type": "application/json"}


# ---------------------------------------------------------------------------
# _validate_tag
# ---------------------------------------------------------------------------


def test_validate_tag_accepts_valid_characters() -> None:
    """Alphanumerics, dots, dashes, and underscores are accepted."""
    apply_mapping._validate_tag("v1.0.0-final_1")


def test_validate_tag_rejects_invalid_characters() -> None:
    """Characters outside the allowed tag charset raise ``ValueError``."""
    with pytest.raises(ValueError, match="Invalid tag format"):
        apply_mapping._validate_tag("bad tag!")


def test_validate_tag_rejects_empty_string() -> None:
    """An empty tag is invalid."""
    with pytest.raises(ValueError, match="Invalid tag format"):
        apply_mapping._validate_tag("")


# ---------------------------------------------------------------------------
# _max_increment
# ---------------------------------------------------------------------------


def test_max_increment_finds_highest_matching_tag() -> None:
    """The highest numeric suffix among matching tags is returned."""
    tags = ["v1.0.0-1", "v1.0.0-2", "v1.0.0-10", "other-tag"]
    assert apply_mapping._max_increment(tags, "v1.0.0-") == 10


def test_max_increment_no_matches_returns_zero() -> None:
    """No matching tags returns 0."""
    assert apply_mapping._max_increment(["other"], "v1.0.0-") == 0


def test_max_increment_ignores_seven_plus_digit_suffixes() -> None:
    """Suffixes with 7+ digits are ignored (avoids matching commit SHAs)."""
    tags = ["v1.0.0-1234567", "v1.0.0-5"]
    assert apply_mapping._max_increment(tags, "v1.0.0-") == 5


def test_max_increment_handles_leading_zeros() -> None:
    """Leading zeros in the numeric suffix are parsed as decimal, not octal."""
    assert apply_mapping._max_increment(["v1.0.0-007"], "v1.0.0-") == 7


def test_max_increment_escapes_regex_metacharacters_in_prefix() -> None:
    """A prefix containing regex metacharacters is treated literally."""
    tags = ["v1.0.0-1", "v1x0x0-9"]
    assert apply_mapping._max_increment(tags, "v1.0.0-") == 1


# ---------------------------------------------------------------------------
# increment_tag
# ---------------------------------------------------------------------------


def test_increment_tag_first_increment() -> None:
    """No existing tags means the first increment is 1."""
    list_tags_fn = _fake_list_tags({"repo": []})
    assert (
        apply_mapping.increment_tag("v1.0.0-{{ incrementer }}", "repo", list_tags_fn)
        == "v1.0.0-1"
    )


def test_increment_tag_increments_past_existing() -> None:
    """The next tag is one past the highest existing matching tag."""
    list_tags_fn = _fake_list_tags({"repo": ["v1.0.0-1", "v1.0.0-2"]})
    assert (
        apply_mapping.increment_tag("v1.0.0-{{ incrementer }}", "repo", list_tags_fn)
        == "v1.0.0-3"
    )


def test_increment_tag_flexible_whitespace() -> None:
    """Extra/no whitespace around the placeholder is handled."""
    list_tags_fn = _fake_list_tags({"repo": []})
    assert apply_mapping.increment_tag("v1-{{incrementer}}", "repo", list_tags_fn) == "v1-1"
    assert (
        apply_mapping.increment_tag("v1-{{   incrementer   }}", "repo", list_tags_fn) == "v1-1"
    )


def test_increment_tag_invalid_result_raises() -> None:
    """A version prefix containing invalid tag characters propagates the validation error."""
    list_tags_fn = _fake_list_tags({"repo": []})
    with pytest.raises(ValueError, match="Invalid tag format"):
        apply_mapping.increment_tag("bad tag-{{ incrementer }}", "repo", list_tags_fn)


# ---------------------------------------------------------------------------
# component_increment_tag
# ---------------------------------------------------------------------------


def test_component_increment_tag_queries_all_repos() -> None:
    """The maximum across all repositories is used."""
    list_tags_fn = _fake_list_tags(
        {"repo-a": ["v1.0.0-3"], "repo-b": ["v1.0.0-5"], "repo-c": []}
    )
    cache: dict[str, int] = {}
    tag = apply_mapping.component_increment_tag(
        "v1.0.0-{{ component-incrementer }}",
        ["repo-a", "repo-b", "repo-c"],
        cache,
        list_tags_fn,
    )
    assert tag == "v1.0.0-6"


def test_component_increment_tag_caches_by_prefix() -> None:
    """A second call with the same prefix reuses the cached value without re-querying."""
    calls: list[str] = []

    def _list_tags(repo: str) -> list[str]:
        calls.append(repo)
        return ["v1.0.0-3"]

    cache: dict[str, int] = {}
    first = apply_mapping.component_increment_tag(
        "v1.0.0-{{ component-incrementer }}", ["repo-a"], cache, _list_tags
    )
    second = apply_mapping.component_increment_tag(
        "v1.0.0-{{ component-incrementer }}", ["repo-a"], cache, _list_tags
    )
    assert first == second == "v1.0.0-4"
    assert calls == ["repo-a"]


def test_component_increment_tag_different_prefixes_not_cached_together() -> None:
    """Different version prefixes get independent cache entries."""
    list_tags_fn = _fake_list_tags({"repo-a": ["v1.0.0-3", "v2.0.0-9"]})
    cache: dict[str, int] = {}
    tag1 = apply_mapping.component_increment_tag(
        "v1.0.0-{{ component-incrementer }}", ["repo-a"], cache, list_tags_fn
    )
    tag2 = apply_mapping.component_increment_tag(
        "v2.0.0-{{ component-incrementer }}", ["repo-a"], cache, list_tags_fn
    )
    assert tag1 == "v1.0.0-4"
    assert tag2 == "v2.0.0-10"


def test_component_increment_tag_empty_repo_list_defaults_to_one() -> None:
    """No repositories means the increment defaults to 1."""
    cache: dict[str, int] = {}
    tag = apply_mapping.component_increment_tag(
        "v1.0.0-{{ component-incrementer }}", [], cache, _fake_list_tags({})
    )
    assert tag == "v1.0.0-1"


def test_component_increment_tag_flexible_whitespace() -> None:
    """Extra whitespace around the placeholder is handled."""
    cache: dict[str, int] = {}
    tag = apply_mapping.component_increment_tag(
        "v1-{{component-incrementer}}", [], cache, _fake_list_tags({})
    )
    assert tag == "v1-1"


# ---------------------------------------------------------------------------
# _substitute_value
# ---------------------------------------------------------------------------


def test_substitute_value_from_substitute_map() -> None:
    """Plain variables are read from the substitute map."""
    assert apply_mapping._substitute_value("git_sha", {"git_sha": "abc"}, {}) == "abc"


def test_substitute_value_from_labels() -> None:
    """``labels.<name>`` variables are read from the labels dict."""
    assert (
        apply_mapping._substitute_value("labels.mylabel", {}, {"mylabel": "value"}) == "value"
    )


def test_substitute_value_missing_returns_empty_string() -> None:
    """Missing values return an empty string rather than raising."""
    assert apply_mapping._substitute_value("unknown", {}, {}) == ""
    assert apply_mapping._substitute_value("labels.missing", {}, {}) == ""


# ---------------------------------------------------------------------------
# translate_one_tag / translate_tags
# ---------------------------------------------------------------------------


def test_translate_one_tag_substitutes_variable() -> None:
    """A simple variable reference is substituted."""
    result = apply_mapping.translate_one_tag(
        "release-{{ git_sha }}", {"git_sha": "abc123"}, {}, "repo", [], {}, _fake_list_tags({})
    )
    assert result == "release-abc123"


def test_translate_one_tag_multiple_variables() -> None:
    """Multiple variable references are all resolved."""
    result = apply_mapping.translate_one_tag(
        "{{ git_short_sha }}-{{ oci_version }}",
        {"git_short_sha": "abc1234", "oci_version": "1_0"},
        {},
        "repo",
        [],
        {},
        _fake_list_tags({}),
    )
    assert result == "abc1234-1_0"


def test_translate_one_tag_label_variable() -> None:
    """A labels.* reference is substituted from the labels dict."""
    result = apply_mapping.translate_one_tag(
        "tag-{{ labels.mylabel }}",
        {},
        {"mylabel": "labelvalue"},
        "repo",
        [],
        {},
        _fake_list_tags({}),
    )
    assert result == "tag-labelvalue"


def test_translate_one_tag_no_variables_passthrough() -> None:
    """A tag with no template variables is returned unchanged (after validation)."""
    result = apply_mapping.translate_one_tag(
        "plain-tag", {}, {}, "repo", [], {}, _fake_list_tags({})
    )
    assert result == "plain-tag"


def test_translate_one_tag_unknown_variable_raises() -> None:
    """An unknown or empty substitution variable raises ``ValueError``."""
    with pytest.raises(ValueError, match="Substitution variable unknown or empty"):
        apply_mapping.translate_one_tag(
            "{{ unknown_var }}", {}, {}, "repo", [], {}, _fake_list_tags({})
        )


def test_translate_one_tag_incrementer() -> None:
    """``{{ incrementer }}`` is resolved via the injected list_tags_fn."""
    result = apply_mapping.translate_one_tag(
        "v1.0.0-{{ incrementer }}",
        {},
        {},
        "repo",
        [],
        {},
        _fake_list_tags({"repo": ["v1.0.0-1"]}),
    )
    assert result == "v1.0.0-2"


def test_translate_one_tag_component_incrementer() -> None:
    """``{{ component-incrementer }}`` is resolved against all_repos."""
    cache: dict[str, int] = {}
    result = apply_mapping.translate_one_tag(
        "v1.0.0-{{ component-incrementer }}",
        {},
        {},
        "repo-a",
        ["repo-a", "repo-b"],
        cache,
        _fake_list_tags({"repo-a": ["v1.0.0-1"], "repo-b": ["v1.0.0-4"]}),
    )
    assert result == "v1.0.0-5"


def test_translate_one_tag_invalid_result_raises() -> None:
    """A substituted value that produces an invalid tag raises ``ValueError``."""
    with pytest.raises(ValueError, match="Invalid tag format"):
        apply_mapping.translate_one_tag(
            "{{ git_sha }}", {"git_sha": "bad sha!"}, {}, "repo", [], {}, _fake_list_tags({})
        )


def test_translate_tags_deduplicates_preserving_order() -> None:
    """Duplicate translated tags are dropped, keeping the first occurrence's position."""
    result = apply_mapping.translate_tags(
        ["static", "{{ git_sha }}", "static"],
        {"git_sha": "static"},
        {},
        "repo",
        [],
        {},
        _fake_list_tags({}),
    )
    assert result == ["static"]


def test_translate_tags_empty_list_returns_empty_list() -> None:
    """An empty tags list returns an empty list."""
    assert apply_mapping.translate_tags([], {}, {}, "repo", [], {}, _fake_list_tags({})) == []


# ---------------------------------------------------------------------------
# ensure_implicit_timestamp_value
# ---------------------------------------------------------------------------


def test_ensure_implicit_timestamp_value_disabled_returns_unchanged() -> None:
    """When disabled, the tag list is returned unchanged."""
    tags = ["a", "b"]
    assert apply_mapping.ensure_implicit_timestamp_value(tags, "", False) is tags


def test_ensure_implicit_timestamp_value_appends_and_dedupes() -> None:
    """The timestamp is appended and the result is deduplicated/sorted."""
    result = apply_mapping.ensure_implicit_timestamp_value(["b", "a"], "c", True)
    assert result == ["a", "b", "c"]


def test_ensure_implicit_timestamp_value_already_present() -> None:
    """No duplicate entry is added if the timestamp tag is already present."""
    result = apply_mapping.ensure_implicit_timestamp_value(["a", "ts"], "ts", True)
    assert result == ["a", "ts"]


def test_ensure_implicit_timestamp_value_empty_timestamp_raises() -> None:
    """An empty timestamp with the flag enabled raises ``ValueError``."""
    with pytest.raises(ValueError, match="timestamp is empty"):
        apply_mapping.ensure_implicit_timestamp_value(["a"], "", True)


# ---------------------------------------------------------------------------
# merge_components
# ---------------------------------------------------------------------------


def test_merge_components_keeps_only_components_in_both() -> None:
    """Components present in only one list are dropped."""
    original = [{"name": "comp1"}, {"name": "only-in-snapshot"}]
    mapping = [{"name": "comp1"}, {"name": "only-in-mapping"}]
    result = apply_mapping.merge_components(original, mapping)
    assert [c["name"] for c in result] == ["comp1"]


def test_merge_components_mapping_overrides_original() -> None:
    """Mapping values win over original values for matching scalar keys."""
    original = [{"name": "comp1", "containerImage": "old@sha256:aaa"}]
    mapping = [{"name": "comp1", "containerImage": "new@sha256:bbb"}]
    result = apply_mapping.merge_components(original, mapping)
    assert result == [{"name": "comp1", "containerImage": "new@sha256:bbb"}]


def test_merge_components_preserves_original_only_keys() -> None:
    """Keys present only on the original side (e.g. source) are preserved."""
    original = [{"name": "comp1", "source": {"git": {"revision": "myrev"}}}]
    mapping = [{"name": "comp1", "repositories": [{"url": "registry.io/repo"}]}]
    result = apply_mapping.merge_components(original, mapping)
    assert result[0]["source"] == {"git": {"revision": "myrev"}}
    assert result[0]["repositories"] == [{"url": "registry.io/repo"}]


def test_merge_components_sorted_by_name() -> None:
    """Merged components are ordered alphabetically by name."""
    original = [{"name": "zeta"}, {"name": "alpha"}]
    mapping = [{"name": "zeta"}, {"name": "alpha"}]
    result = apply_mapping.merge_components(original, mapping)
    assert [c["name"] for c in result] == ["alpha", "zeta"]


def test_merge_components_empty_inputs() -> None:
    """Empty original and mapping lists produce an empty result."""
    assert apply_mapping.merge_components([], []) == []


def test_merge_components_missing_name_raises() -> None:
    """A component with no ``name`` key raises ``ValueError`` instead of merging silently."""
    original = [{"containerImage": "a@sha256:aaa"}]
    mapping = [{"containerImage": "b@sha256:bbb"}]
    with pytest.raises(ValueError, match="missing a valid 'name' field"):
        apply_mapping.merge_components(original, mapping)


def test_merge_components_empty_string_name_raises() -> None:
    """A component with an empty string ``name`` raises ``ValueError``."""
    original = [{"name": "", "containerImage": "a@sha256:aaa"}]
    mapping = [{"name": "comp1"}]
    with pytest.raises(ValueError, match="missing a valid 'name' field"):
        apply_mapping.merge_components(original, mapping)


# ---------------------------------------------------------------------------
# _extract_manifest_info
# ---------------------------------------------------------------------------


def test_extract_manifest_info_standard_image() -> None:
    """Standard OCI/Docker images pull labels, env, and build-date from ``inspect``."""
    raw_manifest = {
        "annotations": {"org.opencontainers.image.version": "1.0.0+build"},
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json"},
    }
    standard = {
        "Labels": {"build-date": "2024-01-15T00:00:00Z", "custom": "val"},
        "Env": ["FOO=bar"],
        "Created": "2024-01-01T00:00:00Z",
    }
    info = apply_mapping._extract_manifest_info(
        _fake_inspect(raw_manifest, standard),
        "registry.io/repo@sha256:abcdef",
        "registry.io/repo@sha256:aaa",
        "amd64",
        "linux",
    )
    assert info.annotations == raw_manifest["annotations"]
    assert info.config_media_type == "application/vnd.oci.image.config.v1+json"
    assert info.labels == standard["Labels"]
    assert info.build_date == "2024-01-15T00:00:00Z"
    assert info.env_variables == ["FOO=bar"]
    assert info.oci_version_raw == "1.0.0+build"


def test_extract_manifest_info_standard_image_falls_back_to_created() -> None:
    """When the ``build-date`` label is absent, ``Created`` is used instead."""
    raw_manifest = {"config": {"mediaType": "application/vnd.docker.container.image.v1+json"}}
    standard = {"Labels": {}, "Env": [], "Created": "2024-01-01T00:00:00Z"}
    info = apply_mapping._extract_manifest_info(
        _fake_inspect(raw_manifest, standard),
        "registry.io/repo@sha256:abcdef",
        "registry.io/repo@sha256:aaa",
        "amd64",
        "linux",
    )
    assert info.build_date == "2024-01-01T00:00:00Z"


def test_extract_manifest_info_non_standard_artifact() -> None:
    """Non-standard artifacts (e.g. Helm charts) skip standard inspect entirely."""
    raw_manifest = {
        "annotations": {"org.opencontainers.image.created": "2024-02-02T00:00:00Z"},
        "config": {"mediaType": "application/vnd.cncf.helm.config.v1+json"},
    }
    info = apply_mapping._extract_manifest_info(
        _fake_inspect(raw_manifest),
        "registry.io/repo@sha256:abcdef",
        "registry.io/repo@sha256:aaa",
        "amd64",
        "linux",
    )
    assert info.labels == {}
    assert info.env_variables == []
    assert info.build_date == "2024-02-02T00:00:00Z"


def test_extract_manifest_info_oci_version_falls_back_to_labels() -> None:
    """When annotations lack an OCI version, the label value is used instead."""
    raw_manifest = {"config": {"mediaType": "application/vnd.oci.image.config.v1+json"}}
    standard = {"Labels": {"org.opencontainers.image.version": "2.0.0"}}
    info = apply_mapping._extract_manifest_info(
        _fake_inspect(raw_manifest, standard),
        "registry.io/repo@sha256:abcdef",
        "registry.io/repo@sha256:aaa",
        "amd64",
        "linux",
    )
    assert info.oci_version_raw == "2.0.0"


# ---------------------------------------------------------------------------
# process_component
# ---------------------------------------------------------------------------


def _base_component(**overrides: Any) -> dict:
    component = {
        "name": "comp1",
        "containerImage": "registry.io/repo@sha256:abcdef1234567890",
        "repositories": [{"url": "registry.io/dest", "tags": ["latest"]}],
    }
    component.update(overrides)
    return component


def test_process_component_invalid_container_image_raises() -> None:
    """A containerImage without a sha256 digest raises ``ValueError``."""
    component = _base_component(containerImage="registry.io/repo:latest")
    with pytest.raises(ValueError, match="invalid containerImage value"):
        apply_mapping.process_component(
            component,
            default_tags=[],
            default_timestamp_format="%s",
            current_timestamp="20240101 00:00:00",
            default_cgw_settings={},
            add_implicit_timestamp_tag=False,
            inspect_fn=_fake_inspect({}),
            list_tags_fn=_fake_list_tags({}),
            get_arch_fn=_fake_get_arch(),
            format_date_fn=_fake_format_date,
        )


def test_process_component_no_architectures_raises() -> None:
    """A ``get_arch_fn`` returning no architectures raises ``RuntimeError``."""
    component = _base_component()
    with pytest.raises(RuntimeError, match="No architectures were discovered"):
        apply_mapping.process_component(
            component,
            default_tags=[],
            default_timestamp_format="%s",
            current_timestamp="20240101 00:00:00",
            default_cgw_settings={},
            add_implicit_timestamp_tag=False,
            inspect_fn=_fake_inspect({}),
            list_tags_fn=_fake_list_tags({}),
            get_arch_fn=lambda image_ref: [],
            format_date_fn=_fake_format_date,
        )


def test_process_component_standard_image_metadata() -> None:
    """Standard OCI image metadata (labels, env, build-date) is attached to the component."""
    component = _base_component()
    raw_manifest = {
        "annotations": {"org.opencontainers.image.version": "1.0.0+build"},
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json"},
    }
    standard = {
        "Labels": {"build-date": "2024-01-15T00:00:00Z", "custom": "val"},
        "Env": ["FOO=bar"],
        "Created": "2024-01-01T00:00:00Z",
    }
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect(raw_manifest, standard),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )

    metadata = component["metadata"]
    assert metadata["env_variables"] == ["FOO=bar"]
    assert {"name": "custom", "value": "val"} in metadata["labels"]
    assert {
        "name": "org.opencontainers.image.version",
        "value": "1.0.0+build",
    } in metadata["annotations"]
    assert metadata["media_type"] == "application/vnd.oci.image.config.v1+json"


def test_process_component_oci_version_plus_replaced_with_underscore() -> None:
    """A ``+`` in the OCI version annotation is replaced with ``_`` in the substitute map."""
    component = _base_component(
        repositories=[{"url": "registry.io/dest", "tags": ["{{ oci_version }}"]}]
    )
    raw_manifest = {
        "annotations": {"org.opencontainers.image.version": "1.0.0+build.5"},
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json"},
    }
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect(raw_manifest, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["repositories"][0]["tags"] == ["1.0.0_build.5"]


def test_process_component_oci_version_defaults_to_unknown() -> None:
    """No OCI version annotation/label defaults the substitution value to 'unknown'."""
    component = _base_component(
        repositories=[{"url": "registry.io/dest", "tags": ["{{ oci_version }}"]}]
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["repositories"][0]["tags"] == ["unknown"]


def test_process_component_oci_version_falls_back_to_labels() -> None:
    """When annotations lack the OCI version, labels are checked instead."""
    component = _base_component(
        repositories=[{"url": "registry.io/dest", "tags": ["{{ oci_version }}"]}]
    )
    raw_manifest = {
        "annotations": {},
        "config": {"mediaType": "application/vnd.docker.container.image.v1+json"},
    }
    standard = {"Labels": {"org.opencontainers.image.version": "2.0.0"}}
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect(raw_manifest, standard),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["repositories"][0]["tags"] == ["2.0.0"]


def test_process_component_non_standard_artifact_uses_annotations_only() -> None:
    """Non-standard artifacts (e.g. Helm charts) skip standard inspect and use annotations."""
    component = _base_component()
    raw_manifest = {
        "annotations": {"org.opencontainers.image.created": "2024-02-01T00:00:00Z"},
        "config": {"mediaType": "application/vnd.cncf.helm.config.v1+json"},
    }
    calls: list[bool] = []

    def inspect_fn(ref: str, **kwargs: Any) -> Any:
        calls.append(bool(kwargs.get("raw")))
        return _completed(json.dumps(raw_manifest if kwargs.get("raw") else {}))

    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=inspect_fn,
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    # Only the raw inspect call should have happened - no standard inspect for Helm charts.
    assert calls == [True]
    assert "labels" not in component.get("metadata", {})
    assert "env_variables" not in component.get("metadata", {})
    assert component["metadata"]["media_type"] == "application/vnd.cncf.helm.config.v1+json"


def test_process_component_no_metadata_added_when_nothing_present() -> None:
    """No metadata key is created if there's nothing to add."""
    component = _base_component()
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert "metadata" not in component


def test_process_component_timestamp_empty_when_no_build_date() -> None:
    """No build-date label/Created/annotation leaves the timestamp substitution empty."""
    component = _base_component(
        repositories=[{"url": "registry.io/dest", "tags": ["tag-{{ timestamp }}"]}]
    )
    with pytest.raises(ValueError, match="Substitution variable unknown or empty"):
        apply_mapping.process_component(
            component,
            default_tags=[],
            default_timestamp_format="%s",
            current_timestamp="20240101 00:00:00",
            default_cgw_settings={},
            add_implicit_timestamp_tag=False,
            inspect_fn=_fake_inspect({}, {}),
            list_tags_fn=_fake_list_tags({}),
            get_arch_fn=_fake_get_arch(),
            format_date_fn=_fake_format_date,
        )


def test_process_component_staged_files_filenames_translated() -> None:
    """Staged file filenames have tag variables substituted."""
    component = _base_component(
        source={"git": {"revision": "abcdef123456"}},
        staged={"files": [{"filename": "release-{{ git_short_sha }}.txt"}]},
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["staged"]["files"][0]["filename"] == "release-abcdef1.txt"


def test_process_component_content_gateway_merge() -> None:
    """Component-level contentGateway settings are deep-merged with defaults."""
    component = _base_component(contentGateway={"accountId": ["222"]})
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={"accountId": ["111"], "publish": True},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["contentGateway"] == {"accountId": ["111", "222"], "publish": True}


def test_process_component_content_gateway_absent_when_empty() -> None:
    """No contentGateway key is added when both defaults and component settings are empty."""
    component = _base_component()
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert "contentGateway" not in component


def test_process_component_tag_layering_defaults_component_repo() -> None:
    """Default tags, componentTags, and repo tags are all merged and deduplicated."""
    component = _base_component(
        componentTags=["comp-tag"],
        repositories=[{"url": "registry.io/dest", "tags": ["repo-tag", "comp-tag"]}],
    )
    apply_mapping.process_component(
        component,
        default_tags=["default-tag"],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert sorted(component["repositories"][0]["tags"]) == [
        "comp-tag",
        "default-tag",
        "repo-tag",
    ]


def test_process_component_no_tags_leaves_repository_tags_untouched() -> None:
    """When there are no tags to set at all, the repository's tags key is left alone."""
    component = _base_component(repositories=[{"url": "registry.io/dest"}])
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert "tags" not in component["repositories"][0]


def test_process_component_add_implicit_timestamp_tag() -> None:
    """The implicit timestamp tag is appended when the flag is enabled."""
    component = _base_component(repositories=[{"url": "registry.io/dest", "tags": ["static"]}])
    raw_manifest = {"annotations": {"org.opencontainers.image.created": "2024-01-01"}}
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=True,
        inspect_fn=_fake_inspect(raw_manifest, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert "2024-01-01|%s" in component["repositories"][0]["tags"]


def test_process_component_registry_conversion_from_quay_temp() -> None:
    """A quay.io temp-namespace repo URL is converted to registry.redhat.io/quay round trip."""
    component = _base_component(
        repositories=[{"url": "quay.io/redhat-prod/rhel8----nodejs", "tags": ["latest"]}]
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    repo = component["repositories"][0]
    assert repo["url"] == "quay.io/redhat-prod/rhel8----nodejs"
    assert repo["rh-registry-repo"] == "registry.redhat.io/rhel8/nodejs"
    assert repo["registry-access-repo"] == "registry.access.redhat.com/rhel8/nodejs"


def test_process_component_registry_conversion_already_registry_format() -> None:
    """A repo already in registry.redhat.io format is converted to quay for the url field."""
    component = _base_component(
        repositories=[{"url": "registry.redhat.io/rhel8/nodejs", "tags": ["latest"]}]
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    repo = component["repositories"][0]
    assert repo["url"] == "quay.io/redhat-prod/rhel8----nodejs"
    assert repo["rh-registry-repo"] == "registry.redhat.io/rhel8/nodejs"
    assert repo["registry-access-repo"] == "registry.access.redhat.com/rhel8/nodejs"


def test_process_component_registry_conversion_unhandled_format_untouched() -> None:
    """A repo URL that isn't a Red Hat registry format is left completely unmodified."""
    component = _base_component(
        repositories=[{"url": "quay.io/someorg/somerepo", "tags": ["latest"]}]
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    repo = component["repositories"][0]
    assert repo["url"] == "quay.io/someorg/somerepo"
    assert "rh-registry-repo" not in repo
    assert "registry-access-repo" not in repo


def test_process_component_multiple_repositories_share_component_incrementer_cache() -> None:
    """component-incrementer is computed once per component and reused across repositories."""
    component = _base_component(
        repositories=[
            {"url": "repo-a", "tags": ["v1-{{ component-incrementer }}"]},
            {"url": "repo-b", "tags": ["v1-{{ component-incrementer }}"]},
        ]
    )
    list_tags_fn = _fake_list_tags({"repo-a": ["v1-2"], "repo-b": []})
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=list_tags_fn,
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["repositories"][0]["tags"] == ["v1-3"]
    assert component["repositories"][1]["tags"] == ["v1-3"]


def test_process_component_git_sha_and_short_sha_substitution() -> None:
    """git_sha and git_short_sha are substituted from source.git.revision."""
    component = _base_component(
        source={"git": {"revision": "abcdef1234567890"}},
        repositories=[
            {"url": "registry.io/dest", "tags": ["{{ git_sha }}", "{{ git_short_sha }}"]}
        ],
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    tags = component["repositories"][0]["tags"]
    assert "abcdef1234567890" in tags
    assert "abcdef1" in tags


def test_process_component_digest_sha_substitution() -> None:
    """digest_sha substitutes the hex digest of the containerImage."""
    component = _base_component(
        containerImage="registry.io/repo@sha256:123456",
        repositories=[{"url": "registry.io/dest", "tags": ["foo-{{ digest_sha }}"]}],
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["repositories"][0]["tags"] == ["foo-123456"]


def test_process_component_uses_component_timestamp_format_override() -> None:
    """A component-level timestampFormat overrides the mapping default."""
    component = _base_component(timestampFormat="%Y-%m-%d")
    captured: list[tuple[str, str]] = []

    def format_date_fn(value: str, fmt: str) -> str:
        captured.append((value, fmt))
        return f"{value}|{fmt}"

    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=format_date_fn,
    )
    assert ("20240101 00:00:00", "%Y-%m-%d") in captured


def test_process_component_inspect_failure_raises() -> None:
    """A failing skopeo inspect call raises ``RuntimeError``."""
    component = _base_component()

    def inspect_fn(ref: str, **kwargs: Any) -> Any:
        return _completed(returncode=1, stderr="boom")

    with pytest.raises(RuntimeError, match="skopeo inspect failed"):
        apply_mapping.process_component(
            component,
            default_tags=[],
            default_timestamp_format="%s",
            current_timestamp="20240101 00:00:00",
            default_cgw_settings={},
            add_implicit_timestamp_tag=False,
            inspect_fn=inspect_fn,
            list_tags_fn=_fake_list_tags({}),
            get_arch_fn=_fake_get_arch(),
            format_date_fn=_fake_format_date,
        )


# ---------------------------------------------------------------------------
# Component-wide Jinja variable substitution
# ---------------------------------------------------------------------------


def test_process_component_jinja_substitution_in_version_field() -> None:
    """Jinja variables in the version field are substituted."""
    component = _base_component(
        source={"git": {"revision": "abcdef1234567890"}},
        version="{{ git_short_sha }}",
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["version"] == "abcdef1"


def test_process_component_jinja_substitution_in_source_context() -> None:
    """Jinja variables in source.git.context are substituted."""
    component = _base_component(
        source={"git": {"revision": "abc123", "context": "path/{{ git_short_sha }}"}},
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["source"]["git"]["context"] == "path/abc123"


def test_process_component_jinja_substitution_multiple_variables() -> None:
    """Multiple variable types are all substituted correctly."""
    component = _base_component(
        source={"git": {"revision": "def456"}},
        version="{{ git_short_sha }}-{{ digest_sha }}",
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["version"] == "def456-abcdef1234567890"


def test_process_component_jinja_substitution_oci_version_in_version_field() -> None:
    """OCI version variable is substituted in version field."""
    component = _base_component(
        version="v{{ oci_version }}",
    )
    raw_manifest = {
        "annotations": {"org.opencontainers.image.version": "1.2.3+build"},
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json"},
    }
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect(raw_manifest, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["version"] == "v1.2.3_build"


def test_process_component_jinja_substitution_timestamp_in_unstable_fields() -> None:
    """Timestamp variable can be used in unstableFields."""
    component = _base_component()
    component["unstableFields"] = {"buildTime": "{{ timestamp }}"}
    raw_manifest = {
        "annotations": {"org.opencontainers.image.created": "2024-01-15"},
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json"},
    }
    standard = {"Labels": {"build-date": "2024-01-15T10:00:00Z"}}
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%Y-%m-%d",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect(raw_manifest, standard),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["unstableFields"]["buildTime"] == "2024-01-15T10:00:00Z|%Y-%m-%d"


def test_process_component_jinja_substitution_release_timestamp_in_contentgateway() -> None:
    """Release timestamp variable is substituted in contentGateway fields."""
    component = _base_component(
        contentGateway={"releaseId": "release-{{ release_timestamp }}"},
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%Y%m%d",
        current_timestamp="20250831 12:34:56",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["contentGateway"]["releaseId"] == "release-20250831 12:34:56|%Y%m%d"


def test_process_component_jinja_substitution_all_substitute_map_variables() -> None:
    """All substitute_map variables are available in component fields."""
    component = _base_component(
        source={"git": {"revision": "abcdef1234567890"}},
    )
    component["unstableFields"] = {
        "gitSha": "{{ git_sha }}",
        "gitShortSha": "{{ git_short_sha }}",
        "digestSha": "{{ digest_sha }}",
        "ociVersion": "{{ oci_version }}",
        "buildTimestamp": "{{ timestamp }}",
        "releaseTimestamp": "{{ release_timestamp }}",
    }
    raw_manifest = {
        "annotations": {
            "org.opencontainers.image.version": "2.0.0",
            "org.opencontainers.image.created": "2024-01-15T00:00:00Z",
        },
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json"},
    }
    standard = {"Labels": {"build-date": "2024-01-15T10:30:00Z"}}
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%Y-%m-%d",
        current_timestamp="20250831 12:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect(raw_manifest, standard),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["unstableFields"]["gitSha"] == "abcdef1234567890"
    assert component["unstableFields"]["gitShortSha"] == "abcdef1"
    assert component["unstableFields"]["digestSha"] == "abcdef1234567890"
    assert component["unstableFields"]["ociVersion"] == "2.0.0"
    assert component["unstableFields"]["buildTimestamp"] == "2024-01-15T10:30:00Z|%Y-%m-%d"
    assert component["unstableFields"]["releaseTimestamp"] == "20250831 12:00:00|%Y-%m-%d"


def test_process_component_jinja_substitution_preserves_non_template_fields() -> None:
    """Fields without Jinja variables are left unchanged."""
    component = _base_component(
        source={"git": {"revision": "abc123", "url": "https://github.com/org/repo.git"}},
        version="1.0.0",
    )
    # Add a field with Jinja to verify substitution still works
    component["unstableFields"] = {"custom": "release-{{ git_sha }}"}

    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    # Non-template fields preserved
    assert component["source"]["git"]["url"] == "https://github.com/org/repo.git"
    assert component["version"] == "1.0.0"
    # Template field substituted
    assert component["unstableFields"]["custom"] == "release-abc123"


def test_process_component_jinja_substitution_combined_with_tags_and_staged() -> None:
    """Jinja substitution works alongside tag and staged file translation."""
    component = _base_component(
        source={"git": {"revision": "commit123"}},
        repositories=[{"url": "registry.io/dest", "tags": ["tag-{{ git_short_sha }}"]}],
        staged={"files": [{"filename": "release-{{ git_short_sha }}.yaml"}]},
        version="{{ git_short_sha }}",
    )
    apply_mapping.process_component(
        component,
        default_tags=[],
        default_timestamp_format="%s",
        current_timestamp="20240101 00:00:00",
        default_cgw_settings={},
        add_implicit_timestamp_tag=False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
    )
    assert component["repositories"][0]["tags"] == ["tag-commit1"]
    assert component["staged"]["files"][0]["filename"] == "release-commit1.yaml"
    assert component["version"] == "commit1"


# ---------------------------------------------------------------------------
# process_components
# ---------------------------------------------------------------------------


def test_process_components_applies_mapping_defaults() -> None:
    """Mapping-level defaults.tags/contentGateway are applied to all components."""
    snapshot = {
        "components": [
            _base_component(name="comp1", repositories=[{"url": "repo1", "tags": []}]),
        ]
    }
    mapping = {"defaults": {"tags": ["default-tag"], "contentGateway": {"publish": True}}}
    apply_mapping.process_components(
        snapshot,
        mapping,
        False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
        current_timestamp_fn=lambda: "NOW",
    )
    component = snapshot["components"][0]
    assert component["repositories"][0]["tags"] == ["default-tag"]
    assert component["contentGateway"] == {"publish": True}


def test_process_components_incrementer_cache_reset_per_component() -> None:
    """Each component gets an independent component-incrementer cache."""
    snapshot = {
        "components": [
            _base_component(
                name="comp1",
                repositories=[{"url": "repo-a", "tags": ["v1-{{ component-incrementer }}"]}],
            ),
            _base_component(
                name="comp2",
                repositories=[{"url": "repo-a", "tags": ["v1-{{ component-incrementer }}"]}],
            ),
        ]
    }
    mapping: dict = {}
    apply_mapping.process_components(
        snapshot,
        mapping,
        False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({"repo-a": ["v1-2"]}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
        current_timestamp_fn=lambda: "NOW",
    )
    assert snapshot["components"][0]["repositories"][0]["tags"] == ["v1-3"]
    assert snapshot["components"][1]["repositories"][0]["tags"] == ["v1-3"]


def test_process_components_no_components_is_a_noop() -> None:
    """An empty components list does not raise."""
    snapshot: dict = {"components": []}
    apply_mapping.process_components(
        snapshot,
        {},
        False,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
        current_timestamp_fn=lambda: "NOW",
    )
    assert snapshot["components"] == []


# ---------------------------------------------------------------------------
# apply_mapping (top-level orchestration)
# ---------------------------------------------------------------------------


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def test_apply_mapping_missing_snapshot_file_raises(tmp_path: Path) -> None:
    """A missing snapshot file raises ``FileNotFoundError``."""
    with pytest.raises(FileNotFoundError):
        apply_mapping.apply_mapping(tmp_path / "missing.json", None)


def test_apply_mapping_no_data_file_returns_false(tmp_path: Path) -> None:
    """A missing data file backs up the snapshot and returns False, unchanged."""
    snapshot_path = tmp_path / "snapshot.json"
    original = {"components": [{"name": "comp1"}]}
    _write_json(snapshot_path, original)

    result = apply_mapping.apply_mapping(snapshot_path, None)

    assert result is False
    assert json.loads(snapshot_path.read_text()) == original
    orig_path = tmp_path / "snapshot.json.orig"
    assert orig_path.is_file()
    assert json.loads(orig_path.read_text()) == original


def test_apply_mapping_data_file_not_a_file_returns_false(tmp_path: Path) -> None:
    """A data path pointing at a non-existent file behaves like no data file."""
    snapshot_path = tmp_path / "snapshot.json"
    _write_json(snapshot_path, {"components": []})
    result = apply_mapping.apply_mapping(snapshot_path, tmp_path / "does-not-exist.json")
    assert result is False


def test_apply_mapping_no_mapping_key_returns_false(tmp_path: Path) -> None:
    """A data file with no 'mapping' key returns False, snapshot unchanged."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    original = {"components": [{"name": "comp1"}]}
    _write_json(snapshot_path, original)
    _write_json(data_path, {"not_mapping": {}})

    result = apply_mapping.apply_mapping(snapshot_path, data_path)

    assert result is False
    assert json.loads(snapshot_path.read_text()) == original


def test_apply_mapping_empty_result_without_fail_on_empty(tmp_path: Path) -> None:
    """No overlapping components produces an empty result without raising by default."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    _write_json(snapshot_path, {"components": [{"name": "not-in-mapping"}]})
    _write_json(data_path, {"mapping": {"components": [{"name": "not-in-snapshot"}]}})

    result = apply_mapping.apply_mapping(snapshot_path, data_path)

    assert result is True
    assert json.loads(snapshot_path.read_text())["components"] == []


def test_apply_mapping_empty_result_with_fail_on_empty_raises(tmp_path: Path) -> None:
    """fail_on_empty_result=True raises when the merge produces zero components."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    _write_json(snapshot_path, {"components": [{"name": "not-in-mapping"}]})
    _write_json(data_path, {"mapping": {"components": [{"name": "not-in-snapshot"}]}})

    with pytest.raises(ValueError, match="Resulting snapshot contains 0 components"):
        apply_mapping.apply_mapping(snapshot_path, data_path, fail_on_empty_result=True)


def test_apply_mapping_full_merge_writes_snapshot(tmp_path: Path) -> None:
    """A full merge processes components and writes the result back to snapshot_path."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    _write_json(
        snapshot_path,
        {
            "components": [
                {"name": "comp1", "containerImage": "registry.io/repo@sha256:abc123"}
            ]
        },
    )
    _write_json(
        data_path,
        {
            "mapping": {
                "components": [
                    {
                        "name": "comp1",
                        "repositories": [{"url": "registry.io/dest", "tags": ["static"]}],
                    }
                ]
            }
        },
    )

    result = apply_mapping.apply_mapping(
        snapshot_path,
        data_path,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
        current_timestamp_fn=lambda: "NOW",
    )

    assert result is True
    snapshot = json.loads(snapshot_path.read_text())
    assert snapshot["components"][0]["repositories"][0]["tags"] == ["static"]


def test_apply_mapping_fail_on_empty_result_false_with_components_does_not_raise(
    tmp_path: Path,
) -> None:
    """fail_on_empty_result=True does not raise when the result is non-empty."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    _write_json(
        snapshot_path,
        {"components": [{"name": "comp1", "containerImage": "registry.io/repo@sha256:abc"}]},
    )
    _write_json(
        data_path,
        {
            "mapping": {
                "components": [
                    {"name": "comp1", "repositories": [{"url": "registry.io/dest"}]}
                ]
            }
        },
    )

    result = apply_mapping.apply_mapping(
        snapshot_path,
        data_path,
        fail_on_empty_result=True,
        inspect_fn=_fake_inspect({}, {}),
        list_tags_fn=_fake_list_tags({}),
        get_arch_fn=_fake_get_arch(),
        format_date_fn=_fake_format_date,
        current_timestamp_fn=lambda: "NOW",
    )
    assert result is True


# ---------------------------------------------------------------------------
# CLI: _parse_args / main
# ---------------------------------------------------------------------------


def test_parse_args_defaults() -> None:
    """Boolean flags default to False when omitted."""
    args = apply_mapping._parse_args(
        ["--snapshot-file", "snap.json", "--data-file", "data.json"]
    )
    assert args.snapshot_file == "snap.json"
    assert args.data_file == "data.json"
    assert args.fail_on_empty_result is False
    assert args.add_implicit_timestamp_tag is False


def test_parse_args_boolean_flags_parsed() -> None:
    """String 'true'/'false' values are parsed to actual booleans, case-insensitively."""
    args = apply_mapping._parse_args(
        [
            "--snapshot-file",
            "snap.json",
            "--data-file",
            "data.json",
            "--fail-on-empty-result",
            "True",
            "--add-implicit-timestamp-tag",
            "TRUE",
        ]
    )
    assert args.fail_on_empty_result is True
    assert args.add_implicit_timestamp_tag is True


def test_parse_args_missing_required_raises_system_exit() -> None:
    """Missing required arguments cause argparse to exit."""
    with pytest.raises(SystemExit):
        apply_mapping._parse_args(["--snapshot-file", "snap.json"])


def test_main_missing_result_mapped_env_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without RESULT_MAPPED set, main() exits via tekton.result_paths_from_env."""
    monkeypatch.delenv("RESULT_MAPPED", raising=False)
    monkeypatch.delenv("CA_CERT_PATH", raising=False)
    with pytest.raises(SystemExit):
        apply_mapping.main(["--snapshot-file", "snap.json", "--data-file", "data.json"])


def test_main_writes_mapped_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() wires CLI args and env vars together and writes the mapped result."""
    monkeypatch.delenv("CA_CERT_PATH", raising=False)
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    result_path = tmp_path / "result.txt"
    _write_json(snapshot_path, {"components": []})
    monkeypatch.setenv("RESULT_MAPPED", str(result_path))

    exit_code = apply_mapping.main(
        [
            "--snapshot-file",
            str(snapshot_path),
            "--data-file",
            str(data_path),
        ]
    )

    assert exit_code == 0
    assert result_path.read_text(encoding="utf-8") == "false"


def test_main_calls_setup_ca_cert(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() calls setup_ca_cert() before doing any work."""
    monkeypatch.delenv("CA_CERT_PATH", raising=False)
    snapshot_path = tmp_path / "snapshot.json"
    result_path = tmp_path / "result.txt"
    _write_json(snapshot_path, {"components": []})
    monkeypatch.setenv("RESULT_MAPPED", str(result_path))

    with mock.patch(f"{TASK}.setup_ca_cert") as setup_mock:
        apply_mapping.main(
            [
                "--snapshot-file",
                str(snapshot_path),
                "--data-file",
                str(tmp_path / "missing-data.json"),
            ]
        )
    setup_mock.assert_called_once()


# ---------------------------------------------------------------------------
# _skopeo_list_repo_tags / _inspect_json (default wiring)
# ---------------------------------------------------------------------------


def test_skopeo_list_repo_tags_success() -> None:
    """Tags are parsed from a successful skopeo list-tags call."""
    with mock.patch(
        f"{TASK}.skopeo.list_tags",
        return_value=_completed(json.dumps({"Tags": ["v1", "v2"]})),
    ):
        assert apply_mapping._skopeo_list_repo_tags("repo") == ["v1", "v2"]


def test_skopeo_list_repo_tags_failure_raises() -> None:
    """A failing skopeo list-tags call raises ``RuntimeError``."""
    with mock.patch(
        f"{TASK}.skopeo.list_tags",
        return_value=_completed(returncode=1, stderr="not found"),
    ):
        with pytest.raises(RuntimeError, match="skopeo list-tags failed"):
            apply_mapping._skopeo_list_repo_tags("repo")


def test_skopeo_list_repo_tags_missing_tags_key_returns_empty_list() -> None:
    """A response with no 'Tags' key returns an empty list."""
    with mock.patch(
        f"{TASK}.skopeo.list_tags",
        return_value=_completed(json.dumps({})),
    ):
        assert apply_mapping._skopeo_list_repo_tags("repo") == []


def test_inspect_json_parses_stdout() -> None:
    """A successful inspect_fn call has its stdout parsed as JSON."""
    inspect_fn = mock.Mock(return_value=_completed(json.dumps({"key": "value"})))
    assert apply_mapping._inspect_json(inspect_fn, "ref", raw=True) == {"key": "value"}
    inspect_fn.assert_called_once_with("ref", raw=True)


def test_inspect_json_failure_raises() -> None:
    """A failing inspect_fn call raises ``RuntimeError`` with the stderr message."""
    inspect_fn = mock.Mock(return_value=_completed(returncode=1, stderr="auth failed"))
    with pytest.raises(RuntimeError, match="auth failed"):
        apply_mapping._inspect_json(inspect_fn, "ref")


# ---------------------------------------------------------------------------
# _get_image_architectures
# ---------------------------------------------------------------------------


def test_get_image_architectures_single_arch() -> None:
    """A single JSON line is parsed into a one-item list."""
    stdout = (
        '{"platform": {"architecture": "amd64", "os": "linux"}, '
        '"digest": "sha256:abc", "multiarch": false}\n'
    )
    with mock.patch(
        f"{TASK}.run_cmd",
        return_value=_completed(stdout=stdout),
    ) as run_mock:
        result = apply_mapping._get_image_architectures("registry.io/repo@sha256:abc")

    assert result == [
        {
            "platform": {"architecture": "amd64", "os": "linux"},
            "digest": "sha256:abc",
            "multiarch": False,
        }
    ]
    cmd = run_mock.call_args[0][0]
    assert cmd == ["get-image-architectures", "registry.io/repo@sha256:abc"]
    assert run_mock.call_args[1]["check"] is False


def test_get_image_architectures_multi_arch() -> None:
    """Multiple newline-delimited JSON objects are all parsed."""
    stdout = (
        '{"platform": {"architecture": "amd64", "os": "linux"}, '
        '"digest": "sha256:aaa", "multiarch": true}\n'
        '{"platform": {"architecture": "arm64", "os": "linux"}, '
        '"digest": "sha256:bbb", "multiarch": true}\n'
    )
    with mock.patch(
        f"{TASK}.run_cmd",
        return_value=_completed(stdout=stdout),
    ):
        result = apply_mapping._get_image_architectures("registry.io/repo@sha256:idx")

    assert len(result) == 2
    assert result[0]["platform"]["architecture"] == "amd64"
    assert result[1]["platform"]["architecture"] == "arm64"


def test_get_image_architectures_skips_blank_lines() -> None:
    """Blank lines in the output are ignored."""
    stdout = (
        '{"platform": {"architecture": "amd64", "os": "linux"}, "digest": "sha256:aaa"}\n\n'
    )
    with mock.patch(
        f"{TASK}.run_cmd",
        return_value=_completed(stdout=stdout),
    ):
        result = apply_mapping._get_image_architectures("registry.io/repo@sha256:aaa")

    assert len(result) == 1


def test_get_image_architectures_custom_retry_times() -> None:
    """``retry_times`` is forwarded as ``--skopeo-retries``."""
    with mock.patch(
        f"{TASK}.run_cmd",
        return_value=_completed(stdout="{}\n"),
    ) as run_mock:
        apply_mapping._get_image_architectures("img:v1", retry_times=5)

    cmd = run_mock.call_args[0][0]
    assert cmd == ["get-image-architectures", "--skopeo-retries", "5", "img:v1"]


def test_get_image_architectures_no_retry_flag_by_default() -> None:
    """No ``--skopeo-retries`` flag is added unless explicitly requested."""
    with mock.patch(
        f"{TASK}.run_cmd",
        return_value=_completed(stdout="{}\n"),
    ) as run_mock:
        apply_mapping._get_image_architectures("img:v1")

    cmd = run_mock.call_args[0][0]
    assert "--skopeo-retries" not in cmd


def test_get_image_architectures_failure_raises() -> None:
    """A non-zero exit code raises ``RuntimeError`` with the stderr message."""
    with mock.patch(
        f"{TASK}.run_cmd",
        return_value=_completed(returncode=1, stdout=""),
    ):
        with pytest.raises(RuntimeError, match="get-image-architectures failed"):
            apply_mapping._get_image_architectures("img:v1")


def test_get_image_architectures_failure_includes_stderr() -> None:
    """The raised error message includes the underlying stderr text."""
    with mock.patch(
        f"{TASK}.run_cmd",
        return_value=_completed(returncode=1, stderr="boom: auth failed"),
    ):
        with pytest.raises(RuntimeError, match="boom: auth failed"):
            apply_mapping._get_image_architectures("img:v1")
