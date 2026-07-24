"""Test check_data_keys task logic."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from release_service_utils.tasks.managed import check_data_keys

REPO_ROOT = Path(__file__).resolve().parents[5]
SCHEMA_PATH = REPO_ROOT / "schemas" / "dataKeys.json"


@pytest.fixture(scope="session")
def schema_path() -> Path:
    """Return the repo dataKeys schema used by validation tests."""
    if not SCHEMA_PATH.is_file():
        pytest.fail(f"missing dataKeys schema: {SCHEMA_PATH}")
    return SCHEMA_PATH


def _valid_release_notes() -> dict[str, Any]:
    """Minimal releaseNotes block that satisfies the dataKeys schema."""
    return {
        "product_id": [123],
        "product_name": "Red Hat Openstack Product",
        "product_version": "1.2.3",
        "product_stream": "rhtas-tp1",
        "cpe": "cpe:/a:example:openstack:el8",
        "type": "RHSA",
        "issues": {
            "fixed": [
                {
                    "id": "RHOSP-12345",
                    "source": "issues.example.com",
                    "summary": "some text about the issue",
                },
                {"id": "1234567", "source": "bugzilla.example.com"},
            ],
        },
        "content": {
            "images": [
                {
                    "containerImage": "quay.io/example/openstack@sha256:abcde",
                    "repository": "rhosp16-rhel8/openstack",
                    "tags": ["latest"],
                    "architecture": "amd64",
                    "signingKey": "abcde",
                    "purl": (
                        "pkg:example/openstack@sha256:abcde?"
                        "repository_url=quay.io/example/rhosp16-rhel8"
                    ),
                },
            ],
        },
        "cves": [{"key": "CVE-2025-12345", "component": "my-component-1"}],
        "synopsis": "test synopsis",
        "topic": "test topic",
        "description": "test description",
        "solution": "test solution",
        "references": ["https://docs.example.com/some/example/release-notes"],
    }


def _write_data(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data) + "\n", encoding="utf-8")


def test_default_schema_path_is_image_location() -> None:
    """Default schema path matches the location baked into the utils image."""
    assert check_data_keys.DEFAULT_SCHEMA_PATH == Path("/home/schemas/dataKeys.json")


def test_resolve_schema_path_returns_existing_file(tmp_path: Path) -> None:
    """Return the path when the schema file exists."""
    schema = tmp_path / "schema.json"
    schema.write_text("{}", encoding="utf-8")
    assert check_data_keys.resolve_schema_path(schema) == schema


def test_resolve_schema_path_missing_file(tmp_path: Path) -> None:
    """Fail when the schema file does not exist."""
    missing = tmp_path / "missing-schema.json"
    with pytest.raises(FileNotFoundError, match="schema file not found"):
        check_data_keys.resolve_schema_path(missing)


def test_parse_systems_param_empty_string() -> None:
    """Treat a blank systems param as an empty array."""
    assert check_data_keys.parse_systems_param("") == []
    assert check_data_keys.parse_systems_param("   ") == []


def test_parse_systems_param_invalid_type_raises() -> None:
    """Reject systems params that are not JSON arrays."""
    with pytest.raises(ValueError, match="JSON array"):
        check_data_keys.parse_systems_param('{"systemName": "cdn"}')


def test_merge_systems_into_data_appends_entries() -> None:
    """Append required systems to an existing systems array."""
    data = {"systems": [{"systemName": "cdn", "dynamic": False}]}
    merged = check_data_keys.merge_systems_into_data(
        data,
        [{"systemName": "releaseNotes", "dynamic": False}],
    )
    assert merged["systems"] == [
        {"systemName": "cdn", "dynamic": False},
        {"systemName": "releaseNotes", "dynamic": False},
    ]


def test_merge_systems_into_data_rejects_non_array_systems() -> None:
    """Reject data files whose systems value is not a JSON array."""
    with pytest.raises(ValueError, match="data systems must be a JSON array"):
        check_data_keys.merge_systems_into_data(
            {"systems": "oops"},
            [{"systemName": "cdn", "dynamic": False}],
        )


def test_run_check_data_keys_rejects_malformed_systems_in_data(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Fail when the data file contains a non-array systems value."""
    data_dir = tmp_path / "data"
    data_file = data_dir / "data.json"
    _write_data(data_file, {"systems": "oops"})
    with pytest.raises(ValueError, match="data systems must be a JSON array"):
        check_data_keys.run_check_data_keys(
            data_dir=data_dir,
            data_path=Path("data.json"),
            schema_path=schema_path,
            systems_json='[{"systemName": "cdn", "dynamic": false}]',
        )


def test_module_main_guard(monkeypatch: pytest.MonkeyPatch, schema_path: Path) -> None:
    """Executing the module as `__main__` propagates failures from main()."""
    import runpy

    monkeypatch.setenv("PARAM_DATA_DIR", "/tmp")
    monkeypatch.setenv("PARAM_DATA_PATH", "missing.json")
    monkeypatch.setenv("SCHEMA_FILE", str(schema_path))
    with pytest.raises(FileNotFoundError, match="No data JSON was provided"):
        runpy.run_module(
            "release_service_utils.tasks.managed" ".check_data_keys.check_data_keys",
            run_name="__main__",
        )


def test_run_check_data_keys_happy_path(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Validate complete data when all required systems are present."""
    data_dir = tmp_path / "data"
    data_file = data_dir / "run/data.json"
    _write_data(
        data_file,
        {
            "releaseNotes": _valid_release_notes(),
            "cdn": {"env": "qa"},
            "intention": "production",
        },
    )
    check_data_keys.run_check_data_keys(
        data_dir=data_dir,
        data_path=Path("run/data.json"),
        schema_path=schema_path,
        systems_json=(
            '[{"systemName": "releaseNotes", "dynamic": false},'
            '{"systemName": "cdn", "dynamic": false},'
            '{"systemName": "intention", "dynamic": false}]'
        ),
    )
    saved = json.loads(data_file.read_text(encoding="utf-8"))
    assert len(saved["systems"]) == 3


def test_run_check_data_keys_missing_data_file(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Fail when the data JSON path does not exist."""
    with pytest.raises(FileNotFoundError, match="No data JSON was provided"):
        check_data_keys.run_check_data_keys(
            data_dir=tmp_path,
            data_path=Path("missing.json"),
            schema_path=schema_path,
            systems_json="[]",
        )


def test_run_check_data_keys_missing_releasenotes_key(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Fail when releaseNotes is required but missing product_id."""
    data_dir = tmp_path / "data"
    data_file = data_dir / "data.json"
    release_notes = _valid_release_notes()
    del release_notes["product_id"]
    _write_data(data_file, {"releaseNotes": release_notes})
    with pytest.raises(ValueError, match="schema validation failed"):
        check_data_keys.run_check_data_keys(
            data_dir=data_dir,
            data_path=Path("data.json"),
            schema_path=schema_path,
            systems_json='[{"systemName": "releaseNotes", "dynamic": false}]',
        )


def test_run_check_data_keys_missing_cdn_key(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Fail when cdn is required but absent from the data file."""
    data_dir = tmp_path / "data"
    data_file = data_dir / "data.json"
    _write_data(data_file, {"releaseNotes": _valid_release_notes()})
    with pytest.raises(ValueError, match="schema validation failed"):
        check_data_keys.run_check_data_keys(
            data_dir=data_dir,
            data_path=Path("data.json"),
            schema_path=schema_path,
            systems_json='[{"systemName": "releaseNotes", "dynamic": false},'
            '{"systemName": "cdn", "dynamic": false}]',
        )


def test_run_check_data_keys_dynamic_true_missing_data_passes(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Pass when a dynamic system is declared but its data is absent."""
    data_dir = tmp_path / "data"
    data_file = data_dir / "data.json"
    _write_data(data_file, {"someOtherData": {"value": "test"}})
    check_data_keys.run_check_data_keys(
        data_dir=data_dir,
        data_path=Path("data.json"),
        schema_path=schema_path,
        systems_json='[{"systemName": "releaseNotes", "dynamic": true}]',
    )


def test_run_check_data_keys_dynamic_false_missing_data_fails(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Fail when a non-dynamic system is declared but its data is absent."""
    data_dir = tmp_path / "data"
    data_file = data_dir / "data.json"
    _write_data(data_file, {"someOtherData": {"value": "test"}})
    with pytest.raises(ValueError, match="schema validation failed"):
        check_data_keys.run_check_data_keys(
            data_dir=data_dir,
            data_path=Path("data.json"),
            schema_path=schema_path,
            systems_json='[{"systemName": "releaseNotes", "dynamic": false}]',
        )


def test_run_check_data_keys_pass_undeclared_system(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Pass when releaseNotes data exists but releaseNotes is not required."""
    data_dir = tmp_path / "data"
    data_file = data_dir / "data.json"
    _write_data(
        data_file,
        {
            "releaseNotes": {
                "product_stream": "test-product-stream",
                "synopsis": "test synopsis",
            },
            "cdn": {"env": "qa"},
        },
    )
    check_data_keys.run_check_data_keys(
        data_dir=data_dir,
        data_path=Path("data.json"),
        schema_path=schema_path,
        systems_json='[{"systemName": "cdn", "dynamic": false}]',
    )


def test_run_check_data_keys_malformed_cve_key(
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Fail when a CVE entry is missing its required component field."""
    data_dir = tmp_path / "data"
    data_file = data_dir / "data.json"
    release_notes = _valid_release_notes()
    release_notes["cves"] = [{"key": "CVE-2022-1234"}]
    _write_data(data_file, {"releaseNotes": release_notes})
    with pytest.raises(ValueError, match="schema validation failed"):
        check_data_keys.run_check_data_keys(
            data_dir=data_dir,
            data_path=Path("data.json"),
            schema_path=schema_path,
            systems_json='[{"systemName": "releaseNotes", "dynamic": false}]',
        )


def test_main_failure_propagates_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Propagate failures from main() with the original exception message."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_DATA_PATH", "missing.json")
    monkeypatch.setenv("SCHEMA_FILE", str(schema_path))
    with pytest.raises(FileNotFoundError, match="No data JSON was provided"):
        check_data_keys.main()


def test_main_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    schema_path: Path,
) -> None:
    """Exit zero after a successful validation run."""
    data_file = tmp_path / "data.json"
    _write_data(data_file, {"cdn": {"env": "qa"}})
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("SCHEMA_FILE", str(schema_path))
    monkeypatch.setenv(
        "PARAM_SYSTEMS",
        '[{"systemName": "cdn", "dynamic": false}]',
    )
    assert check_data_keys.main() == 0
