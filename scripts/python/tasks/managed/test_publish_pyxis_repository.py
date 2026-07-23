"""Tests for `publish_pyxis_repository` task logic."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import publish_pyxis_repository


def _snapshot(
    *,
    components: list[dict[str, object]],
    component_group: str = "my-group",
) -> dict[str, object]:
    return {"componentGroup": component_group, "components": components}


def _repo(url: str) -> dict[str, str]:
    return {"url": url}


def test_resolve_pyxis_api_url_prefers_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use PYXIS_URL when the test harness sets a mock server URL."""
    monkeypatch.setenv("PYXIS_URL", "http://127.0.0.1:8080/v1")
    assert publish_pyxis_repository.resolve_pyxis_api_url() == "http://127.0.0.1:8080/v1"


def test_build_publish_payload_with_and_without_source() -> None:
    """Include source_container_image_enabled only when requested."""
    assert publish_pyxis_repository.build_publish_payload(False) == {
        "published": True,
    }
    assert publish_pyxis_repository.build_publish_payload(True) == {
        "published": True,
        "source_container_image_enabled": True,
    }


def test_should_add_sign_registry_access_only_for_non_terms() -> None:
    """Add sign-registry entries only for standard repos without terms."""
    assert publish_pyxis_repository.should_add_sign_registry_access(
        "registry.access.redhat.com",
        False,
    )
    assert not publish_pyxis_repository.should_add_sign_registry_access(
        "registry.access.redhat.com",
        True,
    )
    assert not publish_pyxis_repository.should_add_sign_registry_access(
        "flatpaks.registry.redhat.io",
        False,
    )


def test_should_patch_repository_skip_and_publish_on_push() -> None:
    """PATCH only when publishing is enabled and publish_on_push is true."""
    assert (
        publish_pyxis_repository.should_patch_repository(
            skip_publishing=True,
            publish_on_push=True,
            pyxis_registry="registry.access.redhat.com",
            pyxis_repo="p/i",
        )
        is False
    )
    assert (
        publish_pyxis_repository.should_patch_repository(
            skip_publishing=False,
            publish_on_push=False,
            pyxis_registry="registry.access.redhat.com",
            pyxis_repo="p/i",
        )
        is False
    )
    assert (
        publish_pyxis_repository.should_patch_repository(
            skip_publishing=False,
            publish_on_push=True,
            pyxis_registry="registry.access.redhat.com",
            pyxis_repo="p/i",
        )
        is True
    )


def test_should_record_catalog_url() -> None:
    """Record catalog URL when already published or when PATCH runs."""
    assert publish_pyxis_repository.should_record_catalog_url(
        repository_published=True,
        should_patch=False,
    )
    assert publish_pyxis_repository.should_record_catalog_url(
        repository_published=False,
        should_patch=True,
    )
    assert not publish_pyxis_repository.should_record_catalog_url(
        repository_published=False,
        should_patch=False,
    )


def test_publish_repositories_happy_path(tmp_path: Path) -> None:
    """Publish standard repos and record catalog URLs for each component."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_get(
        _api: str,
        registry: str,
        repository: str,
        *,
        cert: tuple[str, str],
    ) -> dict[str, object]:
        return {
            "_id": repository.split("/")[-1].replace("image", ""),
            "publish_on_push": True,
            "requires_terms": True,
        }

    def fake_patch(
        _api: str,
        repository_id: str,
        payload: dict[str, object],
        *,
        cert: tuple[str, str],
    ) -> None:
        calls.append((repository_id, payload))

    snapshot = _snapshot(
        components=[
            {
                "name": "component1",
                "repositories": [
                    _repo("quay.io/redhat-prod/my-product----my-image1"),
                    _repo("quay.io/redhat-prod/my-product----my-image2"),
                ],
            },
            {
                "name": "component3",
                "repositories": [_repo("quay.io/redhat-prod/my-product----my-image3")],
            },
        ],
    )

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            side_effect=fake_get,
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
            side_effect=fake_patch,
        ),
    ):
        results = publish_pyxis_repository.publish_repositories(
            snapshot=snapshot,
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=False,
            default_push_source_container=False,
        )

    assert len(results["catalog_urls"]) == 3
    assert sign_file.read_text(encoding="utf-8") == ""
    assert len(calls) == 3


def test_publish_repositories_no_terms_required_sign_list(tmp_path: Path) -> None:
    """Add repos with requires_terms=false to sign-registry-access.txt."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")

    def fake_get(
        _api: str,
        _registry: str,
        repository: str,
        *,
        cert: tuple[str, str],
    ) -> dict[str, object]:
        requires_terms = "my-image5" not in repository and "my-image6" not in repository
        return {
            "_id": "1",
            "publish_on_push": True,
            "requires_terms": requires_terms,
        }

    snapshot = _snapshot(
        components=[
            {
                "name": "component2",
                "repositories": [
                    _repo("quay.io/redhat-prod/my-product----my-image5"),
                    _repo("quay.io/redhat-prod/my-product----my-image6"),
                ],
            },
        ],
    )

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            side_effect=fake_get,
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
        ),
    ):
        publish_pyxis_repository.publish_repositories(
            snapshot=snapshot,
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=False,
            default_push_source_container=False,
        )

    lines = sign_file.read_text(encoding="utf-8").splitlines()
    assert "my-product/my-image5" in lines
    assert "my-product/my-image6" in lines


def test_publish_repositories_skip_publishing_skips_patch(tmp_path: Path) -> None:
    """Still query Pyxis but skip PATCH when skipRepoPublishing is true."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")
    patched = {"count": 0}

    def fake_patch(*_args: object, **_kwargs: object) -> None:
        patched["count"] += 1

    snapshot = _snapshot(
        components=[
            {"name": "c", "repositories": [_repo("quay.io/redhat-prod/p----i1")]},
        ],
    )
    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={
                "_id": "1",
                "publish_on_push": True,
                "published": False,
                "requires_terms": True,
            },
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
            side_effect=fake_patch,
        ),
    ):
        results = publish_pyxis_repository.publish_repositories(
            snapshot=snapshot,
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=True,
            default_push_source_container=False,
        )

    assert results["catalog_urls"] == []
    assert patched["count"] == 0


def test_publish_repositories_skip_publishing_records_already_published(
    tmp_path: Path,
) -> None:
    """Skip PATCH but still record catalog URL when Pyxis reports published."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={
                "_id": "1",
                "publish_on_push": True,
                "published": True,
                "requires_terms": True,
            },
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
            side_effect=lambda *_a, **_k: pytest.fail("patch should not run"),
        ),
    ):
        results = publish_pyxis_repository.publish_repositories(
            snapshot=_snapshot(
                components=[
                    {
                        "name": "c",
                        "repositories": [_repo("quay.io/redhat-prod/p----i1")],
                    },
                ],
            ),
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=True,
            default_push_source_container=False,
        )

    assert len(results["catalog_urls"]) == 1
    assert results["catalog_urls"][0]["name"] == "c"


def test_publish_repositories_publish_on_push_false_skips_patch(tmp_path: Path) -> None:
    """Skip PATCH when publish_on_push is false and repo is not yet published."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={
                "_id": "1",
                "publish_on_push": False,
                "published": False,
                "requires_terms": True,
            },
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
            side_effect=lambda *_a, **_k: pytest.fail("patch should not run"),
        ),
    ):
        results = publish_pyxis_repository.publish_repositories(
            snapshot=_snapshot(
                components=[
                    {
                        "repositories": [
                            _repo("quay.io/redhat-prod/my-product----my-image0"),
                        ],
                    },
                ],
            ),
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=False,
            default_push_source_container=False,
        )

    assert results["catalog_urls"] == []


def test_publish_repositories_publish_on_push_false_records_already_published(
    tmp_path: Path,
) -> None:
    """Skip PATCH but record catalog URL when repo is already published."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={
                "_id": "1",
                "publish_on_push": False,
                "published": True,
                "requires_terms": True,
            },
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
            side_effect=lambda *_a, **_k: pytest.fail("patch should not run"),
        ),
    ):
        results = publish_pyxis_repository.publish_repositories(
            snapshot=_snapshot(
                components=[
                    {
                        "name": "c",
                        "repositories": [
                            _repo("quay.io/redhat-prod/my-product----my-image0"),
                        ],
                    },
                ],
            ),
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=False,
            default_push_source_container=False,
        )

    assert len(results["catalog_urls"]) == 1


def test_publish_repositories_missing_repository_id_raises(tmp_path: Path) -> None:
    """Fail when Pyxis GET does not return a repository _id."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={"detail": "not found"},
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
        ),
    ):
        with pytest.raises(ValueError, match="Unable to get Container Repository"):
            publish_pyxis_repository.publish_repositories(
                snapshot=_snapshot(
                    components=[
                        {
                            "repositories": [
                                _repo("quay.io/redhat-prod/my-product----my-image9"),
                            ],
                        },
                    ],
                ),
                pyxis_api_url="https://pyxis/v1",
                cert=("/tmp/cert", "/tmp/key"),
                sign_registry_access_file=sign_file,
                skip_publishing=False,
                default_push_source_container=False,
            )


def test_publish_repositories_source_container_payload(tmp_path: Path) -> None:
    """PATCH payload includes source_container_image_enabled when enabled."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")
    captured: list[dict[str, object]] = []

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={
                "_id": "1",
                "publish_on_push": True,
                "requires_terms": True,
            },
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
            side_effect=lambda _a, _b, payload, **_k: captured.append(payload),
        ),
    ):
        publish_pyxis_repository.publish_repositories(
            snapshot=_snapshot(
                components=[
                    {
                        "name": "c",
                        "pushSourceContainer": True,
                        "repositories": [_repo("quay.io/redhat-prod/p----i1")],
                    },
                ],
            ),
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=False,
            default_push_source_container=False,
        )

    assert captured == [
        {"published": True, "source_container_image_enabled": True},
    ]


def test_publish_repositories_flatpak_never_sign_registry(tmp_path: Path) -> None:
    """Flatpak repos publish but never appear in sign-registry-access.txt."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={
                "_id": "9",
                "publish_on_push": True,
                "requires_terms": False,
            },
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
        ),
    ):
        results = publish_pyxis_repository.publish_repositories(
            snapshot=_snapshot(
                components=[
                    {
                        "name": "component1",
                        "repositories": [
                            _repo("quay.io/rh-flatpaks-stage/my-product----my-image1"),
                        ],
                    },
                ],
            ),
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=False,
            default_push_source_container=False,
        )

    assert sign_file.read_text(encoding="utf-8") == ""
    assert results["catalog_urls"][0]["url"].startswith(
        "https://catalog.stage.redhat.com/",
    )


def test_run_publish_pyxis_repository_writes_outputs(tmp_path: Path) -> None:
    """Write results JSON and sign-registry-access Tekton result path."""
    data_dir = tmp_path / "data"
    uid = "run-1"
    snapshot_path = data_dir / uid / "snapshot_spec.json"
    data_path = data_dir / uid / "mydata.json"
    results_dir = data_dir / uid / "results"
    snapshot_path.parent.mkdir(parents=True)
    snapshot_path.write_text(
        json.dumps(
            _snapshot(
                components=[
                    {
                        "name": "component1",
                        "repositories": [
                            _repo("quay.io/redhat-prod/my-product----my-image1"),
                        ],
                    },
                ],
            ),
        ),
        encoding="utf-8",
    )
    data_path.write_text(json.dumps({"mapping": {}}), encoding="utf-8")
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "cert").write_text("cert", encoding="utf-8")
    (secret / "key").write_text("key", encoding="utf-8")
    result_path = tmp_path / "sign-result.txt"

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={
                "_id": "1",
                "publish_on_push": True,
                "requires_terms": True,
            },
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
        ),
    ):
        publish_pyxis_repository.run_publish_pyxis_repository(
            data_dir=data_dir,
            snapshot_path=snapshot_path,
            data_path=data_path,
            results_dir_path=results_dir,
            sign_registry_access_result_path=result_path,
            pyxis_secret_mount=secret,
            pyxis_api_url="https://pyxis/v1",
            task_name="publish-pyxis-repository",
        )

    assert result_path.read_text(encoding="utf-8") == f"{uid}/sign-registry-access.txt"
    sign_file = data_dir / uid / "sign-registry-access.txt"
    assert sign_file.is_file()
    assert not (data_dir / "data" / uid / "sign-registry-access.txt").exists()
    results = json.loads(
        (results_dir / "publish-pyxis-repository-results.json").read_text(
            encoding="utf-8",
        ),
    )
    assert results["catalog_urls"][0]["name"] == "component1"


def test_run_publish_missing_snapshot_raises(tmp_path: Path) -> None:
    """Fail fast when the snapshot file is missing."""
    with pytest.raises(FileNotFoundError):
        publish_pyxis_repository.run_publish_pyxis_repository(
            data_dir=tmp_path,
            snapshot_path=tmp_path / "missing.json",
            data_path=tmp_path / "data.json",
            results_dir_path=tmp_path / "results",
            sign_registry_access_result_path=tmp_path / "result",
            pyxis_secret_mount=tmp_path,
            pyxis_api_url="https://pyxis/v1",
            task_name="task",
        )


def test_run_publish_missing_data_raises(tmp_path: Path) -> None:
    """Fail fast when the data JSON file is missing."""
    snapshot = tmp_path / "snap.json"
    snapshot.write_text("{}", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        publish_pyxis_repository.run_publish_pyxis_repository(
            data_dir=tmp_path,
            snapshot_path=snapshot,
            data_path=tmp_path / "missing-data.json",
            results_dir_path=tmp_path / "results",
            sign_registry_access_result_path=tmp_path / "result",
            pyxis_secret_mount=tmp_path,
            pyxis_api_url="https://pyxis/v1",
            task_name="task",
        )


def test_resolve_pyxis_api_url_from_server_param(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Map PARAM_SERVER to a Pyxis URL when PYXIS_URL is unset."""
    monkeypatch.delenv("PYXIS_URL", raising=False)
    monkeypatch.setenv("PARAM_SERVER", "stage")
    assert publish_pyxis_repository.resolve_pyxis_api_url().endswith(
        "pyxis.preprod.api.redhat.com/v1",
    )


def test_resolve_pyxis_api_url_uses_pyxis_url_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prefer PYXIS_URL over PARAM_SERVER when both are available."""
    monkeypatch.setenv("PYXIS_URL", "https://custom.example/pyxis/")
    monkeypatch.setenv("PARAM_SERVER", "production")
    assert publish_pyxis_repository.resolve_pyxis_api_url() == "https://custom.example/pyxis"


def test_skip_repo_publishing_reads_data_flag() -> None:
    """Return true when pyxis.skipRepoPublishing is set in data JSON."""
    assert publish_pyxis_repository.skip_repo_publishing(
        {"pyxis": {"skipRepoPublishing": True}},
    )


def test_publish_unknown_catalog_prefix_raises(tmp_path: Path) -> None:
    """Fail when a repository URL has an unsupported Quay prefix after PATCH."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")
    patch_called = False

    def track_patch(*_a: Any, **_k: Any) -> None:
        nonlocal patch_called
        patch_called = True

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={
                "_id": "1",
                "publish_on_push": True,
                "published": False,
                "requires_terms": True,
            },
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
            side_effect=track_patch,
        ),
    ):
        with pytest.raises(ValueError, match="Unknown repository prefix"):
            publish_pyxis_repository.publish_repositories(
                snapshot=_snapshot(
                    components=[
                        {
                            "repositories": [_repo("quay.io/unknown/product----image")],
                        },
                    ],
                ),
                pyxis_api_url="https://pyxis/v1",
                cert=("/tmp/cert", "/tmp/key"),
                sign_registry_access_file=sign_file,
                skip_publishing=False,
                default_push_source_container=False,
            )
    assert patch_called


def test_module_main_guard_raises_system_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Executing the file as `__main__` triggers `raise SystemExit(main())`."""
    import runpy

    import publish_pyxis_repository

    monkeypatch.setattr("sys.argv", ["publish_pyxis_repository.py"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(
            str(Path(publish_pyxis_repository.__file__)),
            run_name="__main__",
        )
    assert exc.value.code == 1


def test_publish_repositories_skips_invalid_snapshot_rows(tmp_path: Path) -> None:
    """Ignore malformed component and repository rows without failing."""
    sign_file = tmp_path / "sign-registry-access.txt"
    sign_file.write_text("", encoding="utf-8")

    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            side_effect=lambda *_a, **_k: pytest.fail("get should not run"),
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
        ),
    ):
        results = publish_pyxis_repository.publish_repositories(
            snapshot={
                "components": [
                    "not-a-mapping",
                    {"repositories": "not-a-list"},
                    {"repositories": ["bad", {"url": ""}, {"url": 1}]},
                ],
            },
            pyxis_api_url="https://pyxis/v1",
            cert=("/tmp/cert", "/tmp/key"),
            sign_registry_access_file=sign_file,
            skip_publishing=False,
            default_push_source_container=False,
        )

    assert results["catalog_urls"] == []


def test_publish_repositories_invalid_components_type_raises(tmp_path: Path) -> None:
    """Fail when snapshot components is not a JSON array."""
    sign_file = tmp_path / "sign-registry-access.txt"
    with (
        mock.patch(
            "publish_pyxis_repository.pyxis_api.get_repository_json",
            return_value={},
        ),
        mock.patch(
            "publish_pyxis_repository.pyxis_api.patch_repository_json",
        ),
    ):
        with pytest.raises(ValueError, match="components must be a JSON array"):
            publish_pyxis_repository.publish_repositories(
                snapshot={"components": "bad"},
                pyxis_api_url="https://pyxis/v1",
                cert=("/tmp/cert", "/tmp/key"),
                sign_registry_access_file=sign_file,
                skip_publishing=False,
                default_push_source_container=False,
            )


def test_main_unexpected_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Propagate unexpected exceptions from the workflow."""
    monkeypatch.setenv("PARAM_DATA_DIR", "/tmp")
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
    monkeypatch.setenv("RESULT_SIGN_REGISTRY_ACCESS_PATH", "/tmp/r")
    monkeypatch.setenv("PYXIS_URL", "https://pyxis/v1")

    with mock.patch(
        "publish_pyxis_repository.run_publish_pyxis_repository",
        side_effect=RuntimeError("boom"),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            publish_pyxis_repository.main()


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit zero after a successful run when env vars are set."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
    monkeypatch.setenv("RESULT_SIGN_REGISTRY_ACCESS_PATH", str(tmp_path / "r"))
    monkeypatch.setenv("PARAM_SERVER", "production")
    monkeypatch.setenv("PYXIS_URL", "https://pyxis/v1")

    with mock.patch(
        "publish_pyxis_repository.run_publish_pyxis_repository",
    ) as run:
        assert publish_pyxis_repository.main() == 0
    run.assert_called_once()


def test_main_failure_propagates_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Propagate workflow failures from main()."""
    monkeypatch.setenv("PARAM_DATA_DIR", "/tmp")
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
    monkeypatch.setenv("RESULT_SIGN_REGISTRY_ACCESS_PATH", "/tmp/r")
    monkeypatch.setenv("PYXIS_URL", "https://pyxis/v1")

    with mock.patch(
        "publish_pyxis_repository.run_publish_pyxis_repository",
        side_effect=FileNotFoundError("No valid snapshot file was provided."),
    ):
        with pytest.raises(FileNotFoundError, match="No valid snapshot file"):
            publish_pyxis_repository.main()
