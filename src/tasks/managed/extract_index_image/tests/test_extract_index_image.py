"""Test extract_index_image task logic."""

from __future__ import annotations

import json
from pathlib import Path

import extract_index_image
import pytest


def _component(
    ocp_version: str,
    index_image: str,
    index_image_resolved: str,
) -> dict[str, str]:
    """Build one InternalRequest component row."""
    return {
        "ocp_version": ocp_version,
        "index_image": index_image,
        "index_image_resolved": index_image_resolved,
    }


def test_extract_index_image_results_single_component() -> None:
    """Map one component to its OCP version (catalog happy-path test)."""
    payload = {
        "components": [
            _component(
                "v4.12",
                "redhat.com/rh-stage/iib:01",
                "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            ),
        ],
    }

    result = extract_index_image.extract_index_image_results(payload)

    assert result == {
        "index_image": {
            "v4.12": {
                "index_image": "redhat.com/rh-stage/iib:01",
                "index_image_resolved": "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            },
        },
    }


def test_extract_index_image_results_uses_last_component_per_version() -> None:
    """Keep the last component for a shared OCP version (catalog multi test)."""
    payload = {
        "components": [
            _component(
                "v4.12",
                "redhat.com/rh-stage/iib:01",
                "redhat.com/rh-stage/iib@sha256:abcdefghijk",
            ),
            _component(
                "v4.12",
                "redhat.com/rh-stage/iib:02",
                "redhat.com/rh-stage/iib@sha256:1234567890",
            ),
        ],
    }

    result = extract_index_image.extract_index_image_results(payload)

    assert result["index_image"]["v4.12"] == {
        "index_image": "redhat.com/rh-stage/iib:02",
        "index_image_resolved": "redhat.com/rh-stage/iib@sha256:1234567890",
    }
    assert len(result["index_image"]) == 1


def test_extract_index_image_results_multiple_ocp_versions() -> None:
    """Emit one entry per distinct OCP version."""
    payload = {
        "components": [
            _component("v4.12", "img:12", "img@sha256:12"),
            _component("v4.13", "img:13", "img@sha256:13"),
        ],
    }

    result = extract_index_image.extract_index_image_results(payload)

    assert set(result["index_image"]) == {"v4.12", "v4.13"}


def test_extract_index_image_results_empty_components() -> None:
    """Return an empty index_image map when components is empty."""
    result = extract_index_image.extract_index_image_results({"components": []})

    assert result == {"index_image": {}}


def test_extract_index_image_results_missing_components_raises() -> None:
    """Reject input when components is absent."""
    with pytest.raises(ValueError, match="components must be a JSON array"):
        extract_index_image.extract_index_image_results({})


def test_extract_index_image_results_invalid_components_raises() -> None:
    """Reject input when components is not an array."""
    with pytest.raises(ValueError, match="components must be a JSON array"):
        extract_index_image.extract_index_image_results({"components": "bad"})


def test_extract_index_image_results_ignores_non_object_components() -> None:
    """Skip component rows that are not JSON objects."""
    payload = {
        "components": [
            "ignored",
            _component("v4.12", "img:1", "img@sha256:1"),
        ],
    }

    result = extract_index_image.extract_index_image_results(payload)

    assert result["index_image"]["v4.12"]["index_image"] == "img:1"


def test_extract_index_image_results_handles_missing_ocp_version() -> None:
    """Keep the last component when ocp_version is missing or null."""
    payload = {
        "components": [
            {
                "index_image": "img:first",
                "index_image_resolved": "img@sha256:first",
            },
            {
                "ocp_version": None,
                "index_image": "img:last",
                "index_image_resolved": "img@sha256:last",
            },
            _component("v4.12", "img:12", "img@sha256:12"),
        ],
    }

    result = extract_index_image.extract_index_image_results(payload)

    assert result["index_image"][""] == {
        "index_image": "img:last",
        "index_image_resolved": "img@sha256:last",
    }
    assert result["index_image"]["v4.12"]["index_image"] == "img:12"


def test_main_entrypoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Execute the module as ``__main__``."""
    import runpy

    data_dir = tmp_path / "release"
    input_rel = Path("results/internal-request-results.json")
    input_path = data_dir / input_rel
    input_path.parent.mkdir(parents=True)
    input_path.write_text(
        json.dumps({"components": [_component("v4.12", "img:1", "img@sha256:1")]}),
        encoding="utf-8",
    )

    monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
    monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
    monkeypatch.setenv("PARAM_INTERNAL_REQUEST_RESULTS_FILE", str(input_rel))

    with pytest.raises(SystemExit) as exc:
        runpy.run_module(
            "release_service_utils.tasks.managed" ".extract_index_image.extract_index_image",
            run_name="__main__",
        )

    assert exc.value.code == 0


def test_extract_index_image_writes_expected_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Write extract-index-image-results.json and mirror JSON to stdout."""
    data_dir = tmp_path / "release"
    results_dir = Path("run-uid/results")
    input_rel = Path("run-uid/results/internal-request-results.json")
    input_path = data_dir / input_rel
    input_path.parent.mkdir(parents=True)
    input_path.write_text(
        json.dumps(
            {
                "components": [
                    _component("v4.12", "redhat.com/rh-stage/iib:01", "img@sha256:abc"),
                ],
            },
        )
        + "\n",
        encoding="utf-8",
    )

    output_path = extract_index_image.extract_index_image(
        data_dir=data_dir,
        results_dir_path=results_dir,
        internal_request_results_file=input_rel,
    )

    assert output_path == data_dir / results_dir / "extract-index-image-results.json"
    text = output_path.read_text(encoding="utf-8")
    data = json.loads(text)
    assert data["index_image"]["v4.12"]["index_image"] == "redhat.com/rh-stage/iib:01"
    assert capsys.readouterr().out == text


def test_extract_index_image_missing_input_raises(tmp_path: Path) -> None:
    """Raise when the InternalRequest results file is missing (catalog fail test)."""
    with pytest.raises(FileNotFoundError, match="InternalRequest results file not found"):
        extract_index_image.extract_index_image(
            data_dir=tmp_path,
            results_dir_path=Path("results"),
            internal_request_results_file=Path("internal-request-results.json"),
        )


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run main() with env vars and write the results file."""
    data_dir = tmp_path / "release"
    input_rel = Path("results/internal-request-results.json")
    input_path = data_dir / input_rel
    input_path.parent.mkdir(parents=True)
    input_path.write_text(
        json.dumps({"components": [_component("v4.12", "img:1", "img@sha256:1")]}) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
    monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
    monkeypatch.setenv("PARAM_INTERNAL_REQUEST_RESULTS_FILE", str(input_rel))

    assert extract_index_image.main() == 0
    assert (data_dir / "results" / extract_index_image.RESULTS_FILENAME).is_file()


def test_main_missing_input_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate FileNotFoundError when the input file is absent."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_RESULTS_DIR_PATH", "results")
    monkeypatch.setenv("PARAM_INTERNAL_REQUEST_RESULTS_FILE", "missing.json")

    with pytest.raises(FileNotFoundError):
        extract_index_image.main()
