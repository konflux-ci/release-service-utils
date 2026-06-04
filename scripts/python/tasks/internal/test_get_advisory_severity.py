"""Tests for `get_advisory_severity`."""

from __future__ import annotations

import base64
import gzip
import json
import runpy
import subprocess
import sys
import threading
from pathlib import Path
from unittest import mock

import pytest
import requests
import tekton

try:
    import requests_kerberos  # noqa: F401
except ImportError:
    kerberos_mod = mock.MagicMock()
    kerberos_mod.HTTPKerberosAuth = mock.MagicMock
    kerberos_mod.OPTIONAL = 1
    sys.modules["requests_kerberos"] = kerberos_mod

import get_advisory_severity


def _gzip_b64(obj: object) -> str:
    raw = json.dumps(obj).encode("utf-8")
    return base64.standard_b64encode(gzip.compress(raw)).decode("ascii")


def _write_osidb_mount(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "name").write_text("svc/osidb", encoding="utf-8")
    (d / "base64_keytab").write_text(
        base64.b64encode(b"x").decode("ascii"),
        encoding="utf-8",
    )
    (d / "osidb_url").write_text("https://osidb.example", encoding="utf-8")


def _result_paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "result": tmp_path / "result",
        "severity": tmp_path / "severity",
        "internal_pr_name": tmp_path / "internal_pr",
        "internal_task_run_name": tmp_path / "internal_task",
    }


def _no_kinit(*_args: object, **_kwargs: object) -> None:
    return None


def _catalog_flaw_critical() -> dict:
    return {
        "impact": "CRITICAL",
        "affects": [
            {
                "purl": "pkg:oci/kubernetes?repository_url=component&a=b",
                "impact": "",
            }
        ],
    }


def _catalog_flaw_moderate() -> dict:
    return {
        "impact": "MODERATE",
        "affects": [
            {
                "purl": "pkg:oci/kubernetes?repository_url=foo&a=b",
                "impact": "LOW",
            },
            {
                "purl": "pkg:oci/kubernetes?repository_url=component&a=b",
                "impact": "IMPORTANT",
            },
            {"purl": "", "impact": "LOW"},
        ],
    }


def _catalog_flaws() -> dict[str, dict]:
    return {
        "CVE-critical": _catalog_flaw_critical(),
        "CVE-moderate": _catalog_flaw_moderate(),
    }


def _run_with_mocked_fetch(
    tmp_path: Path,
    images: str,
    flaw_cache: dict[str, dict],
) -> dict[str, Path]:
    paths = _result_paths(tmp_path)
    mount = tmp_path / "mount"
    _write_osidb_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n", encoding="utf-8")
    with (
        mock.patch(
            "get_advisory_severity.authentication.kinit_with_retry",
            side_effect=_no_kinit,
        ),
        mock.patch(
            "get_advisory_severity.fetch_flaws_parallel",
            return_value=flaw_cache,
        ),
    ):
        get_advisory_severity.run_get_advisory_severity(
            images_encoded=images,
            mount=mount,
            result_paths=paths,
            pipeline_run_name="pr-1",
            task_run_name="tr-1",
            krb5_template=krb5,
        )
    return paths


def test_unique_fixed_cves_skips_invalid_entries() -> None:
    """Malformed image rows are skipped without raising."""
    images = [
        "not-a-dict",
        {"cves": "not-a-dict"},
        {"cves": {"fixed": "not-a-dict"}},
        {"cves": {"fixed": {"CVE-1": {}}}},
    ]
    assert get_advisory_severity.unique_fixed_cves(images) == ["CVE-1"]


def test_higher_severity_unknown_levels_returns_current() -> None:
    """Unknown impact strings fall back to the current value."""
    assert get_advisory_severity.higher_severity("UNKNOWN", "ALSO") == "UNKNOWN"


def test_purl_impact_entries_skips_invalid_rows() -> None:
    """Non-list affects and invalid affect rows are ignored."""
    assert get_advisory_severity.purl_impact_entries({}) == []
    flaw = {"affects": ["not-a-dict", {"purl": None}, {"purl": ""}]}
    assert get_advisory_severity.purl_impact_entries(flaw) == []


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ("", "empty OSIDB response"),
        ('{"results": []}', "no OSIDB flaw row"),
        ('{"results": [null]}', "invalid OSIDB flaw row"),
        ('{"results": ["not-a-dict"]}', "invalid OSIDB flaw row"),
    ],
)
def test_fetch_flaw_record_rejects_bad_response(body: str, match: str) -> None:
    """`fetch_flaw_record` validates the OSIDB response body."""
    with mock.patch("osidb.fetch_flaw_response", return_value=body):
        with pytest.raises(ValueError, match=match):
            get_advisory_severity.fetch_flaw_record("https://osidb", "tok", "CVE-x")


def test_fetch_flaws_parallel_empty_cve_list() -> None:
    """No CVE ids means no OSIDB calls and an empty cache."""
    assert (
        get_advisory_severity.fetch_flaws_parallel(
            "https://osidb",
            [],
            get_token=lambda _url: pytest.fail("get_token must not be called"),
        )
        == {}
    )


def test_fetch_flaws_parallel_populates_cache() -> None:
    """Parallel fetch stores one flaw record per CVE id."""
    seen: list[str] = []

    def _fetch(_url: str, _token: str, cve_id: str) -> dict:
        seen.append(cve_id)
        return {"impact": "MODERATE", "cve_id": cve_id}

    cache = get_advisory_severity.fetch_flaws_parallel(
        "https://osidb",
        ["CVE-a", "CVE-b"],
        get_token=lambda _url: "tok",
        fetch_flaw=_fetch,
        batch_size=1,
        max_workers=2,
    )
    assert set(cache) == {"CVE-a", "CVE-b"}
    assert set(seen) == {"CVE-a", "CVE-b"}


def test_process_cve_batch_skips_cached_cve() -> None:
    """A CVE already in the batch cache is not fetched again."""
    cache = {"CVE-a": {"impact": "LOW"}}
    cache_lock = threading.Lock()
    fetch_calls: list[str] = []

    def _fetch(_url: str, _token: str, cve_id: str) -> dict:
        fetch_calls.append(cve_id)
        return {"impact": "MODERATE"}

    get_advisory_severity._process_cve_batch(
        0,
        ["CVE-a", "CVE-b"],
        "https://osidb",
        cache,
        cache_lock,
        get_token=lambda _url: "tok",
        fetch_flaw=_fetch,
    )
    assert fetch_calls == ["CVE-b"]
    assert cache["CVE-b"]["impact"] == "MODERATE"


def test_highest_severity_skips_invalid_image_rows() -> None:
    """Non-dict images and malformed cves maps are ignored."""
    images = [
        "skip",
        {"repository": "foo", "cves": "skip"},
        {"repository": "foo", "cves": {"fixed": "skip"}},
    ]
    assert get_advisory_severity.highest_severity_for_images(images, {}) == ""


def test_highest_severity_raises_when_cve_missing_from_cache() -> None:
    """A fixed CVE absent from the flaw cache raises `ValueError`."""
    images = [{"repository": "foo", "cves": {"fixed": {"CVE-missing": {}}}}]
    with pytest.raises(ValueError, match="not found in cache"):
        get_advisory_severity.highest_severity_for_images(images, {})


def test_run_krb5_template_read_error(tmp_path: Path) -> None:
    """Missing krb5 template is wrapped as `CheckStepError`."""
    paths = _result_paths(tmp_path)
    mount = tmp_path / "mount"
    _write_osidb_mount(mount)
    missing_krb5 = tmp_path / "missing.conf"
    with pytest.raises(tekton.CheckStepError, match="Kerberos"):
        get_advisory_severity.run_get_advisory_severity(
            images_encoded=_gzip_b64([{"repository": "foo", "cves": {}}]),
            mount=mount,
            result_paths=paths,
            pipeline_run_name="pr-1",
            task_run_name="tr-1",
            krb5_template=missing_krb5,
        )


def test_module_main_guard_raises_system_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Executing the file as `__main__` exits via `SystemExit(main())`."""
    _setup_main_env(tmp_path, monkeypatch)
    monkeypatch.setattr("sys.argv", ["get_advisory_severity.py"])
    with mock.patch("get_advisory_severity.run_get_advisory_severity"):
        with pytest.raises(SystemExit) as exc:
            runpy.run_path(
                str(Path(get_advisory_severity.__file__)),
                run_name="__main__",
            )
    assert exc.value.code == 0


def test_decode_release_notes_images_roundtrip() -> None:
    """Base64+gzip encoded release-notes JSON decodes to the original list."""
    images = [{"repository": "foo", "cves": {"fixed": {"CVE-1": {"components": []}}}}]
    encoded = _gzip_b64(images)
    assert get_advisory_severity.decode_release_notes_images(encoded) == images


def test_decode_release_notes_images_rejects_non_array() -> None:
    """Non-array JSON after decode raises `ValueError`."""
    with pytest.raises(ValueError, match="JSON array"):
        get_advisory_severity.decode_release_notes_images(_gzip_b64({"x": 1}))


def test_unique_fixed_cves_stable_order() -> None:
    """CVE ids are deduplicated while preserving first-seen order."""
    images = [
        {"cves": {"fixed": {"CVE-b": {}, "CVE-a": {}}}},
        {"cves": {"fixed": {"CVE-a": {}, "CVE-c": {}}}},
    ]
    assert get_advisory_severity.unique_fixed_cves(images) == ["CVE-b", "CVE-a", "CVE-c"]


def test_higher_severity_picks_highest() -> None:
    """`higher_severity` returns the more severe OSIDB impact string."""
    assert get_advisory_severity.higher_severity("MODERATE", "CRITICAL") == "CRITICAL"
    assert get_advisory_severity.higher_severity("CRITICAL", "LOW") == "CRITICAL"
    assert get_advisory_severity.higher_severity("", "IMPORTANT") == "IMPORTANT"


def test_resolve_impact_uses_general_when_component_empty() -> None:
    """Empty component impact falls back to the flaw-level impact."""
    impact = get_advisory_severity.resolve_impact_for_repository(
        _catalog_flaw_critical(),
        "component",
    )
    assert impact == "CRITICAL"


def test_resolve_impact_uses_component_override() -> None:
    """Matching component purl impact overrides the flaw-level impact."""
    impact = get_advisory_severity.resolve_impact_for_repository(
        _catalog_flaw_moderate(),
        "component",
    )
    assert impact == "IMPORTANT"


def test_highest_severity_catalog_happy_path() -> None:
    """Critical flaw impact wins over moderate across catalog mock CVE data."""
    images = get_advisory_severity.decode_release_notes_images(
        _gzip_b64(
            [
                {
                    "repository": "foo",
                    "cves": {
                        "fixed": {
                            "CVE-critical": {"components": []},
                            "CVE-moderate": {"components": []},
                        }
                    },
                },
            ]
        )
    )
    cache = _catalog_flaws()
    assert get_advisory_severity.highest_severity_for_images(images, cache) == "CRITICAL"


def test_highest_severity_no_cves_returns_empty() -> None:
    """Images with no fixed CVEs yield an empty highest-severity string."""
    images = [{"repository": "component", "cves": {}}]
    assert get_advisory_severity.highest_severity_for_images(images, {}) == ""


def test_fetch_flaw_record_parses_first_result() -> None:
    """OSIDB flaw JSON returns the first `results` row."""
    body = json.dumps({"results": [{"impact": "CRITICAL", "cve_id": "CVE-1"}]})
    with mock.patch("osidb.fetch_flaw_response", return_value=body):
        row = get_advisory_severity.fetch_flaw_record("https://osidb", "tok", "CVE-1")
    assert row["impact"] == "CRITICAL"


def test_fetch_flaw_with_token_retry_refreshes_token() -> None:
    """A failed flaw fetch refreshes the bearer token and retries once."""
    fetch_calls: list[str] = []

    def _fetch(_url: str, token: str, _cve_id: str) -> dict:
        fetch_calls.append(token)
        if len(fetch_calls) == 1:
            raise ValueError("expired")
        return {"impact": "LOW"}

    record, token = get_advisory_severity.fetch_flaw_with_token_retry(
        "https://osidb",
        "old-token",
        "CVE-1",
        get_token=lambda _url: "fresh-token",
        fetch_flaw=_fetch,
    )
    assert record["impact"] == "LOW"
    assert token == "fresh-token"


def test_fetch_flaw_with_token_retry_refreshes_on_http_401() -> None:
    """HTTP 401 from OSIDB triggers a token refresh and one retry."""
    fetch_calls: list[str] = []

    def _fetch(_url: str, token: str, _cve_id: str) -> dict:
        fetch_calls.append(token)
        if len(fetch_calls) == 1:
            response = requests.Response()
            response.status_code = 401
            raise requests.HTTPError(response=response)
        return {"impact": "MODERATE"}

    record, token = get_advisory_severity.fetch_flaw_with_token_retry(
        "https://osidb",
        "old-token",
        "CVE-1",
        get_token=lambda _url: "fresh-token",
        fetch_flaw=_fetch,
    )
    assert record["impact"] == "MODERATE"
    assert token == "fresh-token"
    assert fetch_calls == ["old-token", "fresh-token"]


def test_fetch_flaw_with_token_retry_does_not_refresh_on_os_error() -> None:
    """Network failures are not retried with a refreshed token."""

    def _fetch(_url: str, _token: str, _cve_id: str) -> dict:
        raise OSError("connection refused")

    with pytest.raises(OSError, match="connection refused"):
        get_advisory_severity.fetch_flaw_with_token_retry(
            "https://osidb",
            "old-token",
            "CVE-1",
            get_token=lambda _url: pytest.fail("get_token must not be called"),
            fetch_flaw=_fetch,
        )


def test_fetch_flaw_with_token_retry_does_not_refresh_on_http_500() -> None:
    """Non-auth HTTP errors are not retried with a refreshed token."""
    response = requests.Response()
    response.status_code = 500

    def _fetch(_url: str, _token: str, _cve_id: str) -> dict:
        raise requests.HTTPError(response=response)

    with pytest.raises(requests.HTTPError):
        get_advisory_severity.fetch_flaw_with_token_retry(
            "https://osidb",
            "old-token",
            "CVE-1",
            get_token=lambda _url: pytest.fail("get_token must not be called"),
            fetch_flaw=_fetch,
        )


def test_run_success_critical(tmp_path: Path) -> None:
    """Catalog happy path writes `Success` and `Critical` severity."""
    images = _gzip_b64(
        [
            {
                "repository": "foo",
                "cves": {
                    "fixed": {
                        "CVE-critical": {"components": []},
                        "CVE-moderate": {"components": []},
                    }
                },
            }
        ]
    )
    paths = _run_with_mocked_fetch(tmp_path, images, _catalog_flaws())
    assert paths["result"].read_text(encoding="utf-8") == "Success"
    assert paths["severity"].read_text(encoding="utf-8") == "Critical"
    assert paths["internal_pr_name"].read_text(encoding="utf-8") == "pr-1"


def test_run_no_cves_raises(tmp_path: Path) -> None:
    """Release notes with no fixed CVEs raise `CheckStepError`."""
    images = _gzip_b64([{"repository": "component", "cves": {}}])
    paths = _result_paths(tmp_path)
    mount = tmp_path / "mount"
    _write_osidb_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n", encoding="utf-8")
    with (
        mock.patch(
            "get_advisory_severity.authentication.kinit_with_retry",
            side_effect=_no_kinit,
        ),
        mock.patch("get_advisory_severity.fetch_flaws_parallel", return_value={}),
    ):
        with pytest.raises(tekton.CheckStepError, match="Unable to find severity"):
            get_advisory_severity.run_get_advisory_severity(
                images_encoded=images,
                mount=mount,
                result_paths=paths,
                pipeline_run_name="pr-1",
                task_run_name="tr-1",
                krb5_template=krb5,
            )


def test_run_component_important(tmp_path: Path) -> None:
    """Component purl override yields `Important` for a moderate CVE."""
    images = _gzip_b64(
        [
            {
                "repository": "component",
                "cves": {"fixed": {"CVE-moderate": {"components": []}}},
            }
        ]
    )
    paths = _run_with_mocked_fetch(tmp_path, images, _catalog_flaws())
    assert paths["severity"].read_text(encoding="utf-8") == "Important"


def test_run_empty_component_impact_critical(tmp_path: Path) -> None:
    """Empty component impact uses flaw-level `Critical` (catalog regression)."""
    images = _gzip_b64(
        [
            {
                "repository": "component",
                "cves": {"fixed": {"CVE-critical": {"components": []}}},
            }
        ]
    )
    paths = _run_with_mocked_fetch(tmp_path, images, _catalog_flaws())
    assert paths["severity"].read_text(encoding="utf-8") == "Critical"


def test_run_mount_read_error(tmp_path: Path) -> None:
    """Missing OSIDB service-account mount is wrapped as `CheckStepError`."""
    paths = _result_paths(tmp_path)
    with pytest.raises(tekton.CheckStepError, match="OSIDB service account"):
        get_advisory_severity.run_get_advisory_severity(
            images_encoded=_gzip_b64([{"repository": "foo", "cves": {}}]),
            mount=tmp_path / "missing",
            result_paths=paths,
            pipeline_run_name="pr-1",
            task_run_name="tr-1",
        )


def test_run_wraps_kinit_error(tmp_path: Path) -> None:
    """`CalledProcessError` from kinit is wrapped as `CheckStepError`."""
    paths = _result_paths(tmp_path)
    mount = tmp_path / "mount"
    _write_osidb_mount(mount)
    krb5 = tmp_path / "k5.conf"
    krb5.write_text("[libdefaults]\n", encoding="utf-8")

    def _fail_kinit(*_args: object, **_kwargs: object) -> None:
        raise subprocess.CalledProcessError(1, "kinit")

    with mock.patch(
        "get_advisory_severity.authentication.kinit_with_retry",
        side_effect=_fail_kinit,
    ):
        with pytest.raises(tekton.CheckStepError, match="Kerberos"):
            get_advisory_severity.run_get_advisory_severity(
                images_encoded=_gzip_b64(
                    [{"repository": "foo", "cves": {"fixed": {"CVE-1": {}}}}]
                ),
                mount=mount,
                result_paths=paths,
                pipeline_run_name="pr-1",
                task_run_name="tr-1",
                krb5_template=krb5,
            )


def _setup_main_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    paths = {
        "RESULT_RESULT": tmp_path / "result",
        "RESULT_SEVERITY": tmp_path / "severity",
        "RESULT_INTERNAL_REQUEST_PIPELINE_RUN_NAME": tmp_path / "internal_pr",
        "RESULT_INTERNAL_REQUEST_TASK_RUN_NAME": tmp_path / "internal_task",
    }
    for key, path in paths.items():
        monkeypatch.setenv(key, str(path))
        path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("IMAGES_ENCODED", _gzip_b64([{"repository": "foo", "cves": {}}]))
    monkeypatch.setenv("OSIDB_SERVICE_ACCOUNT_MOUNT", str(tmp_path / "mount"))
    monkeypatch.setenv("PARAM_INTERNAL_REQUEST_PIPELINE_RUN_NAME", "pr-main")
    monkeypatch.setenv("PARAM_TASK_RUN_NAME", "tr-main")
    return paths


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`main()` always returns 0 after a normal run."""
    _setup_main_env(tmp_path, monkeypatch)
    with mock.patch("get_advisory_severity.run_get_advisory_severity"):
        assert get_advisory_severity.main() == 0


def test_main_writes_failure_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Workflow failures are written to `RESULT_RESULT`; process still exits 0."""
    paths = _setup_main_env(tmp_path, monkeypatch)
    with mock.patch(
        "get_advisory_severity.run_get_advisory_severity",
        side_effect=tekton.CheckStepError(
            "determining advisory severity from release notes",
            ValueError("Unable to find severity on any cve"),
        ),
    ):
        assert get_advisory_severity.main() == 0
    text = paths["RESULT_RESULT"].read_text(encoding="utf-8")
    assert "Unable to find severity" in text
