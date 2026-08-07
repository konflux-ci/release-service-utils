"""Tests for extract_oot_kmods."""

from __future__ import annotations

import json
import re
import subprocess
import tarfile
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import pytest

from extract_oot_kmods import (
    _extract_layer,
    _ko_prefix,
    _write_summary,
    extract_single_arch,
    get_image_architectures,
    main,
    resolve_arch_name,
    run,
)

# -- Helpers ----------------------------------------------------------


DEFAULT_IMAGE = "quay.io/img@sha256:abc"


def _mock_first_component(image: str = DEFAULT_IMAGE) -> dict[str, str]:
    """Return a dict matching snapshot.first_component() output."""
    return {"container_image": image, "revision": "", "origin_repo": ""}


def _make_layer_tar(
    tar_path: Path,
    ko_files: dict[str, str] | None = None,
    envfile_content: str | None = None,
    envfile_rel: str = "envfile",
) -> None:
    """Create a tar archive containing ``.ko`` stubs and an optional envfile."""
    with tarfile.open(tar_path, "w") as tf:
        if ko_files:
            for name, content in ko_files.items():
                data = content.encode()
                info = tarfile.TarInfo(name=name)
                info.size = len(data)
                from io import BytesIO

                tf.addfile(info, BytesIO(data))
        if envfile_content is not None:
            data = envfile_content.encode()
            info = tarfile.TarInfo(name=envfile_rel)
            info.size = len(data)
            from io import BytesIO

            tf.addfile(info, BytesIO(data))


def _make_image_dir(
    tmp_path: Path,
    ko_files: dict[str, str] | None = None,
    envfile_content: str | None = None,
    envfile_rel: str = "envfile",
    extra_layers: list[dict[str, str]] | None = None,
) -> Path:
    """Build a fake skopeo dir: transport directory with manifest + layers."""
    image_dir = tmp_path / "image"
    image_dir.mkdir()

    layers = []

    if ko_files is not None or envfile_content is not None:
        layer_file = image_dir / "layer1hash"
        _make_layer_tar(
            layer_file,
            ko_files=ko_files,
            envfile_content=envfile_content,
            envfile_rel=envfile_rel,
        )
        layers.append({"digest": "sha256:layer1hash"})

    if extra_layers:
        for i, layer_ko in enumerate(extra_layers, start=2):
            layer_file = image_dir / f"layer{i}hash"
            _make_layer_tar(layer_file, ko_files=layer_ko)
            layers.append({"digest": f"sha256:layer{i}hash"})

    manifest = {"layers": layers}
    (image_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return image_dir


# -- get_image_architectures -----------------------------------------


def test_get_image_architectures_single() -> None:
    """Single-arch NDJSON output is parsed correctly."""
    ndjson = '{"platform":{"architecture":"amd64"},"digest":"sha256:aaa"}\n'
    with patch("extract_oot_kmods.run_cmd_text", return_value=ndjson):
        result = get_image_architectures("quay.io/img@sha256:abc")
    assert len(result) == 1
    assert result[0]["platform"]["architecture"] == "amd64"


def test_get_image_architectures_multi() -> None:
    """Multi-arch NDJSON output produces multiple entries."""
    ndjson = (
        '{"platform":{"architecture":"amd64"},"digest":"sha256:aaa"}\n'
        '{"platform":{"architecture":"arm64"},"digest":"sha256:bbb"}\n'
    )
    with patch("extract_oot_kmods.run_cmd_text", return_value=ndjson):
        result = get_image_architectures("quay.io/img@sha256:abc")
    assert len(result) == 2


def test_get_image_architectures_empty() -> None:
    """Empty output raises ValueError."""
    with patch("extract_oot_kmods.run_cmd_text", return_value=""):
        with pytest.raises(ValueError, match="returned no data"):
            get_image_architectures("quay.io/img@sha256:abc")


def test_get_image_architectures_blank_lines() -> None:
    """Blank lines in output are skipped."""
    ndjson = '\n{"platform":{"architecture":"s390x"},"digest":"sha256:ccc"}\n\n'
    with patch("extract_oot_kmods.run_cmd_text", return_value=ndjson):
        result = get_image_architectures("quay.io/img@sha256:abc")
    assert len(result) == 1
    assert result[0]["platform"]["architecture"] == "s390x"


# -- _ko_prefix -------------------------------------------------------


def test_ko_prefix_strips_leading_slash() -> None:
    """Leading slash is stripped from the path."""
    assert _ko_prefix("/kmods") == "kmods"


def test_ko_prefix_no_leading_slash() -> None:
    """Path without leading slash is returned unchanged."""
    assert _ko_prefix("kmods") == "kmods"


def test_ko_prefix_trailing_slash() -> None:
    """Trailing slash is stripped to avoid double-slash in regex."""
    assert _ko_prefix("/kmods/") == "kmods"


def test_ko_prefix_nested() -> None:
    """Nested path has only the leading slash stripped."""
    assert _ko_prefix("/opt/drivers/kmods") == "opt/drivers/kmods"


# -- _extract_layer ----------------------------------------------------

_KO_PATTERN = re.compile(r"^kmods/.*\.ko$")


def test_extract_layer_ko_files(tmp_path: Path) -> None:
    """`.ko` files are extracted preserving directory structure."""
    layer_file = tmp_path / "layer.tar"
    _make_layer_tar(
        layer_file,
        ko_files={
            "kmods/mod1.ko": "data1",
            "kmods/sub/mod2.ko": "data2",
        },
    )
    dest = tmp_path / "output"
    result = _extract_layer(
        layer_file,
        "kmods",
        _KO_PATTERN,
        "envfile",
        dest,
    )
    assert result is True
    assert (dest / "mod1.ko").exists()
    assert (dest / "sub" / "mod2.ko").exists()


def test_extract_layer_no_matching_ko(tmp_path: Path) -> None:
    """Returns False when layer has no .ko files matching the pattern."""
    layer_file = tmp_path / "layer.tar"
    _make_layer_tar(layer_file, ko_files={"other/file.txt": "data"})
    dest = tmp_path / "output"
    result = _extract_layer(
        layer_file,
        "kmods",
        _KO_PATTERN,
        "envfile",
        dest,
    )
    assert result is False


def test_extract_layer_no_ko_in_prefix(tmp_path: Path) -> None:
    """Returns False when kmods dir has no .ko files after extraction."""
    layer_file = tmp_path / "layer.tar"
    _make_layer_tar(layer_file, ko_files={"kmods/readme.txt": "docs"})
    dest = tmp_path / "output"
    result = _extract_layer(
        layer_file,
        "kmods",
        _KO_PATTERN,
        "envfile",
        dest,
    )
    assert result is False


def test_extract_layer_no_dir(tmp_path: Path) -> None:
    """Returns False when kmods dir does not exist after extraction."""
    layer_file = tmp_path / "layer.tar"
    _make_layer_tar(layer_file, ko_files={"other/mod.ko": "data"})
    dest = tmp_path / "output"
    result = _extract_layer(
        layer_file,
        "kmods",
        _KO_PATTERN,
        "envfile",
        dest,
    )
    assert result is False


def test_extract_layer_with_envfile(tmp_path: Path) -> None:
    """Envfile is extracted alongside .ko files when present."""
    layer_file = tmp_path / "layer.tar"
    _make_layer_tar(
        layer_file,
        ko_files={"kmods/m.ko": "d"},
        envfile_content="ARCH=x86_64\nVER=1.0\n",
        envfile_rel="envfile",
    )
    dest = tmp_path / "output"
    result = _extract_layer(
        layer_file,
        "kmods",
        _KO_PATTERN,
        "envfile",
        dest,
    )
    assert result is True
    assert (dest / "envfile").exists()
    assert "ARCH=x86_64" in (dest / "envfile").read_text()


def test_extract_layer_without_envfile(tmp_path: Path) -> None:
    """No envfile written when absent from layer."""
    layer_file = tmp_path / "layer.tar"
    _make_layer_tar(layer_file, ko_files={"kmods/m.ko": "d"})
    dest = tmp_path / "output"
    _extract_layer(
        layer_file,
        "kmods",
        _KO_PATTERN,
        "envfile",
        dest,
    )
    assert not (dest / "envfile").exists()


def test_extract_layer_nested_envfile(tmp_path: Path) -> None:
    """Envfile with a nested relative path is extracted correctly."""
    layer_file = tmp_path / "layer.tar"
    ko_pattern = re.compile(r"^opt/drivers/kmods/.*\.ko$")
    _make_layer_tar(
        layer_file,
        ko_files={"opt/drivers/kmods/m.ko": "d"},
        envfile_content="ARCH=aarch64\n",
        envfile_rel="opt/drivers/envfile",
    )
    dest = tmp_path / "output"
    result = _extract_layer(
        layer_file,
        "opt/drivers/kmods",
        ko_pattern,
        "opt/drivers/envfile",
        dest,
    )
    assert result is True
    assert (dest / "envfile").exists()


# -- resolve_arch_name -------------------------------------------------


def test_resolve_arch_name_from_envfile(tmp_path: Path) -> None:
    """ARCH from envfile overrides platform_arch."""
    (tmp_path / "envfile").write_text("ARCH=x86_64\n")
    assert resolve_arch_name(tmp_path, "amd64") == "x86_64"


def test_resolve_arch_name_multi_platform(tmp_path: Path) -> None:
    """ARCH=MULTI_PLATFORM falls back to platform_arch."""
    (tmp_path / "envfile").write_text("ARCH=MULTI_PLATFORM\n")
    assert resolve_arch_name(tmp_path, "amd64") == "amd64"


def test_resolve_arch_name_no_envfile(tmp_path: Path) -> None:
    """Missing envfile falls back to platform_arch."""
    assert resolve_arch_name(tmp_path, "arm64") == "arm64"


def test_resolve_arch_name_empty_arch(tmp_path: Path) -> None:
    """Empty ARCH= falls back to platform_arch."""
    (tmp_path / "envfile").write_text("ARCH=\nOTHER=val\n")
    assert resolve_arch_name(tmp_path, "ppc64le") == "ppc64le"


def test_resolve_arch_name_no_arch_line(tmp_path: Path) -> None:
    """Envfile without ARCH line falls back to platform_arch."""
    (tmp_path / "envfile").write_text("VERSION=1.0\nVENDOR=test\n")
    assert resolve_arch_name(tmp_path, "s390x") == "s390x"


def test_resolve_arch_name_double_quoted(tmp_path: Path) -> None:
    """Double-quoted ARCH value has quotes stripped."""
    (tmp_path / "envfile").write_text('ARCH="x86_64"\n')
    assert resolve_arch_name(tmp_path, "amd64") == "x86_64"


def test_resolve_arch_name_single_quoted(tmp_path: Path) -> None:
    """Single-quoted ARCH value has quotes stripped."""
    (tmp_path / "envfile").write_text("ARCH='aarch64'\n")
    assert resolve_arch_name(tmp_path, "arm64") == "aarch64"


# -- extract_single_arch -----------------------------------------------


def _mock_skopeo_copy_with_dir(
    image_dir: Path,
) -> Callable[..., subprocess.CompletedProcess[str]]:
    """Return a side_effect for skopeo.copy that populates the dest dir."""

    def side_effect(
        source: str, dest: str, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        import shutil

        dest_path = Path(dest.removeprefix("dir:"))
        for item in image_dir.iterdir():
            if item.is_file():
                shutil.copy2(item, dest_path / item.name)
        return subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")

    return side_effect


def test_extract_single_arch_success(tmp_path: Path) -> None:
    """End-to-end single arch extraction with .ko files and envfile."""
    image_dir = _make_image_dir(
        tmp_path,
        ko_files={
            "kmods/mod1.ko": "data1",
            "kmods/sub/mod2.ko": "data2",
        },
        envfile_content="ARCH=x86_64\n",
    )

    dest = tmp_path / "output"

    with patch(
        "extract_oot_kmods.skopeo.copy",
        side_effect=_mock_skopeo_copy_with_dir(image_dir),
    ):
        result = extract_single_arch("quay.io/img@sha256:abc", dest, "/kmods", "amd64")

    assert result is True
    assert (dest / "mod1.ko").exists()
    assert (dest / "sub" / "mod2.ko").exists()


def test_extract_single_arch_no_ko_files(tmp_path: Path) -> None:
    """Returns False when no .ko files are present in any layer."""
    image_dir = _make_image_dir(
        tmp_path,
        ko_files={"other/readme.txt": "docs"},
    )
    dest = tmp_path / "output"

    with patch(
        "extract_oot_kmods.skopeo.copy",
        side_effect=_mock_skopeo_copy_with_dir(image_dir),
    ):
        result = extract_single_arch("quay.io/img@sha256:abc", dest, "/kmods", "amd64")

    assert result is False


def test_extract_single_arch_multiple_layers(tmp_path: Path) -> None:
    """Only the first layer with .ko files is processed."""
    image_dir = _make_image_dir(
        tmp_path,
        ko_files={"kmods/first.ko": "first"},
        extra_layers=[{"kmods/second.ko": "second"}],
    )
    dest = tmp_path / "output"

    with patch(
        "extract_oot_kmods.skopeo.copy",
        side_effect=_mock_skopeo_copy_with_dir(image_dir),
    ):
        result = extract_single_arch("quay.io/img@sha256:abc", dest, "/kmods", "amd64")

    assert result is True
    assert (dest / "first.ko").exists()
    assert not (dest / "second.ko").exists()


def test_extract_single_arch_cleanup_on_failure(tmp_path: Path) -> None:
    """Temp directory is cleaned up even when skopeo.copy raises."""
    with patch(
        "extract_oot_kmods.skopeo.copy",
        side_effect=subprocess.CalledProcessError(1, "skopeo"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            extract_single_arch(
                "quay.io/img@sha256:abc",
                tmp_path / "out",
                "/kmods",
                "amd64",
            )


# -- _write_summary ----------------------------------------------------


def test_write_summary(tmp_path: Path) -> None:
    """Summary file is written with correct content."""
    (tmp_path / "x86_64").mkdir()
    (tmp_path / "x86_64" / "mod.ko").write_text("d")
    (tmp_path / "aarch64").mkdir()
    (tmp_path / "aarch64" / "a.ko").write_text("d")
    (tmp_path / "aarch64" / "b.ko").write_text("d")

    _write_summary(tmp_path, 2)

    summary = (tmp_path / "extraction_summary.txt").read_text()
    assert "Total architectures processed: 2" in summary
    assert "x86_64: 1 .ko files" in summary
    assert "aarch64: 2 .ko files" in summary


def test_write_summary_no_ko(tmp_path: Path) -> None:
    """Summary handles arch dirs with no .ko files."""
    (tmp_path / "arm64").mkdir()

    _write_summary(tmp_path, 1)

    summary = (tmp_path / "extraction_summary.txt").read_text()
    assert "arm64: 0 .ko files" in summary


def test_write_summary_empty_output_base(tmp_path: Path) -> None:
    """Summary handles an empty output_base with no arch subdirectories."""
    output_base = tmp_path / "signed-kmods"
    output_base.mkdir()

    _write_summary(output_base, 2)

    summary = (output_base / "extraction_summary.txt").read_text()
    assert "Total architectures processed: 2" in summary


# -- run ----------------------------------------------------------------


def test_run_single_arch(tmp_path: Path) -> None:
    """Single-arch path extracts and renames correctly."""
    arch_data = [{"platform": {"architecture": "amd64"}, "digest": "sha256:aaa"}]

    extracted_dir = tmp_path / "signed-kmods" / "amd64"
    extracted_dir.mkdir(parents=True)
    (extracted_dir / "mod.ko").write_text("d")
    (extracted_dir / "envfile").write_text("ARCH=x86_64\n")

    with (
        patch(
            "extract_oot_kmods.snapshot_helper.first_component",
            return_value=_mock_first_component(),
        ),
        patch(
            "extract_oot_kmods.get_image_architectures",
            return_value=arch_data,
        ),
        patch("extract_oot_kmods.extract_single_arch", return_value=True),
    ):
        run(tmp_path, "snapshot.json", "/kmods", "signed-kmods")

    assert (tmp_path / "signed-kmods" / "x86_64").is_dir()
    assert not (tmp_path / "signed-kmods" / "amd64").exists()


def test_run_single_arch_no_rename(tmp_path: Path) -> None:
    """Single-arch path without envfile does not rename."""
    arch_data = [{"platform": {"architecture": "amd64"}, "digest": "sha256:aaa"}]

    extracted_dir = tmp_path / "signed-kmods" / "amd64"
    extracted_dir.mkdir(parents=True)
    (extracted_dir / "mod.ko").write_text("d")

    with (
        patch(
            "extract_oot_kmods.snapshot_helper.first_component",
            return_value=_mock_first_component(),
        ),
        patch(
            "extract_oot_kmods.get_image_architectures",
            return_value=arch_data,
        ),
        patch("extract_oot_kmods.extract_single_arch", return_value=True),
    ):
        run(tmp_path, "snapshot.json", "/kmods", "signed-kmods")

    assert (tmp_path / "signed-kmods" / "amd64").is_dir()


def test_run_multi_arch(tmp_path: Path) -> None:
    """Multi-arch path processes each arch and writes summary."""
    arch_data = [
        {"platform": {"architecture": "amd64"}, "digest": "sha256:aaa"},
        {"platform": {"architecture": "arm64"}, "digest": "sha256:bbb"},
    ]

    for arch in ["amd64", "arm64"]:
        d = tmp_path / "signed-kmods" / arch
        d.mkdir(parents=True)
        (d / "mod.ko").write_text("d")

    with (
        patch(
            "extract_oot_kmods.snapshot_helper.first_component",
            return_value=_mock_first_component("quay.io/img@sha256:multi"),
        ),
        patch(
            "extract_oot_kmods.get_image_architectures",
            return_value=arch_data,
        ),
        patch("extract_oot_kmods.extract_single_arch", return_value=True),
        patch("extract_oot_kmods._write_summary") as mock_summary,
    ):
        run(tmp_path, "snapshot.json", "/kmods", "signed-kmods")

    mock_summary.assert_called_once()
    call_args = mock_summary.call_args
    assert call_args[0][1] == 2


def test_run_multi_arch_with_rename(tmp_path: Path) -> None:
    """Multi-arch renames directories per envfile ARCH."""
    arch_data = [
        {"platform": {"architecture": "amd64"}, "digest": "sha256:aaa"},
        {"platform": {"architecture": "arm64"}, "digest": "sha256:bbb"},
    ]

    for arch, env_arch in [("amd64", "x86_64"), ("arm64", "aarch64-64k")]:
        d = tmp_path / "signed-kmods" / arch
        d.mkdir(parents=True)
        (d / "mod.ko").write_text("d")
        (d / "envfile").write_text(f"ARCH={env_arch}\n")

    with (
        patch(
            "extract_oot_kmods.snapshot_helper.first_component",
            return_value=_mock_first_component("quay.io/img@sha256:multi"),
        ),
        patch(
            "extract_oot_kmods.get_image_architectures",
            return_value=arch_data,
        ),
        patch("extract_oot_kmods.extract_single_arch", return_value=True),
        patch("extract_oot_kmods._write_summary"),
    ):
        run(tmp_path, "snapshot.json", "/kmods", "signed-kmods")

    assert (tmp_path / "signed-kmods" / "x86_64").is_dir()
    assert (tmp_path / "signed-kmods" / "aarch64-64k").is_dir()
    assert not (tmp_path / "signed-kmods" / "amd64").exists()
    assert not (tmp_path / "signed-kmods" / "arm64").exists()


def test_run_multi_arch_image_url_split(tmp_path: Path) -> None:
    """Multi-arch builds arch-specific image from source_base@digest."""
    arch_data = [
        {"platform": {"architecture": "amd64"}, "digest": "sha256:aaa"},
        {"platform": {"architecture": "arm64"}, "digest": "sha256:bbb"},
    ]

    for arch in ["amd64", "arm64"]:
        d = tmp_path / "signed-kmods" / arch
        d.mkdir(parents=True)
        (d / "mod.ko").write_text("d")

    images_called: list[str] = []

    def capture_extract(image: str, dest: Path, kmods: str, arch: str) -> bool:
        images_called.append(image)
        return True

    with (
        patch(
            "extract_oot_kmods.snapshot_helper.first_component",
            return_value=_mock_first_component("quay.io/img@sha256:multi"),
        ),
        patch(
            "extract_oot_kmods.get_image_architectures",
            return_value=arch_data,
        ),
        patch(
            "extract_oot_kmods.extract_single_arch",
            side_effect=capture_extract,
        ),
        patch("extract_oot_kmods._write_summary"),
    ):
        run(tmp_path, "snapshot.json", "/kmods", "signed-kmods")

    assert images_called == [
        "quay.io/img@sha256:aaa",
        "quay.io/img@sha256:bbb",
    ]


# -- main ---------------------------------------------------------------


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads env and calls run()."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot.json")
    monkeypatch.setenv("PARAM_KMODS_PATH", "/kmods")
    monkeypatch.setenv("PARAM_SIGNED_KMODS_PATH", "signed-kmods")

    with patch("extract_oot_kmods.run") as mock_run:
        assert main() == 0

    mock_run.assert_called_once_with(
        data_dir=tmp_path,
        snapshot_path="snapshot.json",
        kmods_path="/kmods",
        signed_kmods_path="signed-kmods",
    )


def test_main_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when a required env var is missing."""
    monkeypatch.delenv("PARAM_DATA_DIR", raising=False)
    monkeypatch.delenv("PARAM_SNAPSHOT_PATH", raising=False)
    monkeypatch.delenv("PARAM_KMODS_PATH", raising=False)
    monkeypatch.delenv("PARAM_SIGNED_KMODS_PATH", raising=False)

    with pytest.raises(SystemExit):
        main()
