"""Unit tests for rh_direct_sign_image."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from rh_direct_sign_image import (
    PyxisSignature,
    SigningItem,
    batch_signing_items,
    collect_signing_items,
    filter_already_signed,
    find_existing_signatures,
    find_signatures_for_repository,
    get_all_image_digests,
    get_signing_keys,
    get_source_container_digest,
    get_submit_config,
    main,
    process_component,
    setup_argparser,
    submit_batch,
    submit_batches,
    validate_file,
    write_batches,
)

COMPONENT = {
    "name": "comp0",
    "containerImage": "registry.io/image@sha256:abc",
    "repositories": [
        {
            "rh-registry-repo": "registry.redhat.io/myproduct/myrepo",
            "registry-access-repo": "registry.access.redhat.com/myproduct/myrepo",
            "tags": ["v1.0", "latest"],
        }
    ],
}

DATA_FILE: dict = {"mapping": {"defaults": {"pushSourceContainer": False}}}
PYXIS_URL = "https://graphql-pyxis.example.com/graphql/"


# --- setup_argparser --output ---


def test_setup_argparser_output_is_optional_path(tmp_path) -> None:
    """--output defaults to None and --batch-max-size defaults to 14 KiB."""
    snap = tmp_path / "snap.json"
    snap.write_text("{}")
    data = tmp_path / "data.json"
    data.write_text("{}")
    sign = tmp_path / "sign.txt"
    sign.write_text("")

    base_args = [
        "--pyxis-server",
        "stage",
        "--snapshot",
        str(snap),
        "--data-file",
        str(data),
        "--sign-registry-access-file",
        str(sign),
        "--requester",
        "tester",
        "--pipeline-image",
        "quay.io/signing/pipeline:latest",
    ]

    parser = setup_argparser()
    args_without = parser.parse_args(base_args)
    args_with = parser.parse_args(base_args + ["--output", "/tmp/out"])
    args_custom_batch = parser.parse_args(base_args + ["--batch-max-size", "8192"])

    assert args_without.output is None
    assert args_without.batch_max_size == 14 * 1024
    assert args_with.output == Path("/tmp/out")
    assert args_custom_batch.batch_max_size == 8192


def test_setup_argparser_rejects_missing_file(tmp_path) -> None:
    """parse_args raises FileNotFoundError when a required file argument does not exist."""
    snap = tmp_path / "snap.json"
    snap.write_text("{}")
    data = tmp_path / "data.json"
    data.write_text("{}")

    parser = setup_argparser()
    with pytest.raises(FileNotFoundError):
        parser.parse_args(
            [
                "--pyxis-server",
                "stage",
                "--snapshot",
                str(snap),
                "--data-file",
                str(data),
                "--sign-registry-access-file",
                str(tmp_path / "missing.txt"),
                "--requester",
                "tester",
                "--pipeline-image",
                "quay.io/signing/pipeline:latest",
            ]
        )


# --- write_batches ---


def test_write_batches_creates_one_file_per_batch(tmp_path) -> None:
    """Each batch string is written to a separate numbered file."""
    write_batches(["aGVsbG8=", "d29ybGQ="], tmp_path)
    assert (tmp_path / "batch_0000.txt").read_text() == "aGVsbG8="
    assert (tmp_path / "batch_0001.txt").read_text() == "d29ybGQ="


def test_write_batches_creates_output_directory_if_missing(tmp_path) -> None:
    """Output directory is created when it does not exist."""
    output_dir = tmp_path / "new" / "subdir"
    write_batches(["aGVsbG8="], output_dir)
    assert output_dir.is_dir()


def test_write_batches_empty_list_creates_no_files(tmp_path) -> None:
    """No files are written when the batch list is empty."""
    write_batches([], tmp_path)
    assert list(tmp_path.iterdir()) == []


# --- batch_signing_items ---

ITEM = SigningItem("registry.redhat.io/repo:tag", "sha256:abc", "repo", "key-a")


def test_batch_signing_items_empty_list_returns_empty() -> None:
    """Empty input produces no batches."""
    assert batch_signing_items([]) == []


def test_batch_signing_items_single_item_produces_one_batch() -> None:
    """A single item is placed in exactly one batch."""
    assert len(batch_signing_items([ITEM])) == 1


def test_batch_signing_items_batch_is_valid_base64_encoded_json() -> None:
    """Each batch is a base64-encoded JSON array of signing items."""
    (batch,) = batch_signing_items([ITEM])
    decoded = json.loads(base64.b64decode(batch))
    assert decoded == [
        {"reference": "registry.redhat.io/repo:tag", "digest": "sha256:abc", "key": "key-a"}
    ]


def test_batch_signing_items_omits_repository_field() -> None:
    """The repository field is not included in the serialised batch."""
    (batch,) = batch_signing_items([ITEM])
    decoded = json.loads(base64.b64decode(batch))
    assert "repository" not in decoded[0]


def test_batch_signing_items_all_items_fit_in_one_batch() -> None:
    """Items that fit within the limit are packed into a single batch."""
    items = [
        SigningItem(f"registry.redhat.io/repo:tag{i}", "sha256:abc", "repo", "key-a")
        for i in range(10)
    ]
    batches = batch_signing_items(items)
    assert len(batches) == 1
    assert len(json.loads(base64.b64decode(batches[0]))) == 10


def test_batch_signing_items_splits_when_limit_reached() -> None:
    """A new batch is started when adding an item would exceed the byte limit."""
    # Measure the encoded size of a single-item batch and set limit just above it
    single_size = len(
        base64.b64encode(
            json.dumps(
                [{"reference": ITEM.reference, "digest": ITEM.digest, "key": ITEM.key}]
            ).encode()
        )
    )
    batches = batch_signing_items([ITEM, ITEM, ITEM], max_batch_bytes=single_size + 1)
    assert len(batches) == 3


def test_batch_signing_items_packs_as_many_as_fit() -> None:
    """Items are greedily packed until the limit is reached before splitting."""
    # Measure two-item batch size, set limit just above it → 4 items → 2 batches of 2
    two_item_json = json.dumps(
        [
            {"reference": ITEM.reference, "digest": ITEM.digest, "key": ITEM.key},
            {"reference": ITEM.reference, "digest": ITEM.digest, "key": ITEM.key},
        ]
    )
    two_size = len(base64.b64encode(two_item_json.encode()))
    batches = batch_signing_items([ITEM] * 4, max_batch_bytes=two_size + 1)
    assert len(batches) == 2
    for batch in batches:
        assert len(json.loads(base64.b64decode(batch))) == 2


# --- get_signing_keys ---


def test_get_signing_keys_returns_single_key() -> None:
    """SIG_KEY_NAME is returned as a single-element list."""
    cm = {"data": {"SIG_KEY_NAME": "redhate2etesting"}}
    assert get_signing_keys(cm) == ["redhate2etesting"]


def test_get_signing_keys_prefers_sig_key_names_over_sig_key_name() -> None:
    """SIG_KEY_NAMES takes precedence over SIG_KEY_NAME when both are present."""
    cm = {"data": {"SIG_KEY_NAME": "old-key", "SIG_KEY_NAMES": "key-a,key-b"}}
    assert get_signing_keys(cm) == ["key-a", "key-b"]


def test_get_signing_keys_splits_on_space() -> None:
    """SIG_KEY_NAMES splits on spaces as well as commas."""
    cm = {"data": {"SIG_KEY_NAMES": "key-a key-b key-c"}}
    assert get_signing_keys(cm) == ["key-a", "key-b", "key-c"]


def test_get_signing_keys_strips_whitespace() -> None:
    """Leading and trailing whitespace is stripped from each key name."""
    cm = {"data": {"SIG_KEY_NAMES": " key-a , key-b , key-c "}}
    assert get_signing_keys(cm) == ["key-a", "key-b", "key-c"]


def test_get_signing_keys_raises_when_no_key_defined() -> None:
    """KeyError is raised when neither SIG_KEY_NAME nor SIG_KEY_NAMES is present."""
    cm = {"data": {}}
    with pytest.raises(KeyError):
        get_signing_keys(cm)


# --- find_signatures_for_repository ---


@patch("rh_direct_sign_image.pyxis.graphql_query")
def test_find_signatures_for_repository_returns_signature_objects(mock_graphql) -> None:
    """Pyxis response is mapped to a set of PyxisSignature objects."""
    mock_graphql.return_value = {
        "find_signatures": {
            "data": [{"reference": "registry.redhat.io/repo:tag", "sig_key_id": "some-key"}],
            "error": None,
        }
    }

    result = find_signatures_for_repository("https://pyxis.example.com/", "repo", "sha256:abc")

    assert len(result) == 1
    sig = next(iter(result))
    assert sig.reference == "registry.redhat.io/repo:tag"
    assert sig.sig_key_id == "some-key"


@patch("rh_direct_sign_image.pyxis.graphql_query")
def test_find_signatures_for_repository_empty_returns_empty_set(mock_graphql) -> None:
    """An empty Pyxis response returns an empty set."""
    mock_graphql.return_value = {"find_signatures": {"data": [], "error": None}}

    result = find_signatures_for_repository("https://pyxis.example.com/", "repo", "sha256:abc")

    assert result == set()


@patch("rh_direct_sign_image.pyxis.graphql_query")
def test_find_signatures_for_repository_paginates(mock_graphql) -> None:
    """Multiple pages are fetched and their results are combined."""
    page_size = 2
    page1 = [
        {"reference": "registry.redhat.io/repo:tag1", "sig_key_id": "key"},
        {"reference": "registry.redhat.io/repo:tag2", "sig_key_id": "key"},
    ]
    page2 = [{"reference": "registry.redhat.io/repo:tag3", "sig_key_id": "key"}]
    mock_graphql.side_effect = [
        {"find_signatures": {"data": page1, "error": None}},
        {"find_signatures": {"data": page2, "error": None}},
    ]

    result = find_signatures_for_repository(
        "https://pyxis.example.com/", "repo", "sha256:abc", page_size=page_size
    )

    assert len(result) == 3


# --- collect_signing_items ---


def test_collect_signing_items_single_arch_rh_registry_only() -> None:
    """One item per tag is produced for the rh-registry-repo when no registry-access set."""
    items = collect_signing_items(COMPONENT, set(), ["sha256:abc"], None, ["key-a"])

    assert len(items) == 2  # 2 tags x 1 digest x 1 key
    assert (
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:v1.0",
            "sha256:abc",
            "myproduct/myrepo",
            "key-a",
        )
        in items
    )
    assert (
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:latest",
            "sha256:abc",
            "myproduct/myrepo",
            "key-a",
        )
        in items
    )


def test_collect_signing_items_includes_registry_access_when_repo_in_set() -> None:
    """registry-access-repo references are added when the repo is in the allowed set."""
    items = collect_signing_items(
        COMPONENT, {"myproduct/myrepo"}, ["sha256:abc"], None, ["key-a"]
    )

    assert len(items) == 4  # 2 tags x 2 registries x 1 key
    references = {i.reference for i in items}
    assert "registry.redhat.io/myproduct/myrepo:v1.0" in references
    assert "registry.access.redhat.com/myproduct/myrepo:v1.0" in references
    assert "registry.redhat.io/myproduct/myrepo:latest" in references
    assert "registry.access.redhat.com/myproduct/myrepo:latest" in references


def test_collect_signing_items_excludes_registry_access_when_repo_not_in_set() -> None:
    """registry-access-repo references are omitted when the repo is not in the allowed set."""
    items = collect_signing_items(COMPONENT, {"other/repo"}, ["sha256:abc"], None, ["key-a"])

    references = {i.reference for i in items}
    assert not any("registry.access" in r for r in references)


def test_collect_signing_items_multi_arch_creates_item_per_digest() -> None:
    """A signing item is created for every digest in a multi-arch image."""
    digests = ["sha256:index", "sha256:amd64", "sha256:arm64"]
    items = collect_signing_items(COMPONENT, set(), digests, None, ["key-a"])

    assert len(items) == 6  # 3 digests x 2 tags x 1 key
    assert {i.digest for i in items} == {"sha256:index", "sha256:amd64", "sha256:arm64"}


def test_collect_signing_items_creates_item_per_key() -> None:
    """A signing item is created for each signing key."""
    items = collect_signing_items(COMPONENT, set(), ["sha256:abc"], None, ["key-a", "key-b"])

    assert len(items) == 4  # 2 tags x 1 digest x 2 keys
    assert {i.key for i in items} == {"key-a", "key-b"}


def test_collect_signing_items_source_container_adds_source_tag() -> None:
    """Source container items use a -source tag suffix for each regular tag."""
    items = collect_signing_items(COMPONENT, set(), ["sha256:abc"], "sha256:src", ["key-a"])

    source_items = [i for i in items if i.digest == "sha256:src"]
    assert len(source_items) == 2  # 2 tags x 1 key
    references = {i.reference for i in source_items}
    assert "registry.redhat.io/myproduct/myrepo:v1.0-source" in references
    assert "registry.redhat.io/myproduct/myrepo:latest-source" in references


def test_collect_signing_items_source_container_uses_source_digest() -> None:
    """Source container items carry the source digest, not the image digest."""
    items = collect_signing_items(COMPONENT, set(), ["sha256:abc"], "sha256:src", ["key-a"])

    for item in items:
        if "source" in item.reference:
            assert item.digest == "sha256:src"
        else:
            assert item.digest == "sha256:abc"


def test_collect_signing_items_repository_stripped_of_registry() -> None:
    """The repository field contains only the path without the registry hostname."""
    items = collect_signing_items(COMPONENT, set(), ["sha256:abc"], None, ["key-a"])

    assert all(i.repository == "myproduct/myrepo" for i in items)


# --- filter_already_signed ---


def test_filter_already_signed_returns_all_when_nothing_signed() -> None:
    """All items are returned when Pyxis has no existing signatures."""
    items = [
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:v1.0",
            "sha256:abc",
            "myproduct/myrepo",
            "key-a",
        ),
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:latest",
            "sha256:abc",
            "myproduct/myrepo",
            "key-a",
        ),
    ]
    with patch("rh_direct_sign_image.find_existing_signatures", return_value={}):
        result = filter_already_signed(items, PYXIS_URL)

    assert result == items


def test_filter_already_signed_removes_already_signed_item() -> None:
    """Items already signed in Pyxis are excluded from the result."""
    items = [
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:v1.0",
            "sha256:abc",
            "myproduct/myrepo",
            "key-a",
        ),
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:latest",
            "sha256:abc",
            "myproduct/myrepo",
            "key-a",
        ),
    ]
    existing = {
        ("sha256:abc", "myproduct/myrepo"): {
            PyxisSignature("registry.redhat.io/myproduct/myrepo:v1.0", "key-a")
        }
    }
    with patch("rh_direct_sign_image.find_existing_signatures", return_value=existing):
        result = filter_already_signed(items, PYXIS_URL)

    references = {i.reference for i in result}
    assert "registry.redhat.io/myproduct/myrepo:v1.0" not in references
    assert "registry.redhat.io/myproduct/myrepo:latest" in references


def test_filter_already_signed_keeps_item_when_different_key_signed() -> None:
    """An item signed with a different key is not filtered out."""
    items = [
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:v1.0",
            "sha256:abc",
            "myproduct/myrepo",
            "key-a",
        ),
    ]
    existing = {
        ("sha256:abc", "myproduct/myrepo"): {
            PyxisSignature("registry.redhat.io/myproduct/myrepo:v1.0", "other-key")
        }
    }
    with patch("rh_direct_sign_image.find_existing_signatures", return_value=existing):
        result = filter_already_signed(items, PYXIS_URL)

    assert len(result) == 1


def test_filter_already_signed_source_container_items_filtered() -> None:
    """Already-signed source container items are excluded from the result."""
    items = [
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:v1.0-source",
            "sha256:src",
            "myproduct/myrepo",
            "key-a",
        ),
        SigningItem(
            "registry.redhat.io/myproduct/myrepo:latest-source",
            "sha256:src",
            "myproduct/myrepo",
            "key-a",
        ),
    ]
    existing = {
        ("sha256:src", "myproduct/myrepo"): {
            PyxisSignature("registry.redhat.io/myproduct/myrepo:v1.0-source", "key-a")
        }
    }
    with patch("rh_direct_sign_image.find_existing_signatures", return_value=existing):
        result = filter_already_signed(items, PYXIS_URL)

    references = {i.reference for i in result}
    assert "registry.redhat.io/myproduct/myrepo:v1.0-source" not in references
    assert "registry.redhat.io/myproduct/myrepo:latest-source" in references


def test_filter_already_signed_passes_exact_pairs_to_pyxis() -> None:
    """find_existing_signatures is called with the exact (digest, repo) pairs from items."""
    items = [
        SigningItem("registry.redhat.io/prod/repo:v1.0", "sha256:aaa", "prod/repo", "key-a"),
        SigningItem("registry.redhat.io/other/repo:v1.0", "sha256:bbb", "other/repo", "key-a"),
    ]
    with patch("rh_direct_sign_image.find_existing_signatures", return_value={}) as mock_find:
        filter_already_signed(items, PYXIS_URL)

    _, lookups, *_ = mock_find.call_args.args
    assert lookups == {("sha256:aaa", "prod/repo"), ("sha256:bbb", "other/repo")}


# --- process_component ---


def test_process_component_returns_all_candidates() -> None:
    """All signing candidates are collected without filtering against Pyxis."""
    with (
        patch("rh_direct_sign_image.get_all_image_digests", return_value=["sha256:abc"]),
        patch("rh_direct_sign_image.get_source_container_digest", return_value=None),
    ):
        candidates = process_component(COMPONENT, DATA_FILE, set(), signing_keys=["key-a"])

    assert len(candidates) == 2
    references = {i.reference for i in candidates}
    assert "registry.redhat.io/myproduct/myrepo:v1.0" in references
    assert "registry.redhat.io/myproduct/myrepo:latest" in references


def test_process_component_includes_source_container_candidates() -> None:
    """Source container signing candidates are included in the returned list."""
    with (
        patch("rh_direct_sign_image.get_all_image_digests", return_value=["sha256:abc"]),
        patch("rh_direct_sign_image.get_source_container_digest", return_value="sha256:src"),
    ):
        candidates = process_component(COMPONENT, DATA_FILE, set(), signing_keys=["key-a"])

    references = {i.reference for i in candidates}
    assert "registry.redhat.io/myproduct/myrepo:v1.0-source" in references
    assert "registry.redhat.io/myproduct/myrepo:latest-source" in references


# --- get_all_image_digests ---

_SINGLE_MANIFEST = json.dumps(
    {"mediaType": "application/vnd.oci.image.manifest.v1+json", "schemaVersion": 2}
)
_INDEX_MANIFEST = json.dumps(
    {
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {"digest": "sha256:amd64"},
            {"digest": "sha256:arm64"},
        ],
    }
)
_HELM_MANIFEST = json.dumps({"schemaVersion": 2})  # no mediaType, no manifests


def test_get_all_image_digests_single_arch_returns_only_top_level() -> None:
    """A single-arch image returns only its own digest."""
    ref = "registry.io/repo/image@sha256:toplevel"
    with patch("rh_direct_sign_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=_SINGLE_MANIFEST)
        digests = get_all_image_digests(ref)

    assert digests == ["sha256:toplevel"]


def test_get_all_image_digests_multi_arch_includes_nested_digests() -> None:
    """A multi-arch index image includes the top-level and all nested digests."""
    ref = "registry.io/repo/image@sha256:index"
    with patch("rh_direct_sign_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=_INDEX_MANIFEST)
        digests = get_all_image_digests(ref)

    assert digests == ["sha256:index", "sha256:amd64", "sha256:arm64"]


def test_get_all_image_digests_helm_chart_returns_only_top_level() -> None:
    """Single-manifest artifact with no nested manifests returns only the top-level digest."""
    ref = "registry.io/repo/chart@sha256:chart"
    with patch("rh_direct_sign_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=_HELM_MANIFEST)
        digests = get_all_image_digests(ref)

    assert digests == ["sha256:chart"]


def test_get_all_image_digests_calls_select_oci_auth_with_reference() -> None:
    """select-oci-auth is called with the image reference before skopeo inspect."""
    ref = "registry.io/repo/image@sha256:abc"
    with patch("rh_direct_sign_image.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(stdout=_SINGLE_MANIFEST),
        ]
        get_all_image_digests(ref)

    first_call = mock_run.call_args_list[0].args[0]
    assert first_call == ["select-oci-auth", ref]


def test_get_all_image_digests_passes_authfile_to_skopeo() -> None:
    """Skopeo inspect is called with --authfile pointing to the auth credentials."""
    ref = "registry.io/repo/image@sha256:abc"
    with patch("rh_direct_sign_image.run_cmd") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="{}"),
            MagicMock(stdout=_SINGLE_MANIFEST),
        ]
        get_all_image_digests(ref)

    skopeo_call = mock_run.call_args_list[1].args[0]
    assert skopeo_call[0] == "skopeo"
    assert "--authfile" in skopeo_call
    assert f"docker://{ref}" in skopeo_call


# --- get_source_container_digest ---


def test_get_source_container_digest_returns_none_when_disabled() -> None:
    """Returns None when pushSourceContainer is False."""
    component = {"containerImage": "registry.io/repo@sha256:abc", "pushSourceContainer": False}
    result = get_source_container_digest(component, default_push_source_container=True)
    assert result is None


def test_get_source_container_digest_uses_default_when_flag_absent() -> None:
    """Returns None when pushSourceContainer is absent and default is False."""
    component = {"containerImage": "registry.io/repo@sha256:abc"}
    result = get_source_container_digest(component, default_push_source_container=False)
    assert result is None


def test_get_source_container_digest_resolves_digest() -> None:
    """Delegates to oras_resolve and returns the digest."""
    component = {
        "containerImage": "registry.io/repo@sha256:deadbeef",
        "pushSourceContainer": True,
    }
    with patch("rh_direct_sign_image.oras_resolve", return_value="sha256:sourceabc"):
        result = get_source_container_digest(component, default_push_source_container=False)

    assert result == "sha256:sourceabc"


def test_get_source_container_digest_constructs_correct_source_reference() -> None:
    """Source reference is built as <repo>:sha256-<sha>.src."""
    component = {
        "containerImage": "registry.io/myrepo@sha256:deadbeef",
        "pushSourceContainer": True,
    }
    with patch("rh_direct_sign_image.oras_resolve", return_value="sha256:src") as mock_resolve:
        get_source_container_digest(component, default_push_source_container=False)

    mock_resolve.assert_called_once_with("registry.io/myrepo:sha256-deadbeef.src")


# --- find_existing_signatures ---


def test_find_existing_signatures_queries_each_pair() -> None:
    """find_signatures_for_repository is called once per (digest, repo) pair."""
    lookups = {("sha256:aaa", "repo/a"), ("sha256:bbb", "repo/b")}
    sig_a = PyxisSignature("registry.io/repo/a:tag", "key")
    with patch(
        "rh_direct_sign_image.find_signatures_for_repository",
        side_effect=[{sig_a}, set()],
    ):
        results = find_existing_signatures(PYXIS_URL, lookups)

    assert len(results) == 2
    assert any(results[k] == {sig_a} for k in results)


def test_find_existing_signatures_returns_empty_set_for_missing_pair() -> None:
    """A pair with no signatures maps to an empty set."""
    lookups = {("sha256:abc", "repo/x")}
    with patch("rh_direct_sign_image.find_signatures_for_repository", return_value=set()):
        results = find_existing_signatures(PYXIS_URL, lookups)

    assert results[("sha256:abc", "repo/x")] == set()


# --- main ---


def test_main_collects_candidates_and_writes_batches(tmp_path) -> None:
    """Main writes batch files when --output is provided."""
    snapshot = {"components": [COMPONENT]}
    data_file = {"mapping": {"defaults": {"pushSourceContainer": False}}}
    sign_file = tmp_path / "sign.txt"
    sign_file.write_text("")
    snap_file = tmp_path / "snapshot.json"
    snap_file.write_text(json.dumps(snapshot))
    data_path = tmp_path / "data.json"
    data_path.write_text(json.dumps(data_file))
    output_dir = tmp_path / "batches"

    item = SigningItem(
        "registry.redhat.io/myproduct/myrepo:v1.0", "sha256:abc", "myproduct/myrepo", "key-a"
    )
    with (
        patch(
            "rh_direct_sign_image.get_configmap",
            return_value={"data": {"SIG_KEY_NAME": "key-a"}},
        ),
        patch("rh_direct_sign_image.process_component", return_value=[item]),
        patch("rh_direct_sign_image.filter_already_signed", return_value=[item]),
        patch(
            "sys.argv",
            [
                "prepare_container_signing",
                "--pyxis-server",
                "stage",
                "--snapshot",
                str(snap_file),
                "--data-file",
                str(data_path),
                "--sign-registry-access-file",
                str(sign_file),
                "--output",
                str(output_dir),
                "--requester",
                "tester",
                "--pipeline-image",
                "quay.io/signing/pipeline:latest",
            ],
        ),
    ):
        main()

    assert output_dir.is_dir()
    assert len(list(output_dir.iterdir())) == 1


# --- validate_file ---


def test_validate_file_returns_path_for_existing_file(tmp_path) -> None:
    """validate_file returns a Path when the file exists."""
    f = tmp_path / "test.txt"
    f.write_text("x")
    assert validate_file(str(f)) == f


def test_validate_file_raises_for_missing_file(tmp_path) -> None:
    """validate_file raises FileNotFoundError when the file does not exist."""
    with pytest.raises(FileNotFoundError):
        validate_file(str(tmp_path / "missing.txt"))


def test_main_returns_zero_on_success(tmp_path) -> None:
    """Main returns 0 when the workflow completes successfully."""
    snapshot = {"components": []}
    data_file = {"mapping": {"defaults": {"pushSourceContainer": False}}}
    sign_file = tmp_path / "sign.txt"
    sign_file.write_text("")
    snap_file = tmp_path / "snapshot.json"
    snap_file.write_text(json.dumps(snapshot))
    data_path = tmp_path / "data.json"
    data_path.write_text(json.dumps(data_file))

    with (
        patch(
            "rh_direct_sign_image.get_configmap",
            return_value={"data": {"SIG_KEY_NAME": "key-a"}},
        ),
        patch("rh_direct_sign_image.filter_already_signed", return_value=[]),
        patch(
            "sys.argv",
            [
                "prepare_container_signing",
                "--pyxis-server",
                "stage",
                "--snapshot",
                str(snap_file),
                "--data-file",
                str(data_path),
                "--sign-registry-access-file",
                str(sign_file),
                "--requester",
                "tester",
                "--pipeline-image",
                "quay.io/signing/pipeline:latest",
            ],
        ),
    ):
        result = main()

    assert result == 0


def test_main_returns_one_on_unexpected_error(tmp_path) -> None:
    """Main returns 1 and does not propagate exceptions on unexpected failure."""
    sign_file = tmp_path / "sign.txt"
    sign_file.write_text("")
    snap_file = tmp_path / "snapshot.json"
    snap_file.write_text(json.dumps({"components": []}))
    data_path = tmp_path / "data.json"
    data_path.write_text(json.dumps({}))

    with (
        patch(
            "rh_direct_sign_image.get_configmap",
            side_effect=RuntimeError("something went wrong"),
        ),
        patch(
            "sys.argv",
            [
                "prepare_container_signing",
                "--pyxis-server",
                "stage",
                "--snapshot",
                str(snap_file),
                "--data-file",
                str(data_path),
                "--sign-registry-access-file",
                str(sign_file),
                "--requester",
                "tester",
                "--pipeline-image",
                "quay.io/signing/pipeline:latest",
            ],
        ),
    ):
        result = main()

    assert result == 1


# --- get_submit_config ---

_FULL_CONFIGMAP = {
    "data": {
        "SIG_KEY_NAME": "key-a",
        "PYXIS_SSL_CERT_SECRET_NAME": "pyxis-cert-secret",
        "PYXIS_GRAPHQL_URL": "https://graphql-pyxis.example.com/graphql/",
        "KERBEROS_PRINCIPAL": "svc@REALM",
        "KERBEROS_KEYTAB": "/etc/keytab",
        "KERBEROS_KEYTAB_SECRET": "keytab-secret",
    }
}


def _make_submit_args(**overrides):
    """Return a Namespace with all submit-related args set to safe defaults."""
    import argparse

    defaults = dict(
        pipeline="container-signing",
        requester="tester",
        request_timeout="1800",
        pipeline_timeout="0h30m0s",
        task_timeout="0h25m0s",
        service_account="signing-pipeline-sa",
        task_id="task-uid-123",
        pipelinerun_uid="pr-uid-456",
        signing_repo="https://gitlab.cee.redhat.com/signing/signing.git",
        signing_revision="main",
        concurrent_limit=8,
        pipeline_image="quay.io/signing/pipeline:latest",
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def test_get_submit_config_builds_from_configmap_and_args() -> None:
    """get_submit_config maps configmap fields and CLI args into a SubmitConfig."""
    args = _make_submit_args()
    cfg = get_submit_config(_FULL_CONFIGMAP, args, {})

    assert cfg.pyxis_ssl_cert_secret_name == "pyxis-cert-secret"
    assert cfg.pyxis_graphql_url == "https://graphql-pyxis.example.com/graphql/"
    assert cfg.kerberos_principal == "svc@REALM"
    assert cfg.kerberos_keytab == "/etc/keytab"
    assert cfg.kerberos_keytab_secret == "keytab-secret"
    assert cfg.pipeline == "container-signing"
    assert cfg.requester == "tester"
    assert cfg.task_id == "task-uid-123"
    assert cfg.pipelinerun_uid == "pr-uid-456"
    assert cfg.concurrent_limit == 8
    assert cfg.pipeline_image == "quay.io/signing/pipeline:latest"


def test_get_submit_config_uses_intention_from_data_file() -> None:
    """get_submit_config reads intention from data_file."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(), {"intention": "release"})
    assert cfg.intention == "release"


def test_get_submit_config_defaults_intention_to_unknown() -> None:
    """get_submit_config defaults intention to 'unknown' when absent from data_file."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(), {})
    assert cfg.intention == "unknown"


def test_get_submit_config_raises_on_missing_configmap_key() -> None:
    """get_submit_config raises KeyError when a required configmap key is absent."""
    bad_cm = {"data": {"SIG_KEY_NAME": "key-a"}}
    with pytest.raises(KeyError):
        get_submit_config(bad_cm, _make_submit_args(), {})


# --- submit_batch ---


def test_submit_batch_reads_file_and_calls_internal_request(tmp_path) -> None:
    """submit_batch reads the batch file content and passes it to internal-request."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(), {})
    batch_file = tmp_path / "batch_0000.txt"
    batch_file.write_text("base64content==")

    with patch("rh_direct_sign_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        submit_batch(batch_file, cfg)

    cmd = mock_run.call_args.args[0]
    assert cmd[0] in ("internal-request", "internal-request")
    assert "--pipeline" in cmd and "container-signing" in cmd
    assert "-p" in cmd
    assert any("signing_requests=base64content==" in a for a in cmd)
    assert any("requester=tester" in a for a in cmd)
    assert any("pyxis_ssl_cert_secret_name=pyxis-cert-secret" in a for a in cmd)
    assert any(
        "pyxis_graphql_url=https://graphql-pyxis.example.com/graphql/" in a for a in cmd
    )
    assert any("kerberos_principal=svc@REALM" in a for a in cmd)
    assert any("kerberos_keytab=/etc/keytab" in a for a in cmd)
    assert any("kerberos_keytab_secret=keytab-secret" in a for a in cmd)
    assert any(
        "internal-services.appstudio.openshift.io/group-id=task-uid-123" in a for a in cmd
    )
    assert any(
        "internal-services.appstudio.openshift.io/pipelinerun-uid=pr-uid-456" in a for a in cmd
    )
    assert any("internal-services.appstudio.openshift.io/rate-limited=true" in a for a in cmd)
    assert "-t" in cmd and "1800" in cmd
    assert "--pipeline-timeout" in cmd and "0h30m0s" in cmd
    assert "--task-timeout" in cmd and "0h25m0s" in cmd
    assert "--service-account" in cmd and "signing-pipeline-sa" in cmd
    assert "-s" in cmd and "true" in cmd
    assert any("pipeline_image=quay.io/signing/pipeline:latest" in a for a in cmd)


def test_submit_batch_includes_intention_label(tmp_path) -> None:
    """submit_batch passes the intention label to internal-request."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(), {"intention": "release"})
    batch_file = tmp_path / "batch_0000.txt"
    batch_file.write_text("base64content==")

    with patch("rh_direct_sign_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        submit_batch(batch_file, cfg)

    cmd = mock_run.call_args.args[0]
    assert any("internal-services.appstudio.openshift.io/intention=release" in a for a in cmd)


# --- submit_batches ---


def test_submit_batches_submits_each_batch_file(tmp_path) -> None:
    """submit_batches calls submit_batch once per file in the batch directory."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(), {})
    for i, content in enumerate(["batch1==", "batch2==", "batch3=="]):
        (tmp_path / f"batch_{i:04d}.txt").write_text(content)

    with patch("rh_direct_sign_image.submit_batch") as mock_submit:
        submit_batches(tmp_path, cfg)

    assert mock_submit.call_count == 3
    submitted_paths = {call.args[0] for call in mock_submit.call_args_list}
    assert submitted_paths == {
        tmp_path / "batch_0000.txt",
        tmp_path / "batch_0001.txt",
        tmp_path / "batch_0002.txt",
    }


def test_submit_batches_uses_concurrent_limit(tmp_path) -> None:
    """submit_batches passes concurrent_limit as max_workers to ThreadPoolExecutor."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(concurrent_limit=3), {})
    (tmp_path / "batch_0000.txt").write_text("b1")

    with (
        patch("rh_direct_sign_image.submit_batch"),
        patch("rh_direct_sign_image.ThreadPoolExecutor") as mock_pool_cls,
        patch("rh_direct_sign_image.as_completed", return_value=[]),
    ):
        mock_pool = MagicMock()
        mock_pool_cls.return_value.__enter__ = MagicMock(return_value=mock_pool)
        mock_pool_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_pool.submit = MagicMock(return_value=MagicMock())
        submit_batches(tmp_path, cfg)

    mock_pool_cls.assert_called_once_with(max_workers=3)


def test_submit_batches_logs_all_succeeded(tmp_path) -> None:
    """submit_batches logs that all batches succeeded when none fail."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(), {})
    for i in range(3):
        (tmp_path / f"batch_{i:04d}.txt").write_text(f"b{i}")

    with (
        patch("rh_direct_sign_image.submit_batch"),
        patch("rh_direct_sign_image.LOGGER") as mock_logger,
    ):
        submit_batches(tmp_path, cfg)

    mock_logger.info.assert_any_call("Batch submission summary: %d succeeded, %d failed", 3, 0)


def test_submit_batches_logs_partial_failure_counts(tmp_path) -> None:
    """submit_batches logs correct success/failure counts when some batches fail."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(), {})
    for i in range(3):
        (tmp_path / f"batch_{i:04d}.txt").write_text(f"b{i}")

    call_count = 0

    def fail_first(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("request failed")

    with (
        patch("rh_direct_sign_image.submit_batch", side_effect=fail_first),
        patch("rh_direct_sign_image.LOGGER") as mock_logger,
        pytest.raises(RuntimeError),
    ):
        submit_batches(tmp_path, cfg)

    mock_logger.info.assert_any_call("Batch submission summary: %d succeeded, %d failed", 2, 1)


def test_submit_batches_raises_after_logging_summary(tmp_path) -> None:
    """submit_batches raises RuntimeError after logging the summary when batches fail."""
    cfg = get_submit_config(_FULL_CONFIGMAP, _make_submit_args(), {})
    (tmp_path / "batch_0000.txt").write_text("b1")

    with (
        patch("rh_direct_sign_image.submit_batch", side_effect=RuntimeError("request failed")),
        pytest.raises(RuntimeError, match="1 batch.*failed"),
    ):
        submit_batches(tmp_path, cfg)


# --- setup_argparser with submit flags ---


def test_setup_argparser_submit_requests_defaults(tmp_path) -> None:
    """--submit-requests is False by default; submission flags have correct defaults."""
    snap = tmp_path / "s.json"
    snap.write_text("{}")
    data = tmp_path / "d.json"
    data.write_text("{}")
    sign = tmp_path / "r.txt"
    sign.write_text("")

    parser = setup_argparser()
    args = parser.parse_args(
        [
            "--pyxis-server",
            "stage",
            "--snapshot",
            str(snap),
            "--data-file",
            str(data),
            "--sign-registry-access-file",
            str(sign),
            "--requester",
            "tester",
            "--pipeline-image",
            "quay.io/signing/pipeline:latest",
        ]
    )

    assert args.submit_requests is False
    assert args.concurrent_limit == 8
    assert args.request_timeout == "1800"
    assert args.pipeline_timeout == "0h30m0s"
    assert args.task_timeout == "0h25m0s"
    assert args.service_account == "signing-pipeline-sa"


# --- main with submit ---


def test_main_submits_batches_when_flag_set(tmp_path) -> None:
    """Main writes batches to files and calls submit_batches when --submit-requests is set."""
    snapshot = {"components": []}
    data_file = {"mapping": {"defaults": {"pushSourceContainer": False}}}
    sign_file = tmp_path / "sign.txt"
    sign_file.write_text("")
    snap_file = tmp_path / "snapshot.json"
    snap_file.write_text(json.dumps(snapshot))
    data_path = tmp_path / "data.json"
    data_path.write_text(json.dumps(data_file))
    output_dir = tmp_path / "batches"

    with (
        patch(
            "rh_direct_sign_image.get_configmap",
            return_value=_FULL_CONFIGMAP,
        ),
        patch(
            "rh_direct_sign_image.filter_already_signed",
            return_value=[
                SigningItem("registry.redhat.io/repo:tag", "sha256:abc", "repo", "key-a"),
            ],
        ),
        patch("rh_direct_sign_image.submit_batches") as mock_submit,
        patch(
            "sys.argv",
            [
                "rh_direct_sign_image",
                "--pyxis-server",
                "stage",
                "--snapshot",
                str(snap_file),
                "--data-file",
                str(data_path),
                "--sign-registry-access-file",
                str(sign_file),
                "--output",
                str(output_dir),
                "--submit-requests",
                "--pipeline",
                "container-signing",
                "--requester",
                "tester",
                "--pipeline-image",
                "quay.io/signing/pipeline:latest",
                "--task-id",
                "task-123",
                "--pipelinerun-uid",
                "pr-456",
            ],
        ),
    ):
        result = main()

    assert result == 0
    mock_submit.assert_called_once()
    # first arg to submit_batches is the batch directory
    assert mock_submit.call_args.args[0] == output_dir


def test_main_uses_temp_dir_when_output_not_specified(tmp_path) -> None:
    """Main uses a temporary directory for batches when --output is omitted."""
    snapshot = {"components": []}
    data_file = {"mapping": {"defaults": {"pushSourceContainer": False}}}
    sign_file = tmp_path / "sign.txt"
    sign_file.write_text("")
    snap_file = tmp_path / "snapshot.json"
    snap_file.write_text(json.dumps(snapshot))
    data_path = tmp_path / "data.json"
    data_path.write_text(json.dumps(data_file))

    with (
        patch(
            "rh_direct_sign_image.get_configmap",
            return_value=_FULL_CONFIGMAP,
        ),
        patch(
            "rh_direct_sign_image.filter_already_signed",
            return_value=[
                SigningItem("registry.redhat.io/repo:tag", "sha256:abc", "repo", "key-a"),
            ],
        ),
        patch("rh_direct_sign_image.submit_batches") as mock_submit,
        patch(
            "sys.argv",
            [
                "rh_direct_sign_image",
                "--pyxis-server",
                "stage",
                "--snapshot",
                str(snap_file),
                "--data-file",
                str(data_path),
                "--sign-registry-access-file",
                str(sign_file),
                "--submit-requests",
                "--pipeline",
                "container-signing",
                "--requester",
                "tester",
                "--pipeline-image",
                "quay.io/signing/pipeline:latest",
                "--task-id",
                "task-123",
                "--pipelinerun-uid",
                "pr-456",
            ],
        ),
    ):
        result = main()

    assert result == 0
    mock_submit.assert_called_once()
    # batch dir is some temporary path (not None)
    assert mock_submit.call_args.args[0] is not None


def test_main_always_writes_batches(tmp_path) -> None:
    """Main writes batches to a temp dir even when --output is omitted."""
    snapshot = {"components": []}
    data_file = {"mapping": {"defaults": {"pushSourceContainer": False}}}
    sign_file = tmp_path / "sign.txt"
    sign_file.write_text("")
    snap_file = tmp_path / "snapshot.json"
    snap_file.write_text(json.dumps(snapshot))
    data_path = tmp_path / "data.json"
    data_path.write_text(json.dumps(data_file))

    with (
        patch(
            "rh_direct_sign_image.get_configmap",
            return_value={"data": {"SIG_KEY_NAME": "key-a"}},
        ),
        patch(
            "rh_direct_sign_image.filter_already_signed",
            return_value=[
                SigningItem("registry.redhat.io/repo:tag", "sha256:abc", "repo", "key-a"),
            ],
        ),
        patch("rh_direct_sign_image.write_batches") as mock_write,
        patch(
            "sys.argv",
            [
                "prepare_container_signing",
                "--pyxis-server",
                "stage",
                "--snapshot",
                str(snap_file),
                "--data-file",
                str(data_path),
                "--sign-registry-access-file",
                str(sign_file),
                "--requester",
                "tester",
                "--pipeline-image",
                "quay.io/signing/pipeline:latest",
            ],
        ),
    ):
        result = main()

    assert result == 0
    mock_write.assert_called_once()
