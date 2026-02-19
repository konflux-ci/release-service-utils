import os
import json
import pytest
from unittest.mock import MagicMock, call, patch, ANY
import publish_to_cgw_wrapper as cgw_wrapper


@pytest.fixture
def cosign_content_dir(tmpdir):
    content_dir = tmpdir.mkdir("content_dir_1")
    files = [
        "cosign",
        "cosign-linux-amd64.gz",
        "cosign-darwin-amd64.gz",
        "fake-name-linux-amd64.gz",
        "ignored",
        "sha256sum.txt",
        "sha256sum.txt.gpg",
        "sha256sum.txt.sig",
    ]
    for filename in files:
        content_dir.join(filename).write("")
    return content_dir


@pytest.fixture
def gitsign_content_dir(tmpdir):
    content_dir = tmpdir.mkdir("content_dir_2")
    files = [
        "gitsign",
        "gitsign-linux-amd64.gz",
        "gitsign-darwin-amd64.gz",
        "ignored",
        "sha256sum.txt",
        "sha256sum.txt.gpg",
        "sha256sum.txt.sig",
    ]
    for filename in files:
        content_dir.join(filename).write("")
    return content_dir


@pytest.fixture
def data_json(cosign_content_dir, gitsign_content_dir):
    return {
        "application": "test-app",
        "artifacts": {},
        "components": [
            {
                "containerImage": "quay.io/org/tenant/cosign@sha256:abcdef12345",
                "name": "cosign",
                "files": [
                    {"source": "/releases/cosign", "arch": "amd64", "os": "linux"},
                    {
                        "source": "/releases/cosign-linux-amd64.gz",
                        "arch": "amd64",
                        "os": "linux",
                    },
                    {
                        "source": "/releases/cosign-darwin-amd64.gz",
                        "arch": "amd64",
                        "os": "darwin",
                    },
                ],
                "contentGateway": {
                    "productName": "product_name_1",
                    "productCode": "product_code_1",
                    "productVersionName": "1.1",
                    "contentDir": str(cosign_content_dir),
                },
            },
            {
                "containerImage": "quay.io/org/tenant/gitsign@sha256:abcdef12345",
                "name": "gitsign",
                "files": [
                    {"source": "/releases/gitsign", "arch": "amd64", "os": "linux"},
                    {
                        "source": "/releases/gitsign-linux-amd64.gz",
                        "arch": "amd64",
                        "os": "linux",
                    },
                    {
                        "source": "/releases/gitsign-darwin-amd64.gz",
                        "arch": "amd64",
                        "os": "darwin",
                    },
                ],
                "contentGateway": {
                    "productName": "product_name_2",
                    "productCode": "product_code_2",
                    "productVersionName": "1.2",
                    "contentDir": str(gitsign_content_dir),
                },
            },
        ],
    }


@pytest.fixture
def metadata():
    """Metadata fixture: checksums order 1-3, RPA files order component_index*1000+i."""
    h = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    dl = f"/content/origin/files/sha256/{h[:2]}/{h}"
    base = {
        "type": "FILE",
        "hidden": False,
        "invisible": False,
        "productVersionId": 4156067,
    }
    return [
        {
            **base,
            "shortURL": "/cgw/product_code_1/1.1/sha256sum.txt",
            "downloadURL": f"{dl}/sha256sum.txt",
            "label": "Checksum",
            "order": 1,
        },
        {
            **base,
            "shortURL": "/cgw/product_code_1/1.1/sha256sum.txt.gpg",
            "downloadURL": f"{dl}/sha256sum.txt.gpg",
            "label": "Checksum - GPG",
            "order": 2,
        },
        {
            **base,
            "shortURL": "/cgw/product_code_1/1.1/sha256sum.txt.sig",
            "downloadURL": f"{dl}/sha256sum.txt.sig",
            "label": "Checksum - Signature",
            "order": 3,
        },
        {
            **base,
            "shortURL": "/cgw/product_code_1/1.1/cosign",
            "downloadURL": f"{dl}/cosign",
            "label": "cosign",
            "order": 1000,
        },
        {
            **base,
            "shortURL": "/cgw/product_code_1/1.1/cosign-linux-amd64.gz",
            "downloadURL": f"{dl}/cosign-linux-amd64.gz",
            "label": "cosign-linux-amd64.gz",
            "order": 1001,
        },
        {
            **base,
            "shortURL": "/cgw/product_code_1/1.1/cosign-darwin-amd64.gz",
            "downloadURL": f"{dl}/cosign-darwin-amd64.gz",
            "label": "cosign-darwin-amd64.gz",
            "order": 1002,
        },
    ]


@patch("publish_to_cgw_wrapper.call_cgw_api")
@patch.dict(os.environ, {"CGW_USERNAME": "username", "CGW_PASSWORD": "password"})
@patch("sys.argv", new_callable=lambda: ["publish_to_cgw_wrapper.py"])
def test_main_creates_files(
    mock_sys_argv,
    mock_call,
    data_json,
):
    """
    Test main() for two components where all files are created:
    - First component processes 6 files (3 checksums + 3 RPA) and creates all 6.
    - Second component processes 6 files (3 checksums + 3 RPA) and creates all 6.
    """
    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_json",
        json.dumps(data_json),
    ]
    mock_sys_argv.extend(test_args)

    mock_call.side_effect = [
        # For first component
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356067, "name": "product_name_1", "productCode": "product_code_1"}
            ]
        ),
        MagicMock(json=lambda: [{"id": 4156067, "versionName": "1.1"}]),  # Get version
        MagicMock(json=lambda: []),  # existing files
        MagicMock(json=lambda: 4567),  # sha256sum.txt (created)
        MagicMock(json=lambda: 4568),  # sha256sum.txt.gpg (created)
        MagicMock(json=lambda: 4569),  # sha256sum.txt.sig (created)
        MagicMock(json=lambda: 4570),  # cosign (created)
        MagicMock(json=lambda: 4571),  # cosign-linux-amd64.gz (created)
        MagicMock(json=lambda: 4572),  # cosign-darwin-amd64.gz (created)
        # For second component
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356068, "name": "product_name_2", "productCode": "product_code_2"}
            ]
        ),
        MagicMock(json=lambda: [{"id": 4156068, "versionName": "1.2"}]),  # Get version
        MagicMock(json=lambda: []),  # existing files
        MagicMock(json=lambda: 4573),  # sha256sum.txt (created)
        MagicMock(json=lambda: 4574),  # sha256sum.txt.gpg (created)
        MagicMock(json=lambda: 4575),  # sha256sum.txt.sig (created)
        MagicMock(json=lambda: 4576),  # gitsign (created)
        MagicMock(json=lambda: 4577),  # gitsign-linux-amd64.gz (created)
        MagicMock(json=lambda: 4578),  # gitsign-darwin-amd64.gz (created)
    ]

    results_json = cgw_wrapper.main()
    assert results_json[0]["no_of_files_processed"] == 6
    assert results_json[0]["no_of_files_created"] == 6
    assert results_json[0]["no_of_files_skipped"] == 0
    assert results_json[1]["no_of_files_processed"] == 6
    assert results_json[1]["no_of_files_created"] == 6
    assert results_json[1]["no_of_files_skipped"] == 0


@patch("publish_to_cgw_wrapper.call_cgw_api")
@patch.dict(os.environ, {"CGW_USERNAME": "user", "CGW_PASSWORD": "password"})
@patch("sys.argv", new_callable=lambda: ["publish_to_cgw_wrapper.py"])
def test_main_skips_3_creates_2(mock_sys_argv, mock_call, data_json, metadata):
    """
    Test main() for two components:
    - First component should skip 3 existing files and create 3 new files.
    - Second component should create 6 new files with no skips.
    """
    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_json",
        json.dumps(data_json),
    ]
    mock_sys_argv.extend(test_args)

    existing_files_c1 = [
        {**metadata[0], "id": 4567},  # sha256sum.txt (exists)
        {**metadata[1], "id": 4568},  # sha256sum.txt.gpg (exists)
        {**metadata[2], "id": 4569},  # sha256sum.txt.sig (exists)
    ]
    mock_call.side_effect = [
        # For first component
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356067, "name": "product_name_1", "productCode": "product_code_1"}
            ]
        ),
        MagicMock(json=lambda: [{"id": 4156067, "versionName": "1.1"}]),  # Get version
        MagicMock(json=lambda: existing_files_c1),  # existing files
        MagicMock(json=lambda: 4570),  # cosign (created)
        MagicMock(json=lambda: 4571),  # cosign-linux-amd64.gz (created)
        MagicMock(json=lambda: 4572),  # cosign-darwin-amd64.gz (created)
        # For second component
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356068, "name": "product_name_2", "productCode": "product_code_2"}
            ]
        ),
        MagicMock(json=lambda: [{"id": 4156068, "versionName": "1.2"}]),  # Get version
        MagicMock(json=lambda: []),  # existing files
        MagicMock(json=lambda: 4573),  # sha256sum.txt (created)
        MagicMock(json=lambda: 4574),  # sha256sum.txt.gpg (created)
        MagicMock(json=lambda: 4575),  # sha256sum.txt.sig (created)
        MagicMock(json=lambda: 4576),  # gitsign (created)
        MagicMock(json=lambda: 4577),  # gitsign-linux-amd64.gz (created)
        MagicMock(json=lambda: 4578),  # gitsign-darwin-amd64.gz (created)
    ]

    results_json = cgw_wrapper.main()
    assert results_json[0]["no_of_files_processed"] == 6
    assert results_json[0]["no_of_files_created"] == 3
    assert results_json[0]["no_of_files_skipped"] == 3
    assert results_json[1]["no_of_files_processed"] == 6
    assert results_json[1]["no_of_files_created"] == 6
    assert results_json[1]["no_of_files_skipped"] == 0


@patch("publish_to_cgw_wrapper.call_cgw_api")
@patch.dict(os.environ, {"CGW_USERNAME": "user", "CGW_PASSWORD": "password"})
@patch("sys.argv", new_callable=lambda: ["publish_to_cgw_wrapper.py"])
def test_main_partial_skip_fail_rollback(mock_sys_argv, mock_call, data_json):
    """
    Test main() with 2 components where:
    - The first component successfully creates 6 files.
    - The second component creates 1 file, then fails on the next file.
    - A failure in the 2nd component triggers rollback of its created
      file and all files from the first component.
    """
    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_json",
        json.dumps(data_json),
    ]
    mock_sys_argv.extend(test_args)

    mock_call.side_effect = [
        # First component
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356067, "name": "product_name_1", "productCode": "product_code_1"}
            ]
        ),
        MagicMock(json=lambda: [{"id": 4156067, "versionName": "1.1"}]),  # Get version
        MagicMock(json=lambda: []),  # existing files
        MagicMock(json=lambda: 4567),  # sha256sum.txt (created)
        MagicMock(json=lambda: 4568),  # sha256sum.txt.gpg (created)
        MagicMock(json=lambda: 4569),  # sha256sum.txt.sig (created)
        MagicMock(json=lambda: 4570),  # cosign (created)
        MagicMock(json=lambda: 4571),  # cosign-linux-amd64.gz (created)
        MagicMock(json=lambda: 4572),  # cosign-darwin-amd64.gz (created)
        # Second component
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356068, "name": "product_name_2", "productCode": "product_code_2"}
            ]
        ),
        MagicMock(json=lambda: [{"id": 4156068, "versionName": "1.2"}]),  # Get version
        MagicMock(json=lambda: []),  # existing files
        MagicMock(json=lambda: 4573),  # sha256sum.txt (created)
        RuntimeError("File creation failed in second component"),
        MagicMock(),  # delete file 4573
        MagicMock(),  # delete file 4567
        MagicMock(),  # delete file 4568
        MagicMock(),  # delete file 4569
        MagicMock(),  # delete file 4570
        MagicMock(),  # delete file 4571
        MagicMock(),  # delete file 4572
    ]

    with pytest.raises(SystemExit) as exc:
        cgw_wrapper.main()
    assert exc.value.code == 1

    expected_delete_calls = [
        # Second component file
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356068/versions/4156068/files/4573",
            session=ANY,
        ),
        # First component files
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4567",
            session=ANY,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4568",
            session=ANY,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4569",
            session=ANY,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4570",
            session=ANY,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4571",
            session=ANY,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4572",
            session=ANY,
        ),
    ]
    mock_call.assert_has_calls(expected_delete_calls, any_order=True)
    assert mock_call.call_count == 21
