import os
import json
import pytest
from unittest.mock import MagicMock, call, patch, ANY
import publish_to_cgw_wrapper as cgw_wrapper


@pytest.fixture
def content_dir(tmpdir):
    content_dir = tmpdir.mkdir("content_dir")
    files = [
        "checksum.sig",
        "cosign",
        "cosign-checksum.gpg",
        "cosign-linux-amd64.gz",
        "fake-name-linux-amd64.gz",
        "gitsign-darwin-amd64.gz",
        "podman-darwin-amd64.gz",
        "konflux-darwin-amd64.gz",
        "ignored",
        "sha256778877.txt",
    ]
    for filename in files:
        content_dir.join(filename).write("")
    return content_dir


@pytest.fixture
def data_file():
    return {
        "mapping": {
            "components": [
                {
                    "contentGateway": {
                        "mirrorOpenshiftPush": True,
                        "productName": "product_name_1",
                        "productCode": "product_code_1",
                        "productVersionName": "1.1",
                        "components": [
                            {
                                "name": "cosign",
                                "description": "Red Hat OpenShift Local Sandbox Test",
                                "hidden": False,
                            },
                            {
                                "name": "gitsign",
                                "description": "Red Hat OpenShift Local Sandbox Test",
                                "hidden": False,
                            },
                        ],
                    }
                },
                {
                    "contentGateway": {
                        "mirrorOpenshiftPush": True,
                        "productName": "product_name_2",
                        "productCode": "product_code_2",
                        "productVersionName": "1.2",
                        "components": [
                            {
                                "name": "podman",
                                "description": "Red Hat OpenShift Local Sandbox Test",
                                "hidden": False,
                            },
                            {
                                "name": "konflux",
                                "description": "Red Hat OpenShift Local Sandbox Test",
                                "hidden": False,
                            },
                        ],
                    }
                },
            ]
        }
    }


@pytest.fixture
def metadata():
    """Metadata for component one"""
    return [
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "description": "Red Hat OpenShift Local Sandbox Test",
            "shortURL": "/pub/cgw/product_code_1/1.1/cosign-linux-amd64.gz",
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b9"
                "34ca495991b7852b855/cosign-linux-amd64.gz"
            ),
            "label": "cosign-linux-amd64.gz",
        },
        {
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b9"
                "34ca495991b7852b855/sha256778877.txt"
            ),
            "shortURL": "/pub/cgw/product_code_1/1.1/sha256778877.txt",
            "label": "Checksum",
            "type": "FILE",
            "hidden": False,
            "invisible": False,
        },
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "description": "Red Hat OpenShift Local Sandbox Test",
            "shortURL": "/pub/cgw/product_code_1/1.1/cosign",
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b9"
                "34ca495991b7852b855/cosign"
            ),
            "label": "cosign",
        },
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "description": "Red Hat OpenShift Local Sandbox Test",
            "shortURL": "/pub/cgw/product_code_1/1.1/cosign-checksum.gpg",
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b9"
                "34ca495991b7852b855/cosign-checksum.gpg"
            ),
            "label": "cosign-checksum.gpg",
        },
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "description": "Red Hat OpenShift Local Sandbox Test",
            "shortURL": "/pub/cgw/product_code_1/1.1/gitsign-darwin-amd64.gz",
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b9"
                "34ca495991b7852b855/gitsign-darwin-amd64.gz"
            ),
            "label": "gitsign-darwin-amd64.gz",
        },
    ]


@pytest.fixture
def temp_data_file(tmp_path, data_file):
    """Create a temporary data file."""
    data_file_path = tmp_path / "data.json"
    data_file_path.write_text(json.dumps(data_file))
    return data_file_path


@pytest.fixture
def temp_output_file(tmp_path):
    """Create a temporary output file path."""
    return tmp_path / "result.txt"


@patch("publish_to_cgw_wrapper.call_cgw_api")
@patch.dict(os.environ, {"CGW_USERNAME": "username", "CGW_PASSWORD": "password"})
@patch("sys.argv", new_callable=lambda: ["publish_to_cgw_wrapper.py"])
def test_main_creates_files(
    mock_sys_argv, mock_call, temp_data_file, temp_output_file, content_dir
):
    """
    Test main() for two contentGateway components where all files are created:
    - First component processes 5 files and creates all 5.
    - Second component processes 3 files and creates all 3.
    """
    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_file",
        str(temp_data_file),
        "--content_dir",
        str(content_dir),
        "--output_file",
        str(temp_output_file),
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
        MagicMock(json=lambda: 4567),  # cosign-linux-amd64.gz (created)
        MagicMock(json=lambda: 4568),  # sha256778877.txt (created)
        MagicMock(json=lambda: 4569),  # cosign (created)
        MagicMock(json=lambda: 4570),  # cosign-checksum.gpg (created)
        MagicMock(json=lambda: 4571),  # gitsign-darwin-amd64.gz (created)
        # For second components
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356068, "name": "product_name_2", "productCode": "product_code_2"}
            ]
        ),
        MagicMock(json=lambda: [{"id": 4156068, "versionName": "1.2"}]),  # Get version
        MagicMock(json=lambda: []),  # existing files
        MagicMock(json=lambda: 4572),  # podman-darwin-amd64.gz (created)
        MagicMock(json=lambda: 4573),  # sha256778877.txt (created)
        MagicMock(json=lambda: 4574),  # konflux-darwin-amd64.gz (created)
    ]

    cgw_wrapper.main()
    assert temp_output_file.exists()
    final_result_path = temp_output_file.read_text(encoding="utf-8")
    assert final_result_path.endswith("result.json")
    with open(final_result_path) as f:
        result_json = json.load(f)
    assert result_json[0]["no_of_files_processed"] == 5
    assert result_json[0]["no_of_files_created"] == 5
    assert result_json[0]["no_of_files_skipped"] == 0
    assert result_json[1]["no_of_files_processed"] == 3
    assert result_json[1]["no_of_files_created"] == 3
    assert result_json[1]["no_of_files_skipped"] == 0


@patch("publish_to_cgw_wrapper.call_cgw_api")
@patch.dict(os.environ, {"CGW_USERNAME": "user", "CGW_PASSWORD": "password"})
@patch("sys.argv", new_callable=lambda: ["publish_to_cgw_wrapper.py"])
def test_main_skips_3_creates_2(
    mock_sys_argv, mock_call, temp_data_file, temp_output_file, content_dir, metadata
):
    """
    Test main() for two contentGateway components:
    - First component should skip 3 existing files and create 2 new files.
    - Second component should create 3 new files with no skips.
    """
    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_file",
        str(temp_data_file),
        "--content_dir",
        str(content_dir),
        "--output_file",
        str(temp_output_file),
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
        MagicMock(
            json=lambda: [
                {**metadata[0], "id": 4567},  # cosign-linux-amd64.gz (exists)
                {**metadata[1], "id": 4568},  # sha256778877.txt (exists)
                {**metadata[2], "id": 4569},  # cosign (exists)
            ]  # Get existing files
        ),
        MagicMock(json=lambda: 4570),  # sha256778877.txt (create)
        MagicMock(json=lambda: 4571),  # cosign-checksum.gpg (create)
        # For second component
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356068, "name": "product_name_2", "productCode": "product_code_2"}
            ]
        ),
        MagicMock(json=lambda: [{"id": 4156068, "versionName": "1.2"}]),  # Get version
        MagicMock(json=lambda: []),  # Get existing files
        MagicMock(json=lambda: 4572),  # podman-darwin-amd64.gz (create)
        MagicMock(json=lambda: 4573),  # sha256778877.txt (create)
        MagicMock(json=lambda: 4574),  # konflux-darwin-amd64.gz (create)
    ]

    cgw_wrapper.main()

    assert temp_output_file.exists()

    final_result_path = temp_output_file.read_text(encoding="utf-8")
    with open(final_result_path) as f:
        result_json = json.load(f)

    assert result_json[0]["no_of_files_processed"] == 5
    assert result_json[0]["no_of_files_created"] == 2
    assert result_json[0]["no_of_files_skipped"] == 3
    assert result_json[1]["no_of_files_processed"] == 3
    assert result_json[1]["no_of_files_created"] == 3
    assert result_json[1]["no_of_files_skipped"] == 0


@patch("publish_to_cgw_wrapper.call_cgw_api")
@patch.dict(os.environ, {"CGW_USERNAME": "user", "CGW_PASSWORD": "password"})
@patch("sys.argv", new_callable=lambda: ["publish_to_cgw_wrapper.py"])
def test_main_partial_skip_fail_rollback(
    mock_sys_argv,
    mock_call,
    temp_data_file,
    temp_output_file,
    content_dir,
):
    """
    Test main() with contentGateway components where:
    - The first component successfully creates 5 files.
    - The second component creates 1 file, then fails on the next file.
    - A failure in the 2 component triggers rollback of its created
      file and all files from the first component.
    """
    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_file",
        str(temp_data_file),
        "--content_dir",
        str(content_dir),
        "--output_file",
        str(temp_output_file),
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
        MagicMock(json=lambda: []),  # Get existing files
        MagicMock(json=lambda: 4571),  # cosign-linux-amd64.gz (created)
        MagicMock(json=lambda: 4572),  # sha256778877.txt (created)
        MagicMock(json=lambda: 4573),  # cosign (created)
        MagicMock(json=lambda: 4574),  # cosign-checksum.gpg (created)
        MagicMock(json=lambda: 4575),  # gitsign-darwin-amd64.gz (created)
        # Second component
        MagicMock(  # Get products
            json=lambda: [
                {"id": 356068, "name": "product_name_2", "productCode": "product_code_2"}
            ]
        ),  # Get products
        MagicMock(json=lambda: [{"id": 4156068, "versionName": "1.2"}]),  # Get version
        MagicMock(json=lambda: []),  # Get existing files
        MagicMock(json=lambda: 4576),  # podman-darwin-amd64.gz (created)
        RuntimeError("File creation failed in second component"),
        MagicMock(),  # delete file 4576
        MagicMock(),  # delete file 4571
        MagicMock(),  # delete file 4572
        MagicMock(),  # delete file 4573
        MagicMock(),  # delete file 4574
    ]

    with pytest.raises(SystemExit) as exc:
        cgw_wrapper.main()
    assert exc.value.code == 1

    expected_delete_calls = [
        # Second component file
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356068/versions/4156068/files/4576",
            session=ANY,
        ),
        # First component files
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
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4573",
            session=ANY,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4574",
            session=ANY,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/356067/versions/4156067/files/4575",
            session=ANY,
        ),
    ]
    mock_call.assert_has_calls(expected_delete_calls, any_order=True)
    assert mock_call.call_count == 19


@patch("publish_to_cgw_wrapper.call_cgw_api")
@patch.dict(os.environ, {"CGW_USERNAME": "user", "CGW_PASSWORD": "password"})
@patch("sys.argv", new_callable=lambda: ["publish_to_cgw_wrapper.py"])
def test_main_fails_when_components_missing(mock_sys_argv, temp_output_file, tmp_path, caplog):
    """Test that main() exits when contentGateway.components is missing or empty."""
    data_file_path = tmp_path / "data.json"
    invalid_data = {
        "mapping": {
            "components": [
                {
                    "contentGateway": {
                        "productName": "product_name_1",
                        "productCode": "product_code_1",
                        "productVersionName": "1.1",
                    }
                }
            ]
        }
    }
    data_file_path.write_text(json.dumps(invalid_data))

    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_file",
        str(data_file_path),
        "--content_dir",
        str(tmp_path),
        "--output_file",
        str(temp_output_file),
    ]
    mock_sys_argv.extend(test_args)

    with pytest.raises(SystemExit) as exc_info:
        cgw_wrapper.main()

    assert exc_info.value.code == 1
    assert "One or more missing `contentGateway.components' in data" in caplog.text
