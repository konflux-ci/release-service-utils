import hashlib
import pytest
import requests
from unittest.mock import MagicMock, patch, call
import publish_to_cgw_wrapper as cgw_wrapper


@pytest.fixture
def session():
    return requests.Session()


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
        "ignored",
        "sha256778877.txt",
    ]
    for filename in files:
        content_dir.join(filename).write("")
    return content_dir


@pytest.fixture
def data_file():
    return {
        "contentGateway": {
            "mirrorOpenshiftPush": True,
            "productName": "product_name_1",
            "productCode": "product_code_1",
            "productVersionName": "1.1",
            "components": [
                {
                    "name": "cosign",
                    "description": "Red Hat OpenShift Local Sandbox Test",
                    "shortURL": "/cgw/product_code_1/1.1",
                    "hidden": False,
                },
                {
                    "name": "gitsign",
                    "description": "Red Hat OpenShift Local Sandbox Test",
                    "shortURL": "/cgw/product_code_1/1.1",
                    "hidden": False,
                },
            ],
        }
    }


@pytest.fixture
def metadata():
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


def test_parse_args():
    """Test parsing command line arguments."""
    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_file",
        "./data.json",
        "--content_dir",
        "./content_dir",
        "--output_file",
        "./result.txt",
    ]
    with patch("sys.argv", ["publish_to_cgw_wrapper.py"] + test_args):
        args = cgw_wrapper.parse_args()
        assert args.cgw_host == "https://cgw.com/cgw/rest/admin"
        assert args.data_file == "./data.json"
        assert args.content_dir == "./content_dir"
        assert args.output_file == "./result.txt"


def test_parse_args_missing_required():
    """Test parsing command line arguments with missing required arguments."""
    test_args = []
    with patch("sys.argv", ["publish_to_cgw_wrapper.py"] + test_args):
        with pytest.raises(SystemExit) as excinfo:
            cgw_wrapper.parse_args()
        assert excinfo.value.code == 2


@patch("requests.Session.request")
def test_call_cgw_api_success(mock_request, session):
    """Test successful API call"""
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {"result": "success"}
    mock_request.return_value = mock_response

    response = cgw_wrapper.call_cgw_api(
        host="https://cgw.com/cgw/rest/admin",
        method="GET",
        endpoint="/endpoint",
        session=session,
    )

    assert mock_request.call_count == 1
    assert response.json() == {"result": "success"}


@patch("requests.Session.request")
def test_call_cgw_api_failure(mock_request, session):
    """Test API call failure when response is not OK."""
    mock_response = MagicMock()
    mock_response.ok = False
    mock_response.status_code = 401
    mock_response.text = "Unauthorized"
    mock_request.return_value = mock_response
    with pytest.raises(RuntimeError, match="API call failed: Unauthorized"):
        cgw_wrapper.call_cgw_api(
            host="https://cgw.com/cgw/rest/admin",
            method="GET",
            endpoint="/endpoint",
            session=session,
        )


@patch("requests.Session.request")
def test_call_cgw_api_exception(mock_request, session):
    """Test API call failure when an exception is raised."""
    mock_request.side_effect = requests.exceptions.RequestException("Connection error")
    with pytest.raises(RuntimeError, match="API call failed: Connection error"):
        cgw_wrapper.call_cgw_api(
            host="https://cgw.com/cgw/rest/admin",
            method="GET",
            endpoint="/endpoint",
            session=session,
        )


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_get_product_id(mock_call):
    """Test getting product ID."""
    mock_call.return_value.json.return_value = [
        {"name": "product_name_1", "productCode": "code1", "id": 5468},
        {"name": "product_name_2", "productCode": "code2", "id": 6789},
    ]
    product_id = cgw_wrapper.get_product_id(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        product_name="product_name_1",
        product_code="code1",
    )
    mock_call.assert_called_once_with(
        host="https://cgw.com/cgw/rest/admin", method="GET", endpoint="/products", session=None
    )
    assert product_id == 5468


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_get_product_id_not_found(mock_call):
    """Test getting product ID when product is not found."""
    mock_call.return_value.json.return_value = [
        {"name": "product_name_1", "productCode": "code1", "id": 5468},
        {"name": "product_name_2", "productCode": "code2", "id": 6789},
    ]
    with pytest.raises(
        ValueError, match="Product product_name_3 not found with product code code3"
    ):
        cgw_wrapper.get_product_id(
            host="https://cgw.com/cgw/rest/admin",
            session=None,
            product_name="product_name_3",
            product_code="code3",
        )
    mock_call.assert_called_once_with(
        host="https://cgw.com/cgw/rest/admin", method="GET", endpoint="/products", session=None
    )


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_get_version_id(mock_call):
    """Test getting version ID."""
    mock_call.return_value.json.return_value = [
        {"id": 4156067, "versionName": "1.1"},
        {"id": 4156068, "versionName": "1.2"},
    ]
    version_id = cgw_wrapper.get_version_id(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        product_id="4010426",
        version_name="1.2",
    )
    mock_call.assert_called_once_with(
        host="https://cgw.com/cgw/rest/admin",
        method="GET",
        endpoint="/products/4010426/versions",
        session=None,
    )
    assert version_id == 4156068


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_get_version_id_not_found(mock_call):
    """Test getting version ID when version is not found."""
    mock_call.return_value.json.return_value = [
        {"id": 4156067, "versionName": "1.1"},
        {"id": 4156068, "versionName": "1.2"},
    ]
    with pytest.raises(ValueError, match="Version not found: 1.3"):
        cgw_wrapper.get_version_id(
            host="https://cgw.com/cgw/rest/admin",
            session=None,
            product_id="4010426",
            version_name="1.3",
        )
    mock_call.assert_called_once_with(
        host="https://cgw.com/cgw/rest/admin",
        method="GET",
        endpoint="/products/4010426/versions",
        session=None,
    )


def test_generate_download_url(tmpdir):
    """Test generating download URL."""
    file_path = tmpdir.join("cosign-linux-amd64.gz")
    file_path.write(b"")
    expected_hash = hashlib.sha256(b"").hexdigest()
    expected_url = (
        f"/content/origin/files/sha256/{expected_hash[:2]}/{expected_hash}/"
        "cosign-linux-amd64.gz"
    )
    assert (
        cgw_wrapper.generate_download_url(str(tmpdir), "cosign-linux-amd64.gz") == expected_url
    )


def test_generate_metadata(content_dir, data_file, metadata):
    """Test generating metadata."""
    components = data_file["contentGateway"]["components"]
    output_metadata = cgw_wrapper.generate_metadata(
        content_dir=str(content_dir),
        components=components,
        product_Code="product_code_1",
        version_id=4156067,
        version_name="1.1",
        mirror_openshift_Push=True,
    )
    assert len(output_metadata) is len(metadata)
    for expected_file in metadata:
        assert expected_file in output_metadata


def test_file_already_exists(metadata):
    """Test checking if a file already exists."""
    new_file = metadata[0]
    assert cgw_wrapper.file_already_exists(metadata, new_file) is metadata[0]


def test_file_does_not_exist(metadata):
    """Test checking if a file does not exist."""
    new_file = metadata[0].copy()
    new_file["label"] = "nonexistent-file.gz"
    new_file["shortURL"] = "/pub/cgw/product_name_1/1.2/nonexistent-file.gz"
    new_file["downloadURL"] = "/content/origin/files/sha256/0d/somehash/nonexistent-file.gz"
    assert cgw_wrapper.file_already_exists(metadata, new_file) is None


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_rollback_files(mock_call):
    """Test rolling back created files."""
    response = cgw_wrapper.rollback_files(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        product_id="101",
        version_id="201",
        created_file_ids=[4567, 5678],
    )
    mock_call.assert_has_calls(
        [
            call(
                host="https://cgw.com/cgw/rest/admin",
                method="DELETE",
                endpoint="/products/101/versions/201/files/4567",
                session=None,
            ),
            call(
                host="https://cgw.com/cgw/rest/admin",
                method="DELETE",
                endpoint="/products/101/versions/201/files/5678",
                session=None,
            ),
        ]
    )
    assert mock_call.call_count == 2
    assert response is None


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_rollback_files_exception(mock_call):
    """Test rolling back created files when an exception is raised."""
    mock_call.side_effect = [None, RuntimeError("File can not be deleted")]

    with pytest.raises(RuntimeError, match="File can not be deleted"):
        cgw_wrapper.rollback_files(
            host="https://cgw.com/cgw/rest/admin",
            session=None,
            product_id="101",
            version_id="201",
            created_file_ids=[4567, 5678],
        )
    mock_call.assert_has_calls(
        [
            call(
                host="https://cgw.com/cgw/rest/admin",
                method="DELETE",
                endpoint="/products/101/versions/201/files/4567",
                session=None,
            ),
            call(
                host="https://cgw.com/cgw/rest/admin",
                method="DELETE",
                endpoint="/products/101/versions/201/files/5678",
                session=None,
            ),
        ]
    )
    assert mock_call.call_count == 2


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_create_files_success(mock_call, metadata):
    """Test successful file creation with no existing files."""
    mock_call.side_effect = [
        MagicMock(json=lambda: []),  # No existing files
        MagicMock(json=lambda: 4567),  # cosign-linux-amd64.gz (created)
        MagicMock(json=lambda: 4568),  # sha256778877.txt (created)
    ]
    metadata = metadata[:2]

    created, skipped = cgw_wrapper.create_files(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        product_id="101",
        version_id="201",
        metadata=metadata,
    )

    mock_call.assert_has_calls(
        [
            call(
                host="https://cgw.com/cgw/rest/admin",
                method="GET",
                endpoint="/products/101/versions/201/files",
                session=None,
            ),
            call(
                host="https://cgw.com/cgw/rest/admin",
                method="POST",
                endpoint="/products/101/versions/201/files",
                session=None,
                data=metadata[0],
            ),
            call(
                host="https://cgw.com/cgw/rest/admin",
                method="POST",
                endpoint="/products/101/versions/201/files",
                session=None,
                data=metadata[1],
            ),
        ]
    )

    assert mock_call.call_count == 3
    assert created == [4567, 4568]
    assert skipped == []


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_create_files_with_existing(mock_call, metadata):
    """Test file creation when some files already exist."""
    existing_files = [
        {**metadata[0], "id": 4566},  # cosign-linux-amd64.gz (exists)
        {**metadata[1], "id": 4567},  # sha256778877.txt (exists)
    ]

    mock_call.side_effect = [
        MagicMock(json=lambda: existing_files),
        MagicMock(json=lambda: 4568),  # cosign (created)
        MagicMock(json=lambda: 4569),  # cosign-checksum.gpg (created)
        MagicMock(json=lambda: 4570),  # gitsign-darwin-amd64.gz (created)
    ]

    created, skipped = cgw_wrapper.create_files(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        product_id="101",
        version_id="202",
        metadata=metadata,
    )

    expected_calls = [
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="GET",
            endpoint="/products/101/versions/202/files",
            session=None,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="POST",
            endpoint="/products/101/versions/202/files",
            session=None,
            data=metadata[2],
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="POST",
            endpoint="/products/101/versions/202/files",
            session=None,
            data=metadata[3],
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="POST",
            endpoint="/products/101/versions/202/files",
            session=None,
            data=metadata[4],
        ),
    ]

    mock_call.assert_has_calls(expected_calls)
    assert mock_call.call_count == 4
    assert created == [4568, 4569, 4570]
    assert skipped == [4566, 4567]


@patch("publish_to_cgw_wrapper.call_cgw_api")
def test_create_files_exception(mock_call, metadata):
    """Test file creation when an exception is raised and check rollback is successful."""
    mock_call.side_effect = [
        MagicMock(json=lambda: []),
        MagicMock(json=lambda: 4567),
        RuntimeError("File can not be created"),
        None,
    ]

    with pytest.raises(RuntimeError, match="File can not be created"):
        cgw_wrapper.create_files(
            host="https://cgw.com/cgw/rest/admin",
            session=None,
            product_id="101",
            version_id="201",
            metadata=metadata[:2],
        )

    expected_calls = [
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="GET",
            endpoint="/products/101/versions/201/files",
            session=None,
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="POST",
            endpoint="/products/101/versions/201/files",
            session=None,
            data=metadata[0],
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="POST",
            endpoint="/products/101/versions/201/files",
            session=None,
            data=metadata[1],
        ),
        call(
            host="https://cgw.com/cgw/rest/admin",
            method="DELETE",
            endpoint="/products/101/versions/201/files/4567",
            session=None,
        ),
    ]

    mock_call.assert_has_calls(expected_calls, any_order=False)
    assert mock_call.call_count == 4
