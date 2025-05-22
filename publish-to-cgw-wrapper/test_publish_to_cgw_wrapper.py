import hashlib
import pytest
import requests
import json
from unittest.mock import MagicMock, patch, call
import publish_to_cgw_wrapper as cgw_wrapper


@pytest.fixture
def session():
    return requests.Session()


@pytest.fixture
def cosign_content_dir(tmpdir):
    content_dir = tmpdir.mkdir("content_dir_1")
    files = [
        "cosign",
        "cosign-linux-amd64.gz",
        "cosign-darwin-amd64.gz",
        "fake-name-linux-amd64.gz",
        "ignored",
        "cosign-checksum.gpg",
        "sha256778877.txt",
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
        "sha256778877.txt",
        "checksum.sig",
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
                    {"filename": "cosign", "arch": "amd64", "os": "linux"},
                    {"filename": "cosign-linux-amd64.gz", "arch": "amd64", "os": "linux"},
                    {"filename": "cosign-darwin-amd64.gz", "arch": "amd64", "os": "darwin"},
                ],
                "contentGateway": {
                    "productName": "product_name_1",
                    "productCode": "product_code_1",
                    "productVersionName": "1.1",
                    "mirrorOpenshiftPush": True,
                    "contentDir": str(cosign_content_dir),
                },
            },
            {
                "containerImage": "quay.io/org/tenant/gitsign@sha256:abcdef12345",
                "name": "gitsign",
                "files": [
                    {"filename": "gitsign", "arch": "amd64", "os": "linux"},
                    {"filename": "gitsign-linux-amd64.gz", "arch": "amd64", "os": "linux"},
                    {"filename": "gitsign-darwin-amd64.gz", "arch": "amd64", "os": "darwin"},
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
    return [
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "shortURL": "/pub/cgw/product_code_1/1.1/cosign-darwin-amd64.gz",
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427"
                "ae41e4649b934ca495991b7852b855/cosign-darwin-amd64.gz"
            ),
            "label": "cosign-darwin-amd64.gz",
        },
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "shortURL": "/pub/cgw/product_code_1/1.1/cosign-linux-amd64.gz",
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149"
                "afbf4c8996fb92427ae41e4649b934ca495991b7852b855/cosign-linux-amd64.gz"
            ),
            "label": "cosign-linux-amd64.gz",
        },
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149"
                "afbf4c8996fb92427ae41e4649b934ca495991b7852b855/sha256778877.txt"
            ),
            "shortURL": "/pub/cgw/product_code_1/1.1/sha256778877.txt",
            "label": "Checksum",
        },
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "shortURL": "/pub/cgw/product_code_1/1.1/cosign",
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149"
                "afbf4c8996fb92427ae41e4649b934ca495991b7852b855/cosign"
            ),
            "label": "cosign",
        },
        {
            "type": "FILE",
            "hidden": False,
            "invisible": False,
            "productVersionId": 4156067,
            "downloadURL": (
                "/content/origin/files/sha256/e3/e3b0c44298fc1c149afbf4c8996fb92427"
                "ae41e4649b934ca495991b7852b855/cosign-checksum.gpg"
            ),
            "shortURL": "/pub/cgw/product_code_1/1.1/cosign-checksum.gpg",
            "label": "Checksum - GPG",
        },
    ]


def test_load_data_valid(data_json):
    """Test load_data with valid JSON string."""
    result = cgw_wrapper.load_data(json.dumps(data_json))
    assert result == data_json


def test_load_data_invalid():
    """Test load_data raises ValueError on invalid JSON input."""
    with pytest.raises(ValueError, match="Invalid 'data_json' must be a valid JSON string"):
        cgw_wrapper.load_data("/some/path/to/data.json")


def test_validate_components_success(data_json):
    """Test validate_components with valid components"""
    valid_components = cgw_wrapper.validate_components(data_json)
    assert len(valid_components) == 2


def test_validate_components_skips_missing_contentGateway():
    """Test validate_components skips components with no contentGateway and does not raise."""
    data = {"components": [{"name": "missing-contentGateway", "files": [{"filename": "foo"}]}]}

    valid_components = cgw_wrapper.validate_components(data)
    assert len(valid_components) == 0


def test_validate_components_missing_fields():
    """Test validate_components raises error for missing required fields
    in contentGateway and files."""
    invalid_data = {
        "components": [
            {
                "name": "",
                "files": [{"filename": ""}],
                "contentGateway": {
                    "productName": "",
                    "productCode": "",
                    "productVersionName": "1.1",
                    "contentDir": "",
                },
            }
        ]
    }

    with pytest.raises(ValueError) as excinfo:
        cgw_wrapper.validate_components(invalid_data)

    error_msg = str(excinfo.value)
    assert "Component 1 is missing 'name'" in error_msg
    assert "Component 1 is missing 'productName'" in error_msg
    assert "Component 1 is missing 'productCode'" in error_msg
    assert "Component 1 is missing 'contentDir'" in error_msg
    assert "Component 1, file 0 is missing or has empty 'filename'" in error_msg


def test_parse_args(data_json):
    """Test parsing command line arguments."""
    test_args = [
        "--cgw_host",
        "https://cgw.com/cgw/rest/admin",
        "--data_json",
        json.dumps(data_json),
    ]
    with patch("sys.argv", ["publish_to_cgw_wrapper.py"] + test_args):
        args = cgw_wrapper.parse_args()
        assert args.cgw_host == "https://cgw.com/cgw/rest/admin"
        assert args.data_json == json.dumps(data_json)


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


def test_generate_metadata(data_json, metadata):
    """Test generating metadata."""
    content_dir = data_json["components"][0]["contentGateway"]["contentDir"]
    component_name = data_json["components"][0]["name"]
    files = data_json["components"][0]["files"]

    output_metadata = cgw_wrapper.generate_metadata(
        content_dir=content_dir,
        component_name=component_name,
        files=files,
        product_code="product_code_1",
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


@patch("publish_to_cgw_wrapper.create_files")
@patch("publish_to_cgw_wrapper.generate_metadata")
@patch("publish_to_cgw_wrapper.get_version_id")
@patch("publish_to_cgw_wrapper.get_product_id")
def test_process_component_success(
    mock_get_product,
    mock_get_version,
    mock_generate_meta,
    mock_create_files,
    data_json,
    metadata,
):
    """Test successful process_component for a component."""
    mock_get_product.return_value = 123
    mock_get_version.return_value = 456
    mock_generate_meta.return_value = metadata
    mock_create_files.return_value = ([7, 8, 9], [10, 11])

    component = data_json["components"][0]
    component_name = data_json["components"][0]["name"]
    content_dir = data_json["components"][0]["contentGateway"]["contentDir"]
    files = data_json["components"][0]["files"]

    result = cgw_wrapper.process_component(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        component=component,
    )

    mock_get_product.assert_called_once_with(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        product_name="product_name_1",
        product_code="product_code_1",
    )
    mock_get_version.assert_called_once_with(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        product_id=123,
        version_name="1.1",
    )
    mock_generate_meta.assert_called_once_with(
        content_dir=content_dir,
        component_name=component_name,
        files=files,
        product_code="product_code_1",
        version_id=456,
        version_name="1.1",
        mirror_openshift_Push=True,
    )
    mock_create_files.assert_called_once_with(
        host="https://cgw.com/cgw/rest/admin",
        session=None,
        product_id=123,
        version_id=456,
        metadata=metadata,
    )

    assert result["product_id"] == 123
    assert result["product_version_id"] == 456
    assert result["created_file_ids"] == [7, 8, 9]
    assert result["no_of_files_processed"] == len(metadata)
    assert result["no_of_files_created"] == 3
    assert result["no_of_files_skipped"] == 2
    assert result["metadata"] == metadata
