from unittest.mock import patch, MagicMock

from find_signature import (
    find_signature_using_reference,
)

mock_pyxis_api = "https://catalog.redhat.com/api/containers/"
mock_pyxis_graphql_api = "https://graphql.redhat.com/api"

SIGNATURE_ID = "67033c8d76860bfe6a094ecf"
SIGNATURE_ID2 = "67033c8d76860bfe6a094ecg"


@patch("pyxis.get")
@patch("pyxis.graphql_query")
def test_signature_exists(graphql_query, get):
    # Arrange
    args = MagicMock()
    args.pyxis_api = mock_pyxis_api
    args.pyxis_graphql_api = mock_pyxis_graphql_api
    args.manifest_digest = "some_digest"
    args.reference = "quay.io/scoheb/a:latest"

    # signature exists
    signature = generate_signature(SIGNATURE_ID)
    graphql_query.return_value = generate_pyxis_response(
        "find_signature_data_by_index", signature
    )

    get.return_value.json.return_value = {
        "_id": SIGNATURE_ID,
        "reference": args.reference,
    }

    # Act
    found = find_signature_using_reference(
        args.pyxis_api, args.pyxis_graphql_api, args.reference, args.manifest_digest
    )
    assert found


@patch("pyxis.get")
@patch("pyxis.graphql_query")
def test_signature_notfound(graphql_query, get):
    # Arrange
    args = MagicMock()
    args.pyxis_api = mock_pyxis_api
    args.pyxis_graphql_api = mock_pyxis_graphql_api
    args.manifest_digest = "some_digest"
    args.reference = "quay.io/scoheb/a:latest"
    another_reference = "quay.io/scoheb/a:oldest"

    # signature does not exist
    signature1 = generate_signature(SIGNATURE_ID)
    graphql_query.side_effect = [
        generate_pyxis_response("find_signature_data_by_index", signature1),
        generate_pyxis_response("find_signature_data_by_index", []),
    ]

    get.return_value.json.return_value = {
        "_id": SIGNATURE_ID,
        "reference": another_reference,
    }

    # Act
    found = find_signature_using_reference(
        args.pyxis_api, args.pyxis_graphql_api, args.reference, args.manifest_digest
    )
    assert not found


def generate_pyxis_response(query_name, data):
    response_json = {
        query_name: {
            "data": data,
            "error": None,
        }
    }

    return response_json


def generate_signature(id):
    signatures = [
        {
            "_id": id,
        }
    ]
    return signatures
