from unittest.mock import patch, MagicMock

from find_signatures import (
    find_signatures_for_repository,
)

mock_pyxis_graphql_api = "https://graphql.redhat.com/api"

SIGNATURE_ID = "67033c8d76860bfe6a094ecf"
SIGNATURE_ID2 = "67033c8d76860bfe6a094ecg"


@patch("pyxis.graphql_query")
def test_signatures_exist(graphql_query):
    # Arrange
    args = MagicMock()
    args.pyxis_graphql_api = mock_pyxis_graphql_api
    args.manifest_digest = "some_digest"
    args.repository = "scoheb/a"

    reference1 = "quay.io/scoheb/a:abc"
    reference2 = "quay.io/scoheb/a:def"

    # signature exists
    signatures1 = generate_signatures(SIGNATURE_ID)
    signatures2 = generate_signatures(SIGNATURE_ID2)
    signature1 = generate_signature(SIGNATURE_ID, reference1)
    signature2 = generate_signature(SIGNATURE_ID2, reference2)
    graphql_query.side_effect = [
        generate_pyxis_response("find_signature_data_by_index", signatures1),
        generate_pyxis_response("get_signature", signature1),
        generate_pyxis_response("find_signature_data_by_index", signatures2),
        generate_pyxis_response("get_signature", signature2),
        generate_pyxis_response("find_signature_data_by_index", []),
    ]

    # Act
    references = find_signatures_for_repository(
        args.pyxis_graphql_api, args.repository, args.manifest_digest
    )
    assert references
    assert len(references) == 2


@patch("pyxis.graphql_query")
def test_signatures_notfound(graphql_query):
    # Arrange
    args = MagicMock()
    args.pyxis_graphql_api = mock_pyxis_graphql_api
    args.manifest_digest = "some_digest"
    args.repository = "scoheb/a"

    # signatures do not exist
    graphql_query.side_effect = [
        generate_pyxis_response("find_signature_data_by_index", []),
    ]

    # Act
    references = find_signatures_for_repository(
        args.pyxis_graphql_api, args.repository, args.manifest_digest
    )
    assert len(references) == 0


def generate_pyxis_response(query_name, data):
    response_json = {
        query_name: {
            "data": data,
            "error": None,
        }
    }

    return response_json


def generate_signatures(id):
    signatures = [
        {
            "_id": id,
        }
    ]
    return signatures


def generate_signature(id, reference):
    signature = {
        "_id": id,
        "reference": reference,
    }
    return signature
