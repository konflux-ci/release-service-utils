from unittest.mock import patch, MagicMock

from find_signatures import (
    find_signatures_for_repository,
)

mock_pyxis_graphql_api = "https://graphql.redhat.com/api"

SIGNATURE_ID = "67033c8d76860bfe6a094ecf"


@patch("pyxis.graphql_query")
def test_signatures_exist(graphql_query):
    # Arrange
    args = MagicMock()
    args.pyxis_graphql_api = mock_pyxis_graphql_api
    args.manifest_digest = "some_digest"
    args.repository = "scoheb/a"

    reference1 = "quay.io/scoheb/a:abc"

    # signature exists
    signatures1 = generate_signatures(SIGNATURE_ID, reference1)
    graphql_query.side_effect = [
        generate_pyxis_response("find_signatures", signatures1),
    ]

    # Act
    references = find_signatures_for_repository(
        args.pyxis_graphql_api, args.repository, args.manifest_digest
    )
    assert references
    assert len(references) == 1


@patch("pyxis.graphql_query")
def test_signatures_notfound(graphql_query):
    # Arrange
    args = MagicMock()
    args.pyxis_graphql_api = mock_pyxis_graphql_api
    args.manifest_digest = "some_digest"
    args.repository = "scoheb/a"

    # signatures do not exist
    graphql_query.side_effect = [
        generate_pyxis_response("find_signatures", []),
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


def generate_signatures(id, reference):
    signatures = [
        {
            "_id": id,
            "reference": reference,
        }
    ]
    return signatures
