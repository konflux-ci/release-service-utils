"""Unit tests for InternalRequest Pydantic models."""

import json
import pytest
from rsmodels.internal_request_models import InternalRequest


@pytest.fixture
def sample_data():
    """Return the sample dictionary data for testing."""
    return {
        "apiVersion": "appstudio.redhat.com/v1alpha1",
        "kind": "InternalRequest",
        "metadata": {
            "creationTimestamp": "2026-05-22T11:57:31Z",
            "generateName": "publish-index-image-pipeline-",
            "generation": 1,
            "labels": {
                "internal-services.appstudio.openshift.io/pipelinerun-uid": (
                    "16b83d5c-0a57-4c64-b170-004f154bc037"
                )
            },
            "managedFields": [
                {
                    "apiVersion": "appstudio.redhat.com/v1alpha1",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": {
                        "f:metadata": {
                            "f:generateName": {},
                            "f:labels": {
                                ".": {},
                                "f:internal-services.appstudio.openshift.io/pipelinerun-uid": (
                                    {}
                                ),
                            },
                        },
                        "f:spec": {
                            ".": {},
                            "f:params": {
                                ".": {},
                                "f:publishingCredentials": {},
                                "f:retries": {},
                                "f:sourceIndex": {},
                                "f:targetIndex": {},
                                "f:taskGitRevision": {},
                                "f:taskGitUrl": {},
                            },
                            "f:pipeline": {
                                ".": {},
                                "f:pipelineRef": {".": {}, "f:params": {}, "f:resolver": {}},
                            },
                            "f:timeouts": {
                                ".": {},
                                "f:finally": {},
                                "f:pipeline": {},
                                "f:tasks": {},
                            },
                        },
                    },
                    "manager": "OpenAPI-Generator",
                    "operation": "Update",
                    "time": "2026-05-22T11:57:31Z",
                }
            ],
            "name": "publish-index-image-pipeline-c677w",
            "namespace": "default",
            "resourceVersion": "9438",
            "uid": "d1ec1110-3994-4fa4-8fb6-db781dc1a81b",
        },
        "spec": {
            "params": {
                "publishingCredentials": "test-credentials",
                "retries": "2",
                "sourceIndex": "redhat.com/rh-stage/iib:01",
                "targetIndex": "quay.io/scoheb/fbc-target-index-testing:v4.12",
                "taskGitRevision": "main",
                "taskGitUrl": "http://localhost",
            },
            "pipeline": {
                "pipelineRef": {
                    "params": [
                        {"name": "url", "value": "http://localhost"},
                        {"name": "revision", "value": "main"},
                        {
                            "name": "pathInRepo",
                            "value": (
                                "pipelines/internal/publish-index-image-pipeline/"
                                "publish-index-image-pipeline.yaml"
                            ),
                        },
                    ],
                    "resolver": "git",
                }
            },
            "timeouts": {
                "finally": "0h5m0s",
                "pipeline": "00h11m00s",
                "tasks": "00h06m00s",
            },
        },
    }


def test_internal_request_from_dict(sample_data):
    """Test loading InternalRequest using the from_dict method."""
    ir = InternalRequest.from_dict(sample_data)

    assert isinstance(ir, InternalRequest)
    assert ir.apiVersion == "appstudio.redhat.com/v1alpha1"
    assert ir.kind == "InternalRequest"

    # Verify metadata fields
    assert ir.metadata.name == "publish-index-image-pipeline-c677w"
    assert ir.metadata.namespace == "default"
    assert ir.metadata.generateName == "publish-index-image-pipeline-"
    assert ir.metadata.generation == 1
    assert (
        ir.metadata.labels["internal-services.appstudio.openshift.io/pipelinerun-uid"]
        == "16b83d5c-0a57-4c64-b170-004f154bc037"
    )

    # Verify spec fields
    assert ir.spec.params["publishingCredentials"] == "test-credentials"
    assert ir.spec.params["retries"] == "2"
    assert ir.spec.params["sourceIndex"] == "redhat.com/rh-stage/iib:01"
    assert ir.spec.params["targetIndex"] == "quay.io/scoheb/fbc-target-index-testing:v4.12"

    # Verify pipelineRef fields
    pipeline_ref = ir.spec.pipeline.pipelineRef
    assert pipeline_ref.resolver == "git"
    assert len(pipeline_ref.params) == 3
    assert pipeline_ref.params[0].name == "url"
    assert pipeline_ref.params[0].value == "http://localhost"

    # Verify timeout fields
    assert ir.spec.timeouts.finally_ == "0h5m0s"
    assert ir.spec.timeouts.pipeline == "00h11m00s"
    assert ir.spec.timeouts.tasks == "00h06m00s"


def test_internal_request_from_json(sample_data):
    """Test loading InternalRequest using the from_json method."""
    json_str = json.dumps(sample_data)
    ir = InternalRequest.from_json(json_str)

    assert isinstance(ir, InternalRequest)
    assert ir.apiVersion == "appstudio.redhat.com/v1alpha1"
    assert ir.kind == "InternalRequest"
    assert ir.metadata.name == "publish-index-image-pipeline-c677w"
    assert ir.spec.timeouts.pipeline == "00h11m00s"


def test_internal_request_from_file(sample_data, tmp_path):
    """Test loading InternalRequest from a file using the from_file method."""
    file_path = tmp_path / "sample_internal_request.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(sample_data, f)

    ir = InternalRequest.from_file(file_path)

    assert isinstance(ir, InternalRequest)
    assert ir.apiVersion == "appstudio.redhat.com/v1alpha1"
    assert ir.kind == "InternalRequest"
    assert ir.metadata.name == "publish-index-image-pipeline-c677w"
    assert ir.spec.timeouts.pipeline == "00h11m00s"
