"""Sample InternalRequest test data as JSON dicts."""

ir_publish_index_image_pipeline = {
    "apiVersion": "appstudio.redhat.com/v1alpha1",
    "kind": "InternalRequest",
    "metadata": {
        "creationTimestamp": "2026-05-19T13:45:44Z",
        "generateName": "publish-index-image-pipeline-",
        "generation": 1,
        "labels": {"internal-services.appstudio.openshift.io/pipelinerun-uid": "1"},
        "name": "publish-index-image-pipeline-ok",
        "namespace": "testing",
        "resourceVersion": "8890351156",
        "uid": "cd910aa9-e62a-4623-89c9-69f6bbe7f6fa",
    },
    "spec": {
        "params": {
            "sourceIndex": "quay.io/example/source-index:latest",
            "targetIndex": "quay.io/example/target-index:latest",
            "publishingCredentials": "encoded-credentials",
            "retries": "3",
            "taskGitUrl": "https://github.com/konflux-ci/release-service-catalog.git",
            "taskGitRevision": "development",
        },
        "pipeline": {
            "pipelineRef": {
                "params": [
                    {
                        "name": "url",
                        "value": "https://github.com/konflux-ci/release-service-catalog.git",
                    },
                    {"name": "revision", "value": "development"},
                    {
                        "name": "pathInRepo",
                        "value": "pipelines/internal/publish-index-image-pipeline/"
                        "publish-index-image-pipeline.yaml",
                    },
                ],
                "resolver": "git",
            }
        },
        "timeouts": {"finally": "0h5m0s", "pipeline": "1h0m0s", "tasks": "0h55m0s"},
    },
    "status": {
        "completionTime": "2026-05-19T13:58:59Z",
        "conditions": [
            {
                "lastTransitionTime": "2026-05-19T13:58:59Z",
                "message": "",
                "reason": "Succeeded",
                "status": "True",
                "type": "Succeeded",
            }
        ],
        "pipelineRun": "internal-services/internalrequest-qwgtd",
        "results": {"requestMessage": "success"},
        "startTime": "2026-05-19T13:45:44Z",
    },
}

ir_publish_index_image_pipeline_failed = {
    "apiVersion": "appstudio.redhat.com/v1alpha1",
    "kind": "InternalRequest",
    "metadata": {
        "creationTimestamp": "2026-05-19T13:45:44Z",
        "generateName": "publish-index-image-pipeline-",
        "generation": 1,
        "labels": {"internal-services.appstudio.openshift.io/pipelinerun-uid": "2"},
        "name": "publish-index-image-pipeline-failed",
        "namespace": "testing",
        "resourceVersion": "8890351156",
        "uid": "cd910aa9-e62a-4623-89c9-69f6bbe7f6fa",
    },
    "spec": {
        "params": {
            "sourceIndex": "quay.io/example/source-index:latest",
            "targetIndex": "quay.io/example/target-index:latest",
            "publishingCredentials": "encoded-credentials",
            "retries": "3",
            "taskGitUrl": "https://github.com/konflux-ci/release-service-catalog.git",
            "taskGitRevision": "development",
        },
        "pipeline": {
            "pipelineRef": {
                "params": [
                    {
                        "name": "url",
                        "value": "https://github.com/konflux-ci/release-service-catalog.git",
                    },
                    {"name": "revision", "value": "development"},
                    {
                        "name": "pathInRepo",
                        "value": "pipelines/internal/publish-index-image-pipeline/"
                        "publish-index-image-pipeline.yaml",
                    },
                ],
                "resolver": "git",
            }
        },
        "timeouts": {"finally": "0h5m0s", "pipeline": "1h0m0s", "tasks": "0h55m0s"},
    },
    "status": {
        "completionTime": "2026-05-19T13:58:59Z",
        "conditions": [
            {
                "lastTransitionTime": "2026-05-19T13:58:59Z",
                "message": "failed",
                "reason": "Rejected",
                "status": "False",
                "type": "Succeeded",
            }
        ],
        "pipelineRun": "internal-services/internalrequest-qwgtd",
        "results": {"requestMessage": "error"},
        "startTime": "2026-05-19T13:45:44Z",
    },
}

IRS = {
    ir_publish_index_image_pipeline["metadata"]["name"]: ir_publish_index_image_pipeline,
    ir_publish_index_image_pipeline_failed["metadata"][
        "name"
    ]: ir_publish_index_image_pipeline_failed,
}
