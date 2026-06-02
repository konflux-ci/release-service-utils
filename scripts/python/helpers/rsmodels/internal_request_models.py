"""Pydantic models for Kubernetes InternalRequest custom resource.

Based on:
- https://github.com/konflux-ci/internal-services
        /blob/main/api/v1alpha1/internalrequest_types.go
- https://github.com/konflux-ci/internal-services
        /blob/main/tekton/utils/pipeline.go

Note: Requires pydantic to be installed: pip install pydantic
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class Param(BaseModel):
    """Tekton parameter with name and value."""

    name: str = Field(..., description="Name of the parameter")
    value: str = Field(..., description="Value of the parameter")


class PipelineRef(BaseModel):
    """Reference to a Pipeline using a Tekton resolver."""

    resolver: str = Field(..., description="Name of a Tekton resolver (e.g., git)")
    params: List[Param] = Field(..., description="Parameters for the resolver")


class TimeoutFields(BaseModel):
    """Tekton timeout fields for PipelineRun execution."""

    pipeline: Optional[str] = Field(
        None,
        description="Total timeout for the PipelineRun (e.g., '1h0m0s')",
        pattern=r"^\d+h\d+m\d+s$",
    )
    tasks: Optional[str] = Field(
        None,
        description="Timeout for tasks in the PipelineRun (e.g., '0h55m0s')",
        pattern=r"^\d+h\d+m\d+s$",
    )
    finally_: Optional[str] = Field(
        None,
        alias="finally",
        description="Timeout for finally tasks (e.g., '0h5m0s')",
        pattern=r"^\d+h\d+m\d+s$",
    )


class Pipeline(BaseModel):
    """Pipeline reference with service account and timeouts."""

    pipelineRef: PipelineRef = Field(..., description="Reference to the Pipeline")
    serviceAccountName: Optional[str] = Field(
        None,
        description="ServiceAccount to use during Pipeline execution",
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
    )
    timeouts: Optional[TimeoutFields] = Field(
        None, description="Timeout definitions for PipelineRun"
    )


class ParameterizedPipeline(BaseModel):
    """Extension of Pipeline with additional parameters."""

    pipelineRef: PipelineRef = Field(..., description="Reference to the Pipeline")
    serviceAccountName: Optional[str] = Field(
        None,
        description="ServiceAccount to use during Pipeline execution",
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
    )
    timeouts: Optional[TimeoutFields] = Field(
        None, description="Timeout definitions for PipelineRun"
    )
    params: Optional[List[Param]] = Field(
        None, description="Additional parameters for the Pipeline"
    )


class TypeMeta(BaseModel):
    """Kubernetes TypeMeta for apiVersion and kind."""

    apiVersion: Optional[str] = Field(None, description="API version")
    kind: Optional[str] = Field(None, description="Resource kind")


class ObjectMeta(BaseModel):
    """Kubernetes ObjectMeta for resource metadata."""

    name: Optional[str] = Field(None, description="Name of the resource")
    generateName: Optional[str] = Field(None, description="Prefix for generated name")
    namespace: Optional[str] = Field(None, description="Namespace of the resource")
    labels: Optional[Dict[str, str]] = Field(None, description="Labels")
    annotations: Optional[Dict[str, str]] = Field(None, description="Annotations")
    uid: Optional[str] = Field(None, description="Unique ID")
    resourceVersion: Optional[str] = Field(None, description="Resource version")
    generation: Optional[int] = Field(None, description="Generation number")
    creationTimestamp: Optional[datetime] = Field(None, description="Creation timestamp")
    deletionTimestamp: Optional[datetime] = Field(None, description="Deletion timestamp")
    finalizers: Optional[List[str]] = Field(None, description="Finalizers")
    ownerReferences: Optional[List[Dict[str, Any]]] = Field(
        None, description="Owner references"
    )


class Condition(BaseModel):
    """Kubernetes Condition type."""

    type: str = Field(..., description="Type of condition")
    status: str = Field(..., description="Status of the condition (True, False, Unknown)")
    observedGeneration: Optional[int] = Field(
        None, description="Generation of the resource when this condition was updated"
    )
    lastTransitionTime: Optional[datetime] = Field(
        None, description="Last time the condition transitioned"
    )
    reason: Optional[str] = Field(None, description="Reason for the condition")
    message: Optional[str] = Field(None, description="Human-readable message")


class InternalRequestSpec(BaseModel):
    """Specification for InternalRequest resource."""

    pipeline: ParameterizedPipeline = Field(
        ..., description="Details of the pipeline to execute"
    )
    params: Optional[Dict[str, str]] = Field(
        None, description="Parameters to pass to Tekton pipeline"
    )
    timeouts: Optional[TimeoutFields] = Field(
        None, description="Timeout definitions for PipelineRun"
    )
    serviceAccount: Optional[str] = Field(
        None,
        description="ServiceAccount for PipelineRun execution",
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
    )


class InternalRequestStatus(BaseModel):
    """Status for InternalRequest resource."""

    startTime: Optional[datetime] = Field(
        None, description="Time when PipelineRun was created"
    )
    completionTime: Optional[datetime] = Field(
        None, description="Time when PipelineRun completed"
    )
    pipelineRun: Optional[str] = Field(
        None,
        description="Namespaced name of executed PipelineRun",
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?\/[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
    )
    conditions: Optional[List[Condition]] = Field(
        None, description="Latest observations for InternalRequest"
    )
    results: Optional[Dict[str, str]] = Field(
        None, description="Results from Tekton PipelineRun"
    )


class InternalRequest(BaseModel):
    """Kubernetes InternalRequest custom resource."""

    apiVersion: str = Field(default="appstudio.redhat.com/v1alpha1", description="API version")
    kind: str = Field(default="InternalRequest", description="Resource kind")
    metadata: ObjectMeta = Field(..., description="Resource metadata")
    spec: InternalRequestSpec = Field(..., description="InternalRequest specification")
    status: Optional[InternalRequestStatus] = Field(None, description="InternalRequest status")

    @field_validator("apiVersion")
    @classmethod
    def validate_api_version(cls, v: str) -> str:
        """Validate apiVersion is set correctly."""
        if v != "appstudio.redhat.com/v1alpha1":
            raise ValueError(
                "apiVersion must be 'appstudio.redhat.com/v1alpha1' for InternalRequest"
            )
        return v

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, v: str) -> str:
        """Validate kind is set correctly."""
        if v != "InternalRequest":
            raise ValueError("kind must be 'InternalRequest'")
        return v

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True
        json_schema_extra = {
            "example": {
                "apiVersion": "appstudio.redhat.com/v1alpha1",
                "kind": "InternalRequest",
                "metadata": {
                    "generateName": "my-pipeline-",
                    "namespace": "default",
                },
                "spec": {
                    "pipeline": {
                        "pipelineRef": {
                            "resolver": "git",
                            "params": [
                                {"name": "url", "value": "https://github.com/example/repo"},
                                {"name": "revision", "value": "main"},
                                {
                                    "name": "pathInRepo",
                                    "value": "pipelines/internal/my-pipeline/my-pipeline.yaml",
                                },
                            ],
                        }
                    },
                    "params": {"key1": "value1", "key2": "value2"},
                    "timeouts": {
                        "pipeline": "1h0m0s",
                        "tasks": "0h55m0s",
                        "finally": "0h5m0s",
                    },
                },
            }
        }

    @classmethod
    def from_json(cls, json_str: str) -> "InternalRequest":
        """Load InternalRequest from a JSON string.

        Args:
            json_str: JSON string representation of an InternalRequest

        Returns:
            InternalRequest instance

        Example:
            >>> json_data = '{"apiVersion": "appstudio.redhat.com/v1alpha1", ...}'
            >>> ir = InternalRequest.from_json(json_data)

        """
        data = json.loads(json_str)
        return cls.model_validate(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InternalRequest":
        """Load InternalRequest from a dictionary.

        Args:
            data: Dictionary representation of an InternalRequest

        Returns:
            InternalRequest instance

        Example:
            >>> data = {"apiVersion": "appstudio.redhat.com/v1alpha1", ...}
            >>> ir = InternalRequest.from_dict(data)

        """
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, file_path: Union[str, Path]) -> "InternalRequest":
        """Load InternalRequest from a JSON file.

        Args:
            file_path: Path to JSON file containing an InternalRequest

        Returns:
            InternalRequest instance

        Example:
            >>> ir = InternalRequest.from_file("ir.json")

        """
        path = Path(file_path)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.model_validate(data)


class InternalRequestList(BaseModel):
    """List of InternalRequest resources."""

    apiVersion: str = Field(default="appstudio.redhat.com/v1alpha1", description="API version")
    kind: str = Field(default="InternalRequestList", description="Resource kind")
    metadata: Optional[Dict[str, Any]] = Field(None, description="List metadata")
    items: List[InternalRequest] = Field(..., description="List of InternalRequests")
