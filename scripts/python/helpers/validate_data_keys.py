"""Pydantic models for validating release data per pipeline profile.

Each pipeline defines which top level systems it needs.
A profile class lists those systems as required fields.

To add a new pipeline:
  1. Add any missing system models below.
  2. Create a profile class with the required systems.
  3. Register it in the PROFILES dict.

To make an optional field required for a specific pipeline use _strict()
which creates a subclass with the field overridden as required.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    create_model,
    model_validator,
)
from pydantic_core import PydanticCustomError


# Every model allows unknown keys so each profile only validates what it
# declares.  populate_by_name lets models with aliases also accept the
# Python field name.
class _Base(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


# Reusable constrained types.

CveKey = Annotated[
    str,
    StringConstraints(pattern=r"^CVE-[0-9]{4}-[0-9]{4,}$"),
    Field(examples=["CVE-2025-12345"]),
]

PyxisServer = Literal["stage", "production", "production-internal", "stage-internal"]
AtlasServer = Literal["stage", "production"]
AdvisoryType = Literal["RHEA", "RHBA", "RHSA"]
CdnEnv = Literal["qa", "stage", "production"]
Intention = Literal["production", "staging"]


# mapping models
# All sub fields are optional here


class Repository(_Base):
    url: str | None = None
    tags: list[str] | None = None


class StagedFile(_Base):
    filename: str | None = None
    source: str | None = None


class Staged(_Base):
    destination: str | None = None
    version: str | None = None
    files: list[StagedFile] | None = None


class ComponentFile(_Base):
    source: str | None = None
    arch: str | None = None
    os_field: str | None = Field(default=None, alias="os")


class ContentGateway(_Base):
    productName: str | None = None
    productCode: str | None = None
    productVersionName: str | None = None
    filePrefix: str | None = None
    mirrorOpenshiftPush: bool | None = None
    contentType: Literal["disk-image", "binary", "generic"] | None = None
    sign: bool | None = None


class ProductInfo(_Base):
    productName: str | None = None
    productCode: str | None = None
    productVersionName: str | None = None


class StarmapEntry(_Base):
    name: str | None = None
    workflow: str | None = None
    cloud: str | None = None
    mappings: dict[str, Any] | None = None
    billing_code_config: dict[str, Any] | None = Field(
        default=None,
        alias="billing-code-config",
    )


class RpmRepository(_Base):
    name: str | None = None
    arch: str | None = None
    repository_id: str | None = None
    repository_name: str | None = None
    distro: str | None = None


class BaseComponent(_Base):
    """Component fields shared by all pipelines."""

    name: str
    canonicalName: str | None = None
    repositories: list[Repository] | None = None
    tags: list[str] | None = None
    componentTags: list[str] | None = None
    staged: Staged | None = None
    files: list[ComponentFile] | None = None
    contentGateway: ContentGateway | None = None
    productInfo: ProductInfo | None = None
    starmap: list[StarmapEntry] | None = None
    pushSourceContainer: bool | None = None
    public: bool | None = None
    contentType: str | None = None


class MappingComponent(BaseComponent):
    """Component for the mapping system. Requires repositories or contentType
    so the pipeline knows where to push."""

    # mode="after" means this only runs when all fields pass.
    # If name is missing this validator is skipped.
    @model_validator(mode="after")
    def check_repositories_or_content_type(self) -> MappingComponent:
        if not self.repositories and self.contentType is None:
            raise PydanticCustomError(
                "missing_target",
                "each component needs 'repositories' (where to push) "
                "or 'contentType' (for non-image artifacts)",
            )
        return self


class MappingDefaults(_Base):
    tags: list[str] | None = None
    pushSourceContainer: bool | None = None
    public: bool | None = None
    contentGateway: ContentGateway | None = None


class Mapping(_Base):
    components: list[MappingComponent] = Field(min_length=1)
    defaults: MappingDefaults | None = None
    registrySecret: str | None = None
    cloudMarketplacesSecret: str | None = None
    cloudMarketplacesPrePush: bool | None = None
    rpm_repositories: list[RpmRepository] | None = Field(
        default=None,
        alias="rpm-repositories",
    )


# releaseNotes models


class ReleaseNotesImage(_Base):
    containerImage: str | None = None
    repository: str | None = None
    tags: list[str] | None = None
    component: str | None = None
    architecture: str | None = None
    signingKey: str | None = None
    purl: str | None = None


class ReleaseNotesArtifact(_Base):
    component: str | None = None
    architecture: str | None = None
    os: str | None = None
    purl: str | None = None
    sbom: str | None = None
    attestation: str | None = None


class ReleaseNotesContent(_Base):
    # An advisory must ship either container images or artifacts.
    images: list[ReleaseNotesImage] | None = None
    artifacts: list[ReleaseNotesArtifact] | None = None

    @model_validator(mode="after")
    def check_images_or_artifacts(self) -> ReleaseNotesContent:
        if not self.images and not self.artifacts:
            raise PydanticCustomError(
                "missing_content",
                "content needs 'images' (container images) " "or 'artifacts' (RPMs, binaries)",
            )
        return self


class FixedIssue(_Base):
    id: str | None = None
    source: str | None = None


class Issues(_Base):
    fixed: list[FixedIssue] | None = None


class Cve(_Base):
    # A CVE entry must have both key and component or neither.
    key: CveKey | None = None
    component: str | None = None
    packages: list[str] | None = None

    @model_validator(mode="after")
    def check_key_and_component_together(self) -> Cve:
        """A CVE is either fully filled in or completely empty."""

        # No fields set nothing to validate
        any_field_set = (
            self.key is not None
            or self.component is not None
            or self.packages is not None
        )
        if not any_field_set:
            return self

        # Something is set check key and component are both present
        missing = []
        if self.key is None:
            missing.append("key")
        if self.component is None:
            missing.append("component")

        if missing:
            raise PydanticCustomError(
                "missing_cve_fields",
                "{fields} required when any CVE field is set",
                {"fields": ", ".join(missing)},
            )
        return self


class ReleaseNotes(_Base):
    product_id: list[int] = Field(min_length=1, examples=[[123]])
    product_name: str = Field(examples=["Red Hat OpenStack Platform"])
    product_version: str = Field(examples=["17.1"])
    product_stream: str = Field(examples=["rhtas-tp1"])
    cpe: str = Field(examples=["cpe:/a:redhat:openstack:17::el9"])
    synopsis: str
    topic: str
    description: str
    solution: str
    content: ReleaseNotesContent
    type: AdvisoryType | None = None
    references: list[str] | None = None
    cves: list[Cve] | None = None
    issues: Issues | None = None
    live_id: int | None = None
    allow_custom_live_id: bool | None = None
    skip_customer_notifications: bool | None = None


# Other system models


class Sign(_Base):
    configMapName: str = Field(examples=["hacbs-signing-pipeline-config"])
    cosignSecretName: str | None = None
    request: str | None = None
    pipelineImage: str | None = None


class Pyxis(_Base):
    server: PyxisServer
    secret: str = Field(examples=["pyxis-cert"])
    skipRepoPublishing: bool | None = None
    includeLayers: bool | None = None


class Atlas(_Base):
    server: AtlasServer
    atlas_sso_secret_name: str | None = Field(
        default=None,
        alias="atlas-sso-secret-name",
    )
    atlas_retry_aws_secret_name: str | None = Field(
        default=None,
        alias="atlas-retry-aws-secret-name",
    )


class Cdn(_Base):
    env: CdnEnv


# pipeline profiles
# Each profile lists the systems that pipeline requires.  Required
# fields have no default optional systems use "| None = None".
# When a pipeline needs required validation on a nested model use
# _strict() to override optional fields as required.


def _strict(base: type[BaseModel], **overrides: Any) -> type[BaseModel]:
    """Return a copy of base with the given fields made required."""
    return create_model(
        f"Strict{base.__name__}",
        __base__=base,
        **overrides,
    )


# The contentGateway system requires each component to include
# contentGateway with productName, productCode, productVersionName
# and contentType all required.
_StrictCG = _strict(
    ContentGateway,
    productName=(str, ...),
    productCode=(str, ...),
    productVersionName=(str, ...),
    contentType=(Literal["disk-image", "binary", "generic"], ...),
)
_CGComponent = _strict(BaseComponent, contentGateway=(_StrictCG, ...))
_CGMapping = _strict(
    Mapping,
    components=(list[_CGComponent], Field(min_length=1)),
)


class MappingDataKeys(_Base):
    """push-to-external-registry: only needs mapping."""

    mapping: Mapping


class RhAdvisoriesDataKeys(_Base):
    """rh-advisories: advisory with signing, pyxis and atlas."""

    releaseNotes: ReleaseNotes
    pyxis: Pyxis
    mapping: Mapping
    sign: Sign
    atlas: Atlas


class PushArtifactsToCdnDataKeys(_Base):
    """push-artifacts-to-cdn: CDN push with content gateway on every component."""

    cdn: Cdn
    # type checker flags this because _CGMapping is built by create_model
    # at runtime and is resloved as a class.
    mapping: _CGMapping  # type: ignore[valid-type]
    intention: Intention
    releaseNotes: ReleaseNotes


PROFILES: dict[str, type[BaseModel]] = {
    "push-to-external-registry": MappingDataKeys,
    "rh-advisories": RhAdvisoriesDataKeys,
    "push-artifacts-to-cdn": PushArtifactsToCdnDataKeys,
}


def validate(data: dict[str, Any], profile: str) -> BaseModel:
    """Validate data against the Pydantic model registered for profile.

    Raises pydantic.ValidationError when the data does not match.
    Raises ValueError when the profile name is not registered.
    """
    model_cls = PROFILES.get(profile)
    if model_cls is None:
        known = ", ".join(sorted(PROFILES))
        raise ValueError(f"Unknown profile {profile!r}. Known profiles: {known}")
    return model_cls.model_validate(data)


def format_errors(exc: ValidationError) -> str:
    """Turn a ValidationError into clean text for the user."""
 
    errors = exc.errors(include_url=False, include_input=False)
    lines = []
    for error in errors:
        path = ".".join(str(part) for part in error["loc"])
        lines.append(f"  {path}: {error['msg']}")
    return "\n".join(lines)
