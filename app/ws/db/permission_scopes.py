import datetime
import enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel, to_pascal

from app.ws.db.types import CurationRequest, StudyCategory, StudyStatus, UserRole


class BaseScopeModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        json_schema_serialization_defaults_required=True,
        field_title_generator=lambda field_name, field_info: to_pascal(
            field_name.replace("_", " ").strip()
        ),
    )


class AuthInputData(BaseModel):
    user_token: None | str = None
    jwt: None | str = None
    study_id: None | str = None
    obfuscation_code: None | str = None


class DecisionType(enum.StrEnum):
    ANY = "any"
    ALL = "all"
    NONE = "none"


class StudyResourceScope(enum.StrEnum):
    LIST = "list"
    CREATE = "create"
    VIEW = "view"
    UPDATE = "update"
    DELETE = "delete"
    DOWNLOAD = "download"
    UPLOAD = "upload"


class StudyResourceDbScope(enum.StrEnum):
    MAKE_PRIVATE = "make-private"
    MAKE_PROVISIONAL = "make-provisional"
    CREATE_REVISION = "create-revision"
    UPDATE_LICENSE = "update-license"


class StudyResource(enum.StrEnum):
    SUBMISSION = "submission"
    VALIDATION_REPORTS = "validation-reports"
    VALIDATION_OVERRIDES = "validation-overrides"
    METADATA_FILES = "metadata-files"
    DATA_FILES = "data-files"
    DATA_FILES_INDEX = "data-files-index"
    DB_METADATA = "db-metadata"
    STUDY_PUBLICATION = "study-publication"
    STUDY_REVISION_DATE = "study-revision-date"
    AUDIT_FILES = "audit-files"
    INTERNAL_FILES = "internal-files"
    STUDY_INDEX = "study-index"


PermisionScopeDict = Annotated[
    dict[StudyResource, list[StudyResourceScope | StudyResourceDbScope]],
    Field(description="Scopes for studyresources"),
]


class StudyResourceScopes(BaseScopeModel):
    resources: PermisionScopeDict = {}


class ScopeFilter(BaseScopeModel):
    allowed_roles: Annotated[
        None | list[UserRole],
        Field(
            description="Expected user roles. "
            "If it is not defined, user role match will be ignored. "
            "If there are multiple roles, user role MUST match any of them."
        ),
    ] = None
    scopes: dict[
        StudyResource,
        list[StudyResourceScope | StudyResourceDbScope],
    ] = {}
    scope_decision: DecisionType = DecisionType.ALL


class PermissionFilter(BaseScopeModel):
    filters: list[ScopeFilter] = []
    decision: DecisionType = DecisionType.ANY


class StudyAccessPermission(BaseScopeModel):
    user_name: str = ""
    user_role: str = ""
    partner: bool = False
    submitter_of_study: bool = False
    obfuscation_code: str = ""
    study_id: str = ""
    study_status: str = ""
    study_category: str = ""
    view: bool = False
    edit: bool = False
    delete: bool = False
    reason: str = ""
    scopes: PermisionScopeDict = {}


class StudyPermissionContext(BaseScopeModel):
    study_id: None | str = None
    study_status: None | StudyStatus = None
    obfuscation_code: None | str = None
    template_version: None | str = None
    sample_template: None | str = None
    study_template: None | str = None
    study_category: None | StudyCategory = None
    reserved_submission_id: None | str = None
    reserved_accession: None | str = None
    mhd_accession: None | str = None
    mhd_model_version: None | str = None
    revision_number: None | int = None
    curation_type: None | CurationRequest = None
    created_at: None | datetime.datetime = None
    first_private_date: None | datetime.datetime = None
    first_public_date: None | datetime.datetime = None
    revision_datetime: None | datetime.datetime = None
    dataset_license: None | str = None
    username: None | str = None
    user_role: None | UserRole = None
    user_api_token: None | str = None
    validated_jwt: None | str = None
    partner_user: None | bool = None
    owner: None | bool = None
    email_verified: None | bool = None


class RoleEvaluationResult(BaseScopeModel):
    context: None | StudyPermissionContext = None
    success: bool = False
    reason: None | str = None
    messages: None | list[str] = None


class StudyPermissionEvaluationResult(RoleEvaluationResult):
    permission: None | StudyAccessPermission = None


STUDY_PAGE_EMPTY_SCOPES: StudyResourceScopes = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [],
        StudyResource.VALIDATION_REPORTS: [],
        StudyResource.VALIDATION_OVERRIDES: [],
        StudyResource.METADATA_FILES: [],
        StudyResource.DATA_FILES: [],
        StudyResource.DATA_FILES_INDEX: [],
        StudyResource.DB_METADATA: [],
        StudyResource.STUDY_PUBLICATION: [],
        StudyResource.STUDY_REVISION_DATE: [],
        StudyResource.AUDIT_FILES: [],
        StudyResource.INTERNAL_FILES: [],
        StudyResource.STUDY_INDEX: [],
    }
)


STUDY_PAGE_ALL_SCOPES: StudyResourceScopes = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.VALIDATION_REPORTS: [
            StudyResourceScope.LIST,
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
        ],
        StudyResource.VALIDATION_OVERRIDES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.METADATA_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
            StudyResourceScope.LIST,
            StudyResourceScope.UPLOAD,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.DATA_FILES: [
            StudyResourceScope.UPLOAD,
            StudyResourceScope.DOWNLOAD,
            StudyResourceScope.DELETE,
        ],
        StudyResource.DATA_FILES_INDEX: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.DB_METADATA: [
            StudyResourceDbScope.MAKE_PRIVATE,
            StudyResourceDbScope.MAKE_PROVISIONAL,
            StudyResourceDbScope.CREATE_REVISION,
            StudyResourceDbScope.UPDATE_LICENSE,
        ],
        StudyResource.STUDY_PUBLICATION: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.STUDY_REVISION_DATE: [
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
        ],
        StudyResource.AUDIT_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.DELETE,
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.INTERNAL_FILES: [
            StudyResourceScope.LIST,
            StudyResourceScope.DELETE,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.STUDY_INDEX: [
            StudyResourceScope.VIEW,
            StudyResourceScope.CREATE,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
    }
)

CURATOR_PRIVATE_STUDY_PAGE_SCOPES = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.VALIDATION_REPORTS: [
            StudyResourceScope.LIST,
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
        ],
        StudyResource.VALIDATION_OVERRIDES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.METADATA_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
            StudyResourceScope.LIST,
            StudyResourceScope.UPLOAD,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.DATA_FILES: [
            StudyResourceScope.UPLOAD,
            StudyResourceScope.DOWNLOAD,
            StudyResourceScope.DELETE,
        ],
        StudyResource.DATA_FILES_INDEX: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.DB_METADATA: [
            StudyResourceDbScope.MAKE_PRIVATE,
            StudyResourceDbScope.UPDATE_LICENSE,
        ],
        StudyResource.STUDY_PUBLICATION: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.STUDY_REVISION_DATE: [
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
        ],
        StudyResource.AUDIT_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.DELETE,
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.INTERNAL_FILES: [
            StudyResourceScope.LIST,
            StudyResourceScope.DELETE,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.STUDY_INDEX: [
            StudyResourceScope.VIEW,
            StudyResourceScope.CREATE,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
    }
)


CURATOR_PUBLIC_STUDY_PAGE_SCOPES = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [StudyResourceScope.VIEW, StudyResourceScope.UPDATE],
        StudyResource.VALIDATION_REPORTS: [
            StudyResourceScope.LIST,
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
        ],
        StudyResource.VALIDATION_OVERRIDES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.METADATA_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
            StudyResourceScope.LIST,
            StudyResourceScope.UPLOAD,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.DATA_FILES: [
            StudyResourceScope.UPLOAD,
            StudyResourceScope.DOWNLOAD,
            StudyResourceScope.DELETE,
        ],
        StudyResource.DATA_FILES_INDEX: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.DB_METADATA: [
            StudyResourceDbScope.CREATE_REVISION,
            StudyResourceDbScope.UPDATE_LICENSE,
        ],
        StudyResource.STUDY_PUBLICATION: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.STUDY_REVISION_DATE: [
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
        ],
        StudyResource.AUDIT_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.DELETE,
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.INTERNAL_FILES: [
            StudyResourceScope.LIST,
            StudyResourceScope.DELETE,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.STUDY_INDEX: [
            StudyResourceScope.VIEW,
            StudyResourceScope.CREATE,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
    }
)

SUBMITTER_PROVISION_STUDY_PAGE_SCOPES: StudyResourceScopes = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.VALIDATION_REPORTS: [
            StudyResourceScope.LIST,
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
        ],
        StudyResource.VALIDATION_OVERRIDES: [StudyResourceScope.VIEW],
        StudyResource.METADATA_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
            StudyResourceScope.LIST,
            StudyResourceScope.UPLOAD,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.DATA_FILES: [
            StudyResourceScope.UPLOAD,
            StudyResourceScope.DOWNLOAD,
            StudyResourceScope.DELETE,
        ],
        StudyResource.DATA_FILES_INDEX: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
        ],
        StudyResource.DB_METADATA: [
            StudyResourceDbScope.MAKE_PRIVATE,
            StudyResourceDbScope.UPDATE_LICENSE,
        ],
        StudyResource.STUDY_PUBLICATION: [
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
            StudyResourceScope.DELETE,
        ],
        StudyResource.STUDY_REVISION_DATE: [
            StudyResourceScope.VIEW,
            StudyResourceScope.UPDATE,
        ],
        StudyResource.AUDIT_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.INTERNAL_FILES: [
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.STUDY_INDEX: [
            StudyResourceScope.VIEW,
            StudyResourceScope.CREATE,
            StudyResourceScope.UPDATE,
        ],
    }
)
SUBMITTER_PRIVATE_STUDY_PAGE_SCOPES: StudyResourceScopes = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [StudyResourceScope.VIEW],
        StudyResource.VALIDATION_REPORTS: [
            StudyResourceScope.LIST,
            StudyResourceScope.CREATE,
            StudyResourceScope.VIEW,
        ],
        StudyResource.VALIDATION_OVERRIDES: [StudyResourceScope.VIEW],
        StudyResource.METADATA_FILES: [
            StudyResourceScope.LIST,
            StudyResourceScope.VIEW,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.DATA_FILES: [StudyResourceScope.DOWNLOAD],
        StudyResource.DATA_FILES_INDEX: [StudyResourceScope.VIEW],
        StudyResource.DB_METADATA: [
            StudyResourceDbScope.MAKE_PROVISIONAL,
            StudyResourceDbScope.CREATE_REVISION,
        ],
        StudyResource.STUDY_PUBLICATION: [StudyResourceScope.VIEW],
        StudyResource.STUDY_REVISION_DATE: [StudyResourceScope.VIEW],
        StudyResource.AUDIT_FILES: [
            StudyResourceScope.CREATE,
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.INTERNAL_FILES: [
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.STUDY_INDEX: [StudyResourceScope.VIEW],
    }
)
SUBMITTER_PUBLIC_STUDY_PAGE_SCOPES: StudyResourceScopes = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [StudyResourceScope.VIEW],
        StudyResource.VALIDATION_REPORTS: [
            StudyResourceScope.LIST,
            StudyResourceScope.VIEW,
        ],
        StudyResource.VALIDATION_OVERRIDES: [StudyResourceScope.VIEW],
        StudyResource.METADATA_FILES: [
            StudyResourceScope.LIST,
            StudyResourceScope.VIEW,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.DATA_FILES: [StudyResourceScope.DOWNLOAD],
        StudyResource.DATA_FILES_INDEX: [StudyResourceScope.VIEW],
        StudyResource.STUDY_PUBLICATION: [StudyResourceScope.VIEW],
        StudyResource.STUDY_REVISION_DATE: [StudyResourceScope.VIEW],
        StudyResource.AUDIT_FILES: [
            StudyResourceScope.VIEW,
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.INTERNAL_FILES: [],
        StudyResource.STUDY_INDEX: [],
    }
)
PUBLIC_STUDY_PAGE_SCOPES: StudyResourceScopes = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [StudyResourceScope.VIEW],
        StudyResource.VALIDATION_REPORTS: [],
        StudyResource.VALIDATION_OVERRIDES: [],
        StudyResource.METADATA_FILES: [
            StudyResourceScope.LIST,
            StudyResourceScope.VIEW,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.DATA_FILES: [StudyResourceScope.DOWNLOAD],
        StudyResource.DATA_FILES_INDEX: [StudyResourceScope.VIEW],
        StudyResource.DB_METADATA: [],
        StudyResource.STUDY_PUBLICATION: [StudyResourceScope.VIEW],
        StudyResource.STUDY_REVISION_DATE: [StudyResourceScope.VIEW],
        StudyResource.AUDIT_FILES: [],
        StudyResource.INTERNAL_FILES: [],
        StudyResource.STUDY_INDEX: [],
    }
)

REVIEWER_PROVISION_STUDY_PAGE_SCOPES: StudyResourceScopes = StudyResourceScopes(
    resources={
        StudyResource.SUBMISSION: [StudyResourceScope.VIEW],
        StudyResource.VALIDATION_REPORTS: [
            StudyResourceScope.LIST,
            StudyResourceScope.VIEW,
        ],
        StudyResource.VALIDATION_OVERRIDES: [StudyResourceScope.VIEW],
        StudyResource.METADATA_FILES: [
            StudyResourceScope.VIEW,
            StudyResourceScope.LIST,
            StudyResourceScope.DOWNLOAD,
        ],
        StudyResource.DATA_FILES: [StudyResourceScope.DOWNLOAD],
        StudyResource.DATA_FILES_INDEX: [StudyResourceScope.VIEW],
        StudyResource.DB_METADATA: [],
        StudyResource.STUDY_PUBLICATION: [StudyResourceScope.VIEW],
        StudyResource.STUDY_REVISION_DATE: [StudyResourceScope.VIEW],
        StudyResource.AUDIT_FILES: [],
        StudyResource.INTERNAL_FILES: [],
        StudyResource.STUDY_INDEX: [StudyResourceScope.VIEW],
    }
)
