from app.utils import DeprecationError
from app.ws.auth.auth_manager import AuthenticationManager
from app.ws.auth.one_time_token import get_jwt_with_one_time_token
from app.ws.db.permission_scopes import (
    AuthInputData,
    DecisionType,
    PermissionFilter,
    RoleEvaluationResult,
    ScopeFilter,
    StudyPermissionEvaluationResult,
    StudyResource,
    StudyResourceDbScope,
    StudyResourceScope,
)
from app.ws.db.types import UserRole
from app.ws.study.user_service import UserService


def auth_endpoint(request):
    return request.path


def public_endpoint(request):
    return request.path


def raise_deprecation_error(request):
    raise DeprecationError(message=f"Deprecated endpoint: '{request.path}'")


def validate_user_has_curator_role(
    request, fail_silently: bool = False, study_required: bool = False
) -> RoleEvaluationResult:
    return validate_user_has_role(
        request=request,
        roles=[UserRole.ROLE_SUPER_USER, UserRole.SYSTEM_ADMIN],
        fail_silently=fail_silently,
        study_required=study_required,
    )


def validate_user_has_submitter_or_super_user_role(
    request, fail_silently: bool = False, study_required: bool = False
) -> RoleEvaluationResult:
    return validate_user_has_role(
        request=request,
        roles=[
            UserRole.ROLE_SUBMITTER,
            UserRole.ROLE_SUPER_USER,
            UserRole.SYSTEM_ADMIN,
        ],
        fail_silently=fail_silently,
        study_required=study_required,
    )


def validate_submission_view(
    request, fail_silently: bool = False, user_required=False
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={StudyResource.SUBMISSION: [StudyResourceScope.VIEW]},
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_submission_update(
    request, fail_silently: bool = False, user_required=True
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={StudyResource.SUBMISSION: [StudyResourceScope.UPDATE]},
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_metadata_files_delete(
    request, fail_silently: bool = False, user_required=True
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={
                        StudyResource.METADATA_FILES: [StudyResourceScope.DELETE],
                    },
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_data_files_upload(
    request, fail_silently: bool = False, user_required=True
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={
                        StudyResource.DATA_FILES: [StudyResourceScope.UPLOAD],
                    }
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_audit_files_update(
    request, fail_silently: bool = False, user_required=True
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={
                        StudyResource.AUDIT_FILES: [StudyResourceScope.CREATE],
                    }
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_audit_files_view(
    request, fail_silently: bool = False, user_required=False
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={
                        StudyResource.AUDIT_FILES: [
                            StudyResourceScope.VIEW,
                            StudyResourceScope.LIST,
                        ],
                    },
                    scope_decision=DecisionType.ANY,
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_data_files_index_view(
    request, fail_silently: bool = False, user_required=False
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={
                        StudyResource.DATA_FILES_INDEX: [StudyResourceScope.VIEW],
                    }
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_study_index_update(
    request, fail_silently: bool = False, user_required=True
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={
                        StudyResource.STUDY_INDEX: [
                            StudyResourceScope.CREATE,
                            StudyResourceScope.UPDATE,
                        ],
                    }
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_study_index_delete(
    request, fail_silently: bool = False, user_required=True
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={
                        StudyResource.STUDY_INDEX: [
                            StudyResourceScope.DELETE,
                        ],
                    }
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


def validate_resource_scopes(
    request,
    resource: StudyResource,
    scopes: list[StudyResourceDbScope | StudyResourceScope],
    fail_silently: bool = False,
    user_required=True,
) -> StudyPermissionEvaluationResult:
    return validate_permissions(
        request,
        permissions=PermissionFilter(
            filters=[
                ScopeFilter(
                    scopes={resource: scopes},
                )
            ]
        ),
        fail_silently=fail_silently,
        user_required=user_required,
    )


###############################################################################
# Role and permission evaluation methods
###############################################################################


def validate_user_has_role(
    request,
    roles: list[UserRole],
    fail_silently: bool = False,
    study_required: bool = False,
) -> RoleEvaluationResult:
    auth_data = get_auth_data(request)
    auth_manager = AuthenticationManager.get_instance()
    return UserService.get_instance(auth_manager).validate_user_roles(
        roles=roles,
        user_token=auth_data.user_token,
        jwt=auth_data.jwt,
        study_id=auth_data.study_id,
        obfuscation_code=auth_data.obfuscation_code,
        fail_silently=fail_silently,
        study_required=study_required,
    )


def get_auth_data(request) -> AuthInputData:
    user_token = request.headers.get("user_token", None) or None

    obfuscation_code = request.headers.get("obfuscation_code", None) or None
    if not obfuscation_code:
        obfuscation_code = request.view_args.get("obfuscation_code", None) or None

    jwt = None
    if "authorization" in request.headers:
        auth = request.headers.get("authorization", None) or None
        if auth:
            parts = auth.split(maxsplit=1)
            if len(parts) == 2 and parts[0].lower() == "bearer":
                jwt = parts[1]
            else:
                # backward compatibility. Use current token as bearer token
                jwt = auth
    if not jwt:
        passcode = request.args.get("passcode", None)
        jwt = get_jwt_with_one_time_token(passcode)

    study_id = request.view_args.get("study_id", "").upper() or None
    if not study_id:
        study_id = request.headers.get("study_id", None) or None

    return AuthInputData(
        user_token=user_token,
        jwt=jwt,
        study_id=study_id,
        obfuscation_code=obfuscation_code,
    )


def validate_permissions(
    request, permissions, fail_silently: bool = False, user_required: bool = False
) -> StudyPermissionEvaluationResult:
    auth_data = get_auth_data(request)

    if not auth_data.study_id and not auth_data.obfuscation_code:
        raise ValueError("study is is not defined")
    auth_manager = AuthenticationManager.get_instance()
    return UserService.get_instance(auth_manager).validate_permissions(
        study_id=auth_data.study_id,
        permissions=permissions,
        user_token=auth_data.user_token,
        obfuscation_code=auth_data.obfuscation_code,
        jwt=auth_data.jwt,
        fail_silently=fail_silently,
        user_required=user_required,
    )
