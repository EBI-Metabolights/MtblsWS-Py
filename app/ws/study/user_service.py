import logging
from typing import Any, List, Self, Union

from sqlalchemy import func, or_

from app.utils import MetabolightsAuthorizationException, MetabolightsException
from app.ws.auth.service import AbstractAuthManager
from app.ws.db import permission_scopes as scopes
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import SimplifiedUserModel, UserModel
from app.ws.db.schemes import Study, User
from app.ws.db.types import (
    ActiveUserRoles,
    CurationRequest,
    StudyCategory,
    StudyStatus,
    UserRole,
    UserStatus,
)
from app.ws.db.utils import datetime_to_int
from app.ws.settings.utils import get_study_settings

logger = logging.getLogger(__name__)


class UserService(object):
    instances: dict[Any, Any] = {}
    db_manager = None
    study_settings = None

    def __init__(self, auth_manager: None | AbstractAuthManager = None):
        self.auth_manager = auth_manager

    @classmethod
    def get_instance(cls, auth_manager: AbstractAuthManager) -> Self:
        input_hash = hash(auth_manager)
        instance = cls.instances.get(input_hash)
        if not instance:
            cls.instances[input_hash] = UserService(auth_manager)
            cls.study_settings = get_study_settings()

        return cls.instances.get(input_hash)

    def get_user_studies(self, user_token):
        try:
            with DBManager.get_instance().session_maker() as db_session:
                base_query = db_session.query(Study)
                query = base_query.join(User, Study.users)
                studies = query.filter(User.apitoken == user_token).all()
                return studies
        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving user from database", exception=e
            )

    def get_study_submitters(self, study_id):
        try:
            with DBManager.get_instance().session_maker() as db_session:
                base_query = db_session.query(User)
                query = base_query.join(Study, User.studies)
                submitters = query.filter(
                    Study.acc == study_id, User.status == UserStatus.ACTIVE.value
                ).all()
                return submitters
        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving submittes from database", exception=e
            )

    def get_permission_context(
        self,
        user_token: None | str = None,
        jwt: None | str = None,
        study_id: None | str = None,
        obfuscation_code: None | str = None,
    ) -> tuple[scopes.StudyPermissionContext, list[str]]:
        permission_context = scopes.StudyPermissionContext()
        username: None | str = None
        messages = []
        if jwt:
            if not self.auth_manager:
                raise MetabolightsException("Authentication manager is not defined.")
            try:
                user = self.auth_manager.validate_oauth2_token(jwt)
                username = user.email
                email_verified = UserStatus(user.status) == UserStatus.ACTIVE
                permission_context.validated_jwt = jwt
                permission_context.username = username
                permission_context.email_verified = email_verified
            except Exception as ex:
                messages.append(f"Invalid JWT token: {str(ex)}")

        with DBManager.get_instance().session_maker() as db_session:
            if study_id or obfuscation_code:
                filters = []
                if study_id:
                    filters.append(
                        or_(
                            Study.reserved_submission_id == study_id,
                            Study.reserved_accession == study_id,
                            Study.mhd_accession == study_id,
                        )
                    )
                if obfuscation_code:
                    filters.append(Study.obfuscationcode == obfuscation_code)

                query = db_session.query(Study)
                study: Study = query.filter(*filters).first()
                if study and obfuscation_code in {study.obfuscationcode, None}:
                    permission_context.obfuscation_code = study.obfuscationcode
                    permission_context.study_id = study.acc
                    permission_context.reserved_accession = study.reserved_accession
                    permission_context.reserved_submission_id = (
                        study.reserved_submission_id
                    )
                    permission_context.mhd_accession = study.mhd_accession
                    permission_context.mhd_model_version = study.mhd_model_version
                    permission_context.study_category = StudyCategory(
                        study.study_category
                    )
                    permission_context.sample_template = study.sample_type
                    permission_context.template_version = study.template_version

                    permission_context.study_status = StudyStatus(study.status)
                    permission_context.curation_type = CurationRequest(
                        study.curation_request
                    )
                    permission_context.revision_number = study.revision_number
                    permission_context.revision_datetime = study.revision_datetime
                    permission_context.first_private_date = study.first_private_date
                    permission_context.first_public_date = study.first_public_date
                    permission_context.study_template = study.study_template
                    permission_context.created_at = study.created_at
                    permission_context.expected_release_date = study.releasedate

            if username or user_token:
                base_query = db_session.query(User)
                user_filter = [User.status == UserStatus.ACTIVE.value]
                if username:
                    user_filter.append(User.username == username)
                else:
                    user_filter.append(User.apitoken == user_token)

                token_user: User = base_query.filter(*user_filter).first() or None
                if token_user:
                    permission_context.username = token_user.username
                    permission_context.user_role = UserRole(token_user.role)
                    permission_context.user_api_token = token_user.apitoken
                    permission_context.partner_user = token_user.partner == 1
            if permission_context.username and permission_context.study_id:
                owner_filter = user_filter.copy()
                owner_filter.append(Study.acc == permission_context.study_id)
                query = base_query.join(Study, User.studies)

                owner = query.filter(*owner_filter).first() or None
                permission_context.owner = True if owner else False
        return permission_context, messages

    ROLE_CODE = {
        "role-01": "Invalid role filter",
        "role-02": "No user or user has not been validated",
        "role-03": "Study is not valid",
        "role-04": "User has the required role",
        "role-05": "User has no required role",
    }

    def validate_user_roles(
        self,
        roles: list[UserRole],
        user_token: None | str = None,
        jwt: None | str = None,
        study_id: None | str = None,
        obfuscation_code: None | str = None,
        fail_silently: bool = False,
        study_required: bool = False,
    ) -> scopes.RoleEvaluationResult:
        if not roles or not isinstance(roles, list):
            if fail_silently:
                return scopes.RoleEvaluationResult(
                    reason="role-01", messages=["Empty role filter"]
                )
            else:
                raise MetabolightsAuthorizationException(
                    message="User has unexpected permissions to execute."
                )

        permission_context, messages = self.get_permission_context(
            user_token=user_token,
            jwt=jwt,
            study_id=study_id,
            obfuscation_code=obfuscation_code,
        )
        result = scopes.RoleEvaluationResult(
            context=permission_context, messages=messages
        )
        if not permission_context.username:
            if fail_silently:
                result.reason = "rule-02"
                return result
            else:
                raise MetabolightsAuthorizationException(
                    message="No user or user has not been validated."
                )

        if study_required and not permission_context.study_id:
            if fail_silently:
                result.reason = "rule-3"
                return result
            else:
                raise MetabolightsAuthorizationException(message="Study is not valid.")

        if permission_context.user_role in roles:
            result.reason = "rule-04"
            result.success = True
            return result
        if fail_silently:
            result.reason = "rule-05"
            return result
        else:
            raise MetabolightsAuthorizationException(
                message="User role has not permission."
            )

    PERMISSION_CODE = {
        "permission-01": "Permission filter is not defined",
        "permission-02": "Not a valid study",
        "permission-03": "Not a valid user",
        "permission-04": "Does not match any permission filter",
        "permission-05": "Match at least one permission filter",
        "permission-06": "Match unexpected permissions",
        "permission-07": "No unexpected permission",
        "permission-08": "Does not match all permission filters",
        "permission-09": "Match all permission filters ",
    }

    def validate_permissions(
        self,
        study_id: str,
        permissions: scopes.PermissionFilter,
        user_token: None | str = None,
        obfuscation_code: None | str = None,
        jwt: None | str = None,
        fail_silently: bool = False,
        user_required: bool = True,
    ) -> scopes.StudyPermissionEvaluationResult:
        if not permissions or not permissions.filters:
            if fail_silently:
                return scopes.StudyPermissionEvaluationResult(reason="permission-01")
            else:
                raise MetabolightsAuthorizationException(
                    message="There is not permission filter"
                )
        permission_context, messages = self.get_permission_context(
            user_token=user_token,
            jwt=jwt,
            study_id=study_id,
            obfuscation_code=obfuscation_code,
        )
        study_permission = self.evaluate_study_permission(
            context=permission_context, user_required=user_required
        )

        result = scopes.StudyPermissionEvaluationResult(
            permission=study_permission, context=permission_context, messages=messages
        )
        if not permission_context.study_id:
            if fail_silently:
                result.reason = "rule-02"
                return result
            else:
                raise MetabolightsAuthorizationException(
                    message="No user or user has not been validated."
                )

        if permission_context.user_role not in ActiveUserRoles and user_required:
            if fail_silently:
                result.reason = "rule-03"
                return result
            else:
                raise MetabolightsAuthorizationException(
                    message="No user or is not validated."
                )

        matches: list[bool] = []
        for filter in [x for x in permissions.filters if x]:
            user_role = permission_context.user_role
            if filter.allowed_roles and user_role not in filter.allowed_roles:
                matches.append(False)
                continue

            if not filter.scopes:
                matches.append(True)
                continue

            for resource, filter_scopes in filter.scopes.items():
                permission_scopes = study_permission.scopes.get(resource, [])
                scope_evaluation = [
                    True if x in permission_scopes else False for x in filter_scopes
                ]
                if filter.scope_decision == scopes.DecisionType.ALL:
                    matches.append(all(scope_evaluation))
                elif filter.scope_decision == scopes.DecisionType.ANY:
                    matches.append(any(scope_evaluation))
                elif filter.scope_decision == scopes.DecisionType.NONE:
                    matches.append(
                        all([True if not x else False for x in scope_evaluation])
                    )
                else:
                    # default decision is all
                    matches.append(all(scope_evaluation))

        if permissions.decision == scopes.DecisionType.ANY:
            if any(matches):
                result.success = True
                result.reason = "permission-04"
                return result
            elif fail_silently:
                result.reason = "permission-05"
                return result
            else:
                raise MetabolightsAuthorizationException(
                    message="User has no permission to execute."
                )
        if permissions.decision == scopes.DecisionType.NONE:
            if all([True if not x else False for x in matches]):
                result.success = True
                result.reason = "permission-06"
                return result
            elif fail_silently:
                result.reason = "permission-07"
                return result
            else:
                raise MetabolightsAuthorizationException(
                    message="User has unexpected permissions to execute."
                )

        if all(matches):
            result.success = True
            result.reason = "permission-08"
            return result
        elif fail_silently:
            result.reason = "permission-09"
            return result
        else:
            raise MetabolightsAuthorizationException(
                message="User has not enough permission to execute."
            )

    REASON_CODE = {
        "reason-01": "not valid study id or no study found with the criteria",
        "reason-02": "Unauthenticated user access",
        "reason-03": "a public study accessed by a user that is not owner or has no curator role",
        "reason-04": "a private study accessed by a user that is not owner or has no curator role",
        "reason-05": "a provisional study accessed by a user that is not owner or has no curator role",
        "reason-06": "a public study accessed by curator",
        "reason-07": "a private study accessed by curator",
        "reason-08": "a provisional study accessed by curator",
        "reason-09": "a provision study accessed by study owner",
        "reason-10": "a private or public study accessed by study owner",
    }

    def evaluate_study_permission(
        self, context: scopes.StudyPermissionContext, user_required: bool = True
    ) -> scopes.StudyAccessPermission:
        permission = scopes.StudyAccessPermission()
        study_id = context.study_id
        user_role = context.user_role
        if not study_id:
            permission.reason = "reason-01"
            self.copy_scopes(permission, scopes.STUDY_PAGE_EMPTY_SCOPES)
            return permission

        permission.user_role = (
            context.user_role.name if context.user_role is not None else None
        )
        permission.user_name = context.username
        permission.partner = context.partner_user
        permission.submitter_of_study = context.owner

        if user_role not in ActiveUserRoles and user_required:
            permission.reason = "reason-02"
            self.copy_scopes(permission, scopes.STUDY_PAGE_EMPTY_SCOPES)
            return permission
        curator_roles = {UserRole.ROLE_SUPER_USER, UserRole.SYSTEM_ADMIN}
        category = (
            context.study_category.name.lower().replace("_", "-")
            if context.study_category
            else None
        )
        if not context.owner and context.user_role not in curator_roles:
            if context.study_status == StudyStatus.PUBLIC:
                permission.study_id = context.study_id
                permission.study_status = context.study_status.name
                permission.study_category = category
                permission.reason = "reason-03"
                self.copy_scopes(permission, scopes.PUBLIC_STUDY_PAGE_SCOPES)
                return permission
            if (
                context.study_status in (StudyStatus.INREVIEW, StudyStatus.PRIVATE)
                and context.obfuscation_code
            ):
                permission.study_id = context.study_id
                permission.study_status = context.study_status.name
                permission.obfuscation_code = context.obfuscation_code
                permission.study_category = category
                self.copy_scopes(permission, scopes.REVIEWER_PRIVATE_STUDY_PAGE_SCOPES)
                permission.reason = "reason-04"
                return permission
            permission.study_id = context.study_id
            permission.reason = "reason-05"
            self.copy_scopes(permission, scopes.STUDY_PAGE_EMPTY_SCOPES)
            return permission

        permission.study_id = context.study_id
        permission.study_status = context.study_status.name
        permission.obfuscation_code = context.obfuscation_code
        permission.study_category = category
        if context.user_role in curator_roles:
            if context.study_status == StudyStatus.PUBLIC:
                self.copy_scopes(permission, scopes.CURATOR_PUBLIC_STUDY_PAGE_SCOPES)
                permission.reason = "reason-06"
                return permission
            elif context.study_status == StudyStatus.PRIVATE:
                permission.reason = "reason-07"
                self.copy_scopes(permission, scopes.CURATOR_PRIVATE_STUDY_PAGE_SCOPES)
                return permission
            permission.reason = "reason-08"
            self.copy_scopes(permission, scopes.CURATOR_PROVISIONAL_STUDY_PAGE_SCOPES)
            return permission

        if context.study_status == StudyStatus.PROVISIONAL:
            self.copy_scopes(permission, scopes.SUBMITTER_PROVISION_STUDY_PAGE_SCOPES)
            permission.reason = "reason-09"
        else:
            permission.reason = "reason-10"

            if context.study_status == StudyStatus.PRIVATE:
                self.copy_scopes(permission, scopes.SUBMITTER_PRIVATE_STUDY_PAGE_SCOPES)

            elif context.study_status == StudyStatus.PUBLIC:
                self.copy_scopes(permission, scopes.SUBMITTER_PUBLIC_STUDY_PAGE_SCOPES)

            else:
                self.copy_scopes(permission, scopes.PUBLIC_STUDY_PAGE_SCOPES)
        return permission

    def copy_scopes(
        self,
        permission: scopes.StudyAccessPermission,
        scopes_obj: scopes.StudyResourceScopes,
    ):
        copy = scopes_obj.model_copy(deep=True)
        submission_scopes = (
            copy.resources.get(scopes.StudyResource.SUBMISSION, []) or []
        )
        for submission in submission_scopes:
            if submission == scopes.StudyResourceScope.UPDATE:
                permission.edit = True
            if submission == scopes.StudyResourceScope.DELETE:
                permission.delete = True
            if submission == scopes.StudyResourceScope.VIEW:
                permission.view = True

        scope_dict: dict[str, Any] = copy.model_dump(by_alias=True)
        resources: dict[str, list] = scope_dict.get("resources", {})

        permission.scopes = {k: v for k, v in resources.items() if v}

    def validate_user_is_submitter_of_study_or_has_curator_role(
        self, user_token, study_id
    ):
        user = None
        exception = None

        try:
            user = self.validate_user_has_curator_role(user_token)
        except Exception as ex:
            exception = ex
            submitters = self.get_study_submitters(study_id)
            if submitters:
                for submitter in submitters:
                    if submitter.apitoken == user_token:
                        user = {
                            "id": submitter.id,
                            "username": submitter.username,
                            "role": submitter.role,
                            "status": submitter.status,
                            "apitoken": submitter.apitoken,
                            "password": submitter.password,
                            "partner": submitter.partner,
                        }

        if not user:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving user from database", exception=exception
            )
        return user

    def validate_user_has_write_access(self, user_token, study_id):
        try:
            with DBManager.get_instance().session_maker() as db_session:
                base_query = db_session.query(
                    User.id,
                    User.username,
                    User.role,
                    User.status,
                    User.apitoken,
                    User.partner,
                    Study.status,
                )
                query = base_query.join(Study, User.studies)
                result = query.filter(
                    Study.acc == study_id,
                    User.apitoken == user_token,
                    User.status == UserStatus.ACTIVE.value,
                ).first()
                if result and StudyStatus(result[6]) in {StudyStatus.PROVISIONAL}:
                    return result
        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving user from database", exception=e
            )

        with DBManager.get_instance().session_maker() as db_session:
            study = db_session.query(Study.acc).filter(Study.acc == study_id).first()
            if study:
                return self.validate_user_has_curator_role(user_token)
            raise MetabolightsAuthorizationException(message="Not a valid study id")

    def validate_user_has_read_access(self, user_token, study_id, obfuscationcode=None):
        if not study_id:
            raise MetabolightsAuthorizationException(message="Not a valid study id")
        try:
            with DBManager.get_instance().session_maker() as db_session:
                base_query = db_session.query(
                    Study.acc, Study.status, Study.obfuscationcode
                )
                study = base_query.filter(Study.acc == study_id).first()
                if not study:
                    raise MetabolightsAuthorizationException(
                        message="Not a valid study id"
                    )
                else:
                    if study[1] == StudyStatus.PUBLIC.value:
                        return True
                    else:
                        if obfuscationcode:
                            if study[2] == obfuscationcode and study[1] in (
                                StudyStatus.INREVIEW.value,
                                StudyStatus.PRIVATE.value,
                                StudyStatus.PROVISIONAL.value,
                            ):
                                return True
                            if study[2] != obfuscationcode:
                                raise MetabolightsAuthorizationException(
                                    message="Not a valid study id or obfuscation code"
                                )

        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving user from database", exception=e
            )
        try:
            with DBManager.get_instance().session_maker() as db_session:
                base_query = db_session.query(
                    User.id,
                    User.username,
                    User.role,
                    User.status,
                    User.apitoken,
                    User.partner,
                    Study.status,
                )
                query = base_query.join(Study, User.studies)
                result = query.filter(
                    Study.acc == study_id,
                    User.apitoken == user_token,
                    User.status == UserStatus.ACTIVE.value,
                ).first()
                if result:
                    return True
                self.validate_user_has_curator_role(user_token)
        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving user from database", exception=e
            )
        return True

    def validate_user_has_curator_role(self, user_token):
        return self.validate_user_by_token(user_token, [UserRole.ROLE_SUPER_USER.value])

    def validate_username_with_submitter_or_super_user_role(self, user_name):
        return self.validate_user_by_username(
            user_name, [UserRole.ROLE_SUBMITTER.value, UserRole.ROLE_SUPER_USER.value]
        )

    def validate_user_has_submitter_or_super_user_role(self, user_token):
        return self.validate_user_by_token(
            user_token, [UserRole.ROLE_SUBMITTER.value, UserRole.ROLE_SUPER_USER.value]
        )

    def validate_user_by_username(
        self, user_name, allowed_role_list, allowed_status_list=None
    ):
        if not user_name:
            raise MetabolightsException(message="Invalid user or credential")

        filter_clause = lambda query: query.filter(
            func.lower(User.username) == user_name.lower()
        )

        return self.validate_user_by_user_field(
            filter_clause, allowed_role_list, allowed_status_list
        )

    def validate_user_by_token(
        self, user_token, allowed_role_list, allowed_status_list=None
    ):
        if not user_token:
            raise MetabolightsException(message="User token is not valid")

        filter_clause = lambda query: query.filter(User.apitoken == user_token)

        return self.validate_user_by_user_field(
            filter_clause, allowed_role_list, allowed_status_list
        )

    def validate_user_by_user_field(
        self, filter_clause, allowed_role_list, allowed_status_list=None
    ):
        if not allowed_role_list:
            raise MetabolightsAuthorizationException(
                message="Define user role to validate"
            )

        if not allowed_status_list:
            allowed_status_list = [UserStatus.ACTIVE.value]

        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(
                    User.id,
                    User.username,
                    User.role,
                    User.status,
                    User.apitoken,
                    User.password,
                    User.partner,
                    User.firstname,
                    User.lastname,
                )
                db_user = filter_clause(query).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Invalid user or credential", exception=e
            )

        if db_user:
            if int(db_user.status) not in allowed_status_list:
                raise MetabolightsAuthorizationException(message="Invalid user status")

            if db_user.role not in allowed_role_list:
                raise MetabolightsAuthorizationException(message="Invalid user role")
            return db_user
        else:
            raise MetabolightsAuthorizationException(
                message="Invalid user or credential"
            )

    def get_simplified_user_by_username(
        self, user_name
    ) -> Union[None, SimplifiedUserModel]:
        if not user_name:
            raise MetabolightsException(message="User token is not valid")

        filter_clause = lambda query: query.filter(User.username == user_name)

        return self.get_simplified_user_by_user_field(filter_clause)

    def get_simplified_user_by_token(
        self, user_token
    ) -> Union[None, SimplifiedUserModel]:
        if not user_token:
            raise MetabolightsException(message="User token is not valid")

        filter_clause = lambda query: query.filter(User.apitoken == user_token)

        return self.get_simplified_user_by_user_field(filter_clause)

    def get_simplified_user_by_user_field(
        self, filter_clause
    ) -> Union[None, SimplifiedUserModel]:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(User)
                db_user: User = filter_clause(query).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving user from database", exception=e
            )

        if db_user:
            m_user = SimplifiedUserModel.model_validate(db_user)
            m_user.email = m_user.email.lower()
            m_user.userName = m_user.userName.lower()
            m_user.fullName = m_user.firstName + " " + m_user.lastName
            m_user.role = UserRole(m_user.role).name
            m_user.partner = True if m_user.partner else False
            m_user.status = UserStatus(m_user.status).name
            m_user.joinDate = datetime_to_int(m_user.joinDate)
            return m_user
        else:
            raise MetabolightsAuthorizationException(message="User not in database")

    def get_db_user_by_user_token(self, user_token: str) -> Union[None, User]:
        filter_clause = lambda query: query.filter(User.apitoken == user_token)
        return self.get_db_user_by_filter_clause(filter_clause=filter_clause)

    def get_db_user_by_user_name(self, user_name: str) -> Union[None, UserModel]:
        filter_clause = lambda query: query.filter(User.username == user_name)
        m_user = self.get_db_user_by_filter_clause(filter_clause=filter_clause)
        m_user.email = m_user.email.lower()
        m_user.userName = m_user.userName.lower()
        m_user.fullName = m_user.firstName + " " + m_user.lastName
        m_user.status = UserStatus(m_user.status).name
        m_user.joinDate = datetime_to_int(m_user.joinDate)
        return m_user

    def get_db_user_by_filter_clause(self, filter_clause) -> Union[None, UserModel]:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(User)
                db_user: User = filter_clause(query).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving user from database", exception=e
            )

        if db_user:
            m_user = UserModel.model_validate(db_user)
            return m_user
        else:
            raise MetabolightsAuthorizationException(message="User not in database")

    def get_db_users_by_filter_clause(self, filter_clause=None) -> List[UserModel]:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(User)
                if filter_clause:
                    db_users = filter_clause(query).all()
                else:
                    db_users = query.all()
        except Exception as e:
            raise MetabolightsAuthorizationException(
                message="Error while retreiving user from database", exception=e
            )
        users: List[UserModel] = []
        if db_users:
            for db_user in db_users:
                m_user = UserModel.model_validate(db_user)
                users.append(m_user)

        return users
