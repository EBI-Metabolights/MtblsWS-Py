from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyAccessPermission
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus, UserRole, UserStatus
from app.ws.study.user_service import UserService


def get_permission_by_study_id(study_id, user_token) -> StudyAccessPermission:
    filter_clause = lambda query: query.filter(Study.acc == study_id)
    return get_study_permission(user_token, filter_clause, obfuscation_code=None)


def get_permission_by_obfuscation_code(user_token, obfuscation_code) -> StudyAccessPermission:
    filter_clause = lambda query: query.filter(
        Study.obfuscationcode == obfuscation_code
    )
    return get_study_permission(
        user_token, filter_clause, obfuscation_code=obfuscation_code
    )


def get_study_permission(
    user_token,
    filter_clause,
    obfuscation_code=None
) -> StudyAccessPermission:
    permission: StudyAccessPermission = StudyAccessPermission()
    with DBManager.get_instance().session_maker() as db_session:
        query = db_session.query(Study.acc, Study.status, Study.obfuscationcode)
        study = filter_clause(query).first()
        if not study:
            return permission
        study_id = study["acc"]
        permission.studyId = study_id

        if (
            obfuscation_code
            and obfuscation_code == study["obfuscationcode"]
            and StudyStatus(study["status"]) in (StudyStatus.INREVIEW, StudyStatus.INCURATION)
        ):
            permission.studyStatus = StudyStatus(study["status"]).name
            permission.obfuscationCode = study["obfuscationcode"]
            permission.view = True
            permission.edit = False
            permission.delete = False
            return permission

        anonymous_user = True
        try:
            user = None
            if user_token:
                user = UserService.get_instance().validate_user_has_submitter_or_super_user_role(
                    user_token
                )
            if user:
                anonymous_user = False
        except Exception:
            pass

        if anonymous_user:
            if StudyStatus(study["status"]) == StudyStatus.PUBLIC:
                permission.studyStatus = StudyStatus(study["status"]).name
                permission.view = True
                permission.edit = False
                permission.delete = False
                return permission
            else:
                return permission

        permission.userName = user["username"]
        base_query = db_session.query(User.id, User.username, User.role, User.status)
        query = base_query.join(Study, User.studies)
        owner = query.filter(
            Study.acc == study_id,
            User.apitoken == user["apitoken"],
            User.status == UserStatus.ACTIVE.value,
        ).first()

        if owner:
            permission.submitterOfStudy = True

        permission.studyStatus = StudyStatus(study["status"]).name
        permission.userRole = UserRole(user["role"]).name
        if UserRole(user["role"]) == UserRole.ROLE_SUPER_USER:
            permission.obfuscationCode = study["obfuscationcode"]
            permission.view = True
            permission.edit = True
            permission.delete = True
            return permission

        if owner:
            permission.obfuscationCode = study["obfuscationcode"]
            permission.view = True
            permission.edit = True
            permission.delete = True
            return permission
        else:
            if StudyStatus(study["status"]) == StudyStatus.PUBLIC:
                permission.view = True
                permission.edit = False
                permission.delete = False
                return permission
            else:
                return permission
