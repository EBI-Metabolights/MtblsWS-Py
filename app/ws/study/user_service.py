from typing import Optional

from flask import current_app as app
from sqlalchemy import func

from app.utils import MetabolightsException, MetabolightsAuthorizationException
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import SimplifiedUserModel
from app.ws.db.schemes import Study, User
from app.ws.db.settings import get_directory_settings
from app.ws.db.types import UserStatus, UserRole
from app.ws.db.utils import datetime_to_int


class UserService(object):
    instance = None
    db_manager = None
    directory_settings = None

    @classmethod
    def get_instance(cls, application=None):
        if not cls.instance:
            cls.instance = UserService()
            if not application:
                application = app
            cls.db_manager = DBManager.get_instance(application)
            cls.directory_settings = get_directory_settings(application)
        return cls.instance

    def validate_user_has_write_access(self, user_token, study_id):

        try:
            with self.db_manager.session_maker() as db_session:
                base_query = db_session.query(User.id, User.username, User.role, User.status, User.apitoken)
                query = base_query.join(Study, User.studies)
                user = query.filter(Study.acc == study_id, User.apitoken == user_token).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(message=f"Error while retreiving user from database", exception=e)

        if user:
            return user
        with self.db_manager.session_maker() as db_session:
            study = db_session.query(Study.acc).filter(Study.acc == study_id).first()
            if study:
                return self.validate_user_has_curator_role(user_token)
            raise MetabolightsException(message=f"Not a valid study id")

    def validate_user_has_curator_role(self, user_token):
        return self.validate_user_by_token(user_token, [UserRole.ROLE_SUPER_USER.value])

    def validate_username_with_submitter_or_super_user_role(self, user_name):
        return self.validate_user_by_username(user_name,
                                              [UserRole.ROLE_SUBMITTER.value, UserRole.ROLE_SUPER_USER.value])

    def validate_user_has_submitter_or_super_user_role(self, user_token):
        return self.validate_user_by_token(user_token, [UserRole.ROLE_SUBMITTER.value, UserRole.ROLE_SUPER_USER.value])

    def validate_user_has_submitter_role(self, user_token):
        return self.validate_user_by_token(user_token, [UserRole.ROLE_SUBMITTER.value])

    def validate_user_by_username(self, user_name, allowed_role_list, allowed_status_list=None):
        if not user_name:
            raise MetabolightsException(message=f"User token is not valid")

        filter_clause = lambda query: query.filter(func.lower(User.username) == user_name.lower())

        return self.validate_user_by_user_field(filter_clause, allowed_role_list, allowed_status_list)

    def validate_user_by_token(self, user_token, allowed_role_list, allowed_status_list=None):
        if not user_token:
            raise MetabolightsException(message=f"User token is not valid")

        filter_clause = lambda query: query.filter(User.apitoken == user_token)

        return self.validate_user_by_user_field(filter_clause, allowed_role_list, allowed_status_list)

    def validate_user_by_user_field(self, filter_clause, allowed_role_list, allowed_status_list=None):

        if not allowed_role_list:
            raise MetabolightsAuthorizationException(message=f"Define user role to validate")

        if not allowed_status_list:
            allowed_status_list = [UserStatus.ACTIVE.value]

        try:
            with self.db_manager.session_maker() as db_session:
                query = db_session.query(User.id, User.username, User.role, User.status, User.apitoken, User.password)
                db_user = filter_clause(query).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(message=f"Error while retreiving user from database", exception=e)

        if db_user:
            if int(db_user.status) not in allowed_status_list:
                raise MetabolightsAuthorizationException(message=f"User status is not accepted")

            if db_user.role not in allowed_role_list:
                raise MetabolightsAuthorizationException(message=f"User role is not accepted")
            return db_user
        else:
            raise MetabolightsAuthorizationException(message=f"User not in database")

    def get_simplified_user_by_username(self, user_name) -> Optional[SimplifiedUserModel]:
        if not user_name:
            raise MetabolightsException(message=f"User token is not valid")

        filter_clause = lambda query: query.filter(User.username == user_name)

        return self.get_simplified_user_by_user_field(filter_clause)

    def get_simplified_user_by_token(self, user_token) -> Optional[SimplifiedUserModel]:
        if not user_token:
            raise MetabolightsException(message=f"User token is not valid")

        filter_clause = lambda query: query.filter(User.apitoken == user_token)

        return self.get_simplified_user_by_user_field(filter_clause)

    def get_simplified_user_by_user_field(self, filter_clause) -> Optional[SimplifiedUserModel]:
        try:
            with self.db_manager.session_maker() as db_session:
                query = db_session.query(User)
                db_user = filter_clause(query).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(message=f"Error while retreiving user from database", exception=e)

        if db_user:
            m_user = SimplifiedUserModel.from_orm(db_user)
            m_user.email = m_user.email.lower()
            m_user.userName = m_user.userName.lower()
            m_user.fullName = m_user.firstName + " " + m_user.lastName
            m_user.role = UserRole(m_user.role).name
            m_user.status = UserStatus(m_user.status).name
            m_user.joinDate = datetime_to_int(m_user.joinDate)
            return m_user
        else:
            raise MetabolightsAuthorizationException(message=f"User not in database")
