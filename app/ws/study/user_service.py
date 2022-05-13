from flask import current_app as app

from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.db.settings import get_database_settings, get_directory_settings
from app.ws.db.types import UserStatus, UserRole, StudyStatus, MetabolightsDBException, MetabolightsException, \
    MetabolightsAuthorizationException
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory


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
        return self.validate_user_by_token(user_token, [UserRole.CURATOR.value])

    def validate_user_has_submitter_or_super_user_role(self, user_token):
        return self.validate_user_by_token(user_token, [UserRole.SUBMITTER.value, UserRole.CURATOR.value])

    def validate_user_has_submitter_role(self, user_token):
        return self.validate_user_by_token(user_token, [UserRole.SUBMITTER.value])

    def validate_user_by_username(self, user_name, allowed_role_list, allowed_status_list=None):
        if not user_name:
            raise MetabolightsException(message=f"User token is not valid")

        filter_clause = lambda query: query.filter(User.username == user_name)

        return self.validate_user_by_user_field(filter_clause, allowed_role_list, allowed_status_list)

    def validate_user_by_token(self, user_token, allowed_role_list, allowed_status_list=None):
        if not user_token:
            raise MetabolightsException(message=f"User token is not valid")

        filter_clause = lambda query: query.filter(User.apitoken == user_token)

        return self.validate_user_by_user_field(filter_clause, allowed_role_list, allowed_status_list)

    def validate_user_by_user_field(self, filter_clause, allowed_role_list, allowed_status_list=None):

            if not allowed_role_list:
                raise MetabolightsAuthorizationException (message=f"Define user role to validate")

            if not allowed_status_list:
                allowed_status_list = [UserStatus.ACTIVE.value]

            try:
                with self.db_manager.session_maker() as db_session:
                    query = db_session.query(User.id, User.username, User.role, User.status, User.apitoken)
                    db_user = filter_clause(query).first()
            except Exception as e:
                raise MetabolightsAuthorizationException(message=f"Error while retreiving user from database", exception=e)

            if db_user:
                if db_user.status not in allowed_status_list:
                    raise MetabolightsAuthorizationException(message=f"User role is not accepted")

                if db_user.role not in allowed_role_list:
                    raise MetabolightsAuthorizationException(message=f"User status is not accepted")
                return db_user
            else:
                raise MetabolightsAuthorizationException(message=f"User not in database")