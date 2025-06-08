from typing import List, Optional, Union

from sqlalchemy import func

from app.utils import MetabolightsException, MetabolightsAuthorizationException
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import SimplifiedUserModel, UserModel
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus, UserStatus, UserRole
from app.ws.db.utils import datetime_to_int
from app.ws.settings.utils import get_study_settings


class UserService(object):
    instance = None
    db_manager = None
    study_settings = None

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            cls.instance = UserService()
            cls.study_settings = get_study_settings()
        return cls.instance
    
    def get_user_studies(self, user_token):
        try:
            with DBManager.get_instance().session_maker() as db_session:
                base_query = db_session.query(Study)
                query = base_query.join(User, Study.users)
                studies = query.filter(User.apitoken == user_token).all()
                return studies
        except Exception as e:
            raise MetabolightsAuthorizationException(message="Error while retreiving user from database", exception=e)

    
    def get_study_submitters(self, study_id):
        try:
            with DBManager.get_instance().session_maker() as db_session:
                base_query = db_session.query(User)
                query = base_query.join(Study, User.studies)
                submitters = query.filter(Study.acc == study_id).all()
                return submitters
        except Exception as e:
            raise MetabolightsAuthorizationException(message="Error while retreiving submittes from database", exception=e)
        
    def validate_user_has_write_access(self, user_token, study_id):

        try:
            with DBManager.get_instance().session_maker() as db_session:
                base_query = db_session.query(User.id, User.username, User.role, User.status, User.apitoken, User.partner)
                query = base_query.join(Study, User.studies)
                user = query.filter(Study.acc == study_id, User.apitoken == user_token,
                                    User.status == UserStatus.ACTIVE.value).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(message="Error while retreiving user from database", exception=e)

        if user:
            return user
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
                base_query = db_session.query(Study.acc, Study.status, Study.obfuscationcode)
                study = base_query.filter(Study.acc == study_id).first()
                if not study:
                    raise MetabolightsAuthorizationException(message="Not a valid study id")
                else:
                    if study[1] == StudyStatus.PUBLIC.value:
                        return True
                    else:
                        if obfuscationcode:
                            if study[2] == obfuscationcode and study[1] in (StudyStatus.INREVIEW.value, StudyStatus.PRIVATE.value):
                                return True
                            if study[2] != obfuscationcode:
                                raise MetabolightsAuthorizationException(message="Not a valid study id or obfuscation code")
                    
        except Exception as e:
            raise MetabolightsAuthorizationException(message="Error while retreiving user from database", exception=e)        
        self.validate_user_has_write_access(user_token, study_id)
        return True
        

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
            raise MetabolightsException(message=f"Invalid user or credential")

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
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(User.id, User.username, User.role, User.status, User.apitoken, User.password, User.partner)
                db_user = filter_clause(query).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(message=f"Invalid user or credential", exception=e)

        if db_user:
            if int(db_user.status) not in allowed_status_list:
                raise MetabolightsAuthorizationException(message=f"Invalid user status")

            if db_user.role not in allowed_role_list:
                raise MetabolightsAuthorizationException(message=f"Invalid user role")
            return db_user
        else:
            raise MetabolightsAuthorizationException(message=f"Invalid user or credential")

    def get_simplified_user_by_username(self, user_name) -> Union[None, SimplifiedUserModel]:
        if not user_name:
            raise MetabolightsException(message=f"User token is not valid")

        filter_clause = lambda query: query.filter(User.username == user_name)

        return self.get_simplified_user_by_user_field(filter_clause)

    def get_simplified_user_by_token(self, user_token) -> Union[None, SimplifiedUserModel]:
        if not user_token:
            raise MetabolightsException(message=f"User token is not valid")

        filter_clause = lambda query: query.filter(User.apitoken == user_token)

        return self.get_simplified_user_by_user_field(filter_clause)

    def get_simplified_user_by_user_field(self, filter_clause) -> Union[None, SimplifiedUserModel]:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(User)
                db_user: User = filter_clause(query).first()
        except Exception as e:
            raise MetabolightsAuthorizationException(message=f"Error while retreiving user from database", exception=e)

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
            raise MetabolightsAuthorizationException(message=f"User not in database")

    def get_db_user_by_user_token(self, user_token: str) -> Union[None, User]:
        filter_clause = lambda query: query.filter(User.apitoken == user_token)
        return  self.get_db_user_by_filter_clause(filter_clause=filter_clause)


    def get_db_user_by_user_name(self, user_name: str) -> Union[None, UserModel]:
        filter_clause = lambda query: query.filter(User.username == user_name)
        m_user =  self.get_db_user_by_filter_clause(filter_clause=filter_clause)
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
            raise MetabolightsAuthorizationException(message=f"Error while retreiving user from database", exception=e)

        if db_user:
            m_user = UserModel.model_validate(db_user)
            return m_user
        else:
            raise MetabolightsAuthorizationException(message=f"User not in database")
        

    def get_db_users_by_filter_clause(self, filter_clause=None) -> List[UserModel]:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(User)
                if filter_clause:
                    db_users = filter_clause(query).all()
                else:
                    db_users = query.all()
        except Exception as e:
            raise MetabolightsAuthorizationException(message=f"Error while retreiving user from database", exception=e)
        users: List[UserModel] = []
        if db_users:
            for db_user in db_users:
                m_user = UserModel.model_validate(db_user)
                users.append(m_user)
            
        return users