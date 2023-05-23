from flask import current_app as app

from app.utils import MetabolightsDBException, MetabolightsFileOperationException, MetabolightsException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User, Stableid, StudyTask
from app.ws.db.types import UserStatus, UserRole, StudyStatus
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory
from app.ws.settings.utils import get_study_settings


def identify_study_id(study_id, obfuscation_code=None):
    if study_id.lower().startswith("reviewer"):
        obfuscation_code = study_id.lower().replace("reviewer", "")
        study = StudyService.get_instance(app).get_study_by_obfuscation_code(obfuscation_code)
        if study and study.status == StudyStatus.INREVIEW.value:
            study_id = study.acc
            return study_id, obfuscation_code
        else:
            raise MetabolightsException(http_code=404, message="Requested study is not valid")
    return study_id, obfuscation_code

class StudyService(object):
    instance = None
    db_manager = None
    study_settings = None

    @classmethod
    def get_instance(cls, application=None):
        if not cls.instance:
            cls.instance = StudyService()
            if not application:
                application = app
            cls.db_manager = DBManager.get_instance(application)
            cls.study_settings = get_study_settings()
        return cls.instance

    def get_study_by_acc(self, study_id):
        try:
            with self.db_manager.session_maker() as db_session:
                query = db_session.query(Study)
                result = query.filter(Study.acc == study_id).first()
                if result:
                    return result
                raise MetabolightsDBException("DB error while retrieving stable id")
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)

    def get_study_by_obfuscation_code(self, obfuscationcode):
        try:
            with self.db_manager.session_maker() as db_session:
                query = db_session.query(Study)
                result = query.filter(Study.obfuscationcode == obfuscationcode).first()
                if result:
                    return result
                raise MetabolightsDBException("DB error while retrieving stable id")
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)
    def get_next_stable_study_id(self):
        try:
            with self.db_manager.session_maker() as db_session:
                query = db_session.query(Stableid.seq)
                result = query.filter(Stableid.prefix == "MTBLS").first()
                if result:
                    return result.seq
                raise MetabolightsDBException("DB error while retrieving stable id")
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)

    def get_study_from_db_and_folder(self, study_id, user_token, optimize_for_es_indexing=False, revalidate_study=True,
                                     include_maf_files=False):

        try:
            with self.db_manager.session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == study_id).first()
                if not db_study_obj:
                    raise MetabolightsDBException(message=f"Study {study_id} is not in database")
                m_study = create_study_model_from_db_study(db_study_obj)
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)

        try:
            update_study_model_from_directory(m_study, self.study_settings.study_metadata_files_root_path,
                                              optimize_for_es_indexing=optimize_for_es_indexing,
                                              revalidate_study=revalidate_study,
                                              user_token_to_revalidate=user_token,
                                              include_maf_files=include_maf_files)
        except Exception as e:
            raise MetabolightsFileOperationException(message=f"Error while reading study folder.",
                                                     exception=e)

        return m_study

    def get_all_authorized_study_ids(self, user_token):

        with self.db_manager.session_maker() as db_session:
            db_user = db_session.query(User).filter(User.apitoken == user_token).first()

            if db_user and int(db_user.status) == UserStatus.ACTIVE.value:
                if UserRole(db_user.role) == UserRole.ROLE_SUPER_USER:
                    study_id_list = db_session.query(Study.acc).order_by(Study.submissiondate).first()
                else:
                    study_id_list = db_session.query(Study.acc) \
                        .filter(Study.status == StudyStatus.PUBLIC.value) \
                        .order_by(Study.submissiondate).first()

                    own_study_id_list = [x.acc for x in db_user.studies if x.status != StudyStatus.PUBLIC.value]
                    study_id_list.extend(own_study_id_list)
                return study_id_list

        return []

    def get_study_tasks(self, study_id, task_name=None):
        try:
            with self.db_manager.session_maker() as db_session:
                query = db_session.query(StudyTask)
                if task_name:
                    filtered = query.filter(StudyTask.study_acc == study_id and StudyTask.task_name == task_name)
                else:
                    filtered = query.filter(StudyTask.study_acc == study_id)
                result = filtered.all()
                return result
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study tasks from database: {str(e)}", exception=e)
