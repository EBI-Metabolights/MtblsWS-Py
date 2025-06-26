import glob
import logging
import os
from sqlalchemy import or_
from typing import Union
from app.config import get_settings
from app.utils import MetabolightsDBException, MetabolightsFileOperationException, MetabolightsException
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyModel
from app.ws.db.schemes import Study, User, Stableid, StudyTask
from app.ws.db.types import UserStatus, UserRole, StudyStatus
from app.ws.db.wrappers import create_study_model_from_db_study, get_user_model, update_study_model_from_directory
from app.ws.utils import read_tsv_with_filter, totuples

logger = logging.getLogger('wslog')

def identify_study_id(study_id: str, obfuscation_code: Union[None, str] = None):
    if study_id.lower().startswith("reviewer"):
        obfuscation_code = study_id.lower().replace("reviewer", "")
        study: Study = StudyService.get_instance().get_study_by_obfuscation_code(obfuscation_code)
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
    def get_instance(cls):
        if not cls.instance:
            cls.instance = StudyService()
            cls.study_settings = get_settings().study
        return cls.instance

    def get_study_by_acc(self, study_id) -> Study:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(Study)
                result = query.filter(Study.acc == study_id).first()
                if result:
                    return result
                raise MetabolightsDBException("DB error while retrieving stable id")
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)

    def get_study_by_req_or_mtbls_id(self, identifier) -> Study:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(Study)
                result = query.filter( or_(Study.reserved_accession == identifier, Study.reserved_submission_id == identifier)).first()
                if result:
                    return result
                raise MetabolightsDBException("DB error while retrieving stable id")
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)


    def get_study_by_obfuscation_code(self, obfuscationcode) -> Study:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(Study)
                result = query.filter(Study.obfuscationcode == obfuscationcode).first()
                if result:
                    return result
                raise MetabolightsDBException("DB error while retrieving stable id")
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)

    def get_next_stable_study_id(self):
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(Stableid.seq)
                result = query.filter(Stableid.prefix == "MTBLS").first()
                if result:
                    return result.seq
                raise MetabolightsDBException("DB error while retrieving stable id")
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)

    def get_study_from_db_and_folder(
        self, study_id, user_token, optimize_for_es_indexing=False, revalidate_study=True, include_maf_files=False
    ) -> StudyModel:
        try:
            with DBManager.get_instance().session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == study_id).first()
                if not db_study_obj:
                    raise MetabolightsDBException(message=f"Study {study_id} is not in database")
                m_study = create_study_model_from_db_study(db_study_obj)
        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study from database: {str(e)}", exception=e)

        try:
            update_study_model_from_directory(
                m_study,
                self.study_settings.mounted_paths.study_metadata_files_root_path,
                optimize_for_es_indexing=optimize_for_es_indexing,
                revalidate_study=revalidate_study,
                user_token_to_revalidate=user_token,
                include_maf_files=include_maf_files,
            )
        except Exception as e:
            raise MetabolightsFileOperationException(message=f"Error while reading study folder.", exception=e)

        return m_study

    def get_public_study_from_db(self, study_id) -> StudyModel:
         with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(Study.status == StudyStatus.PUBLIC.value, Study.acc == study_id)
            study = query.first()
            if not study:
                raise MetabolightsDBException(f"{study_id} does not exist or is not public")
            m_study = create_study_model_from_db_study(study)
            return m_study
        
    def get_public_study_with_detailed_user(self, study_id) -> StudyModel:
         with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(Study.status == StudyStatus.PUBLIC.value, Study.acc == study_id)
            study = query.first()
            if not study:
                raise MetabolightsDBException(f"{study_id} does not exist or is not public")
            m_study = create_study_model_from_db_study(study)
            m_study.users = [get_user_model(x) for x in study.users]
            return m_study
        
    def get_study_maf_rows(self, study_id: Union[None, str], sheet_number: Union[None, int]):
        if study_id is None or sheet_number is None:
            raise MetabolightsDBException("StudyId and sheet number needs to be passed")
        study_id = study_id.upper()

        with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(Study.status == StudyStatus.PUBLIC.value,
                                 Study.acc == study_id)
            study = query.first()

            if not study:
                raise MetabolightsDBException(f"{study_id} does not exist or is not public")
        study_path = get_settings().study.mounted_paths.study_metadata_files_root_path
        study_location = os.path.join(study_path, study_id)
        maflist = []

        for maf in glob.glob(os.path.join(study_location, "m_*.tsv")):
            maf_file_name = os.path.basename(maf)
            maflist.append(maf_file_name)
            
        try:
            maf_index = sheet_number-1
            df_data_dict = {}
            if maf_index < len(maflist):
                maf_file = maflist[sheet_number-1]
                maf_file_path = os.path.join(study_location, maf_file)
                file_df = read_tsv_with_filter(maf_file_path)
                df_data_dict = totuples(file_df.reset_index(), 'rows')
                return df_data_dict
            else:
                df_data_dict['rows'] = None
                return df_data_dict
        except FileNotFoundError:
            raise MetabolightsDBException(f"{maf_file_path} MAF not found")

    def get_all_authorized_study_ids(self, user_token):
        with DBManager.get_instance().session_maker() as db_session:
            db_user = db_session.query(User).filter(User.apitoken == user_token).first()

            if db_user and int(db_user.status) == UserStatus.ACTIVE.value:
                if UserRole(db_user.role) == UserRole.ROLE_SUPER_USER:
                    study_id_list = db_session.query(Study.acc).order_by(Study.submissiondate).first()
                else:
                    study_id_list = (
                        db_session.query(Study.acc)
                        .filter(Study.status == StudyStatus.PUBLIC.value)
                        .order_by(Study.submissiondate)
                        .first()
                    )

                    own_study_id_list = [x.acc for x in db_user.studies if x.status != StudyStatus.PUBLIC.value]
                    study_id_list.extend(own_study_id_list)
                return study_id_list

        return []

    def get_all_study_ids(self):
        with DBManager.get_instance().session_maker() as db_session:
            study_id_list = db_session.query(Study.acc).all()
            return study_id_list

    def get_study_ids_with_status(self, status: StudyStatus):
        with DBManager.get_instance().session_maker() as db_session:
            study_id_list = db_session.query(Study.acc).filter(Study.status == status.value).all()
            return study_id_list
        
    def get_study_tasks(self, study_id, task_name=None):
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(StudyTask)
                if task_name:
                    filtered = query.filter(StudyTask.study_acc == study_id, StudyTask.task_name == task_name)
                else:
                    filtered = query.filter(StudyTask.study_acc == study_id)
                result = filtered.all()
                return result
        except Exception as e:
            raise MetabolightsDBException(
                message=f"Error while retreiving study tasks from database: {str(e)}", exception=e
            )
