import logging

from app.ws.folder_maintenance import StudyFolderMaintenanceTask
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus


logger = logging.getLogger("wslog")


class StudyFolderService:
    def create_audit_folder(
        study: Study,
        folder_name: None | str = None,
        metadata_files_path: None | str = None,
        audit_folder_root_path: None | str = None,
    ):
        study_id = study.acc
        study_status = StudyStatus(study.status)
        public_release_date = study.releasedate
        submission_date = study.submissiondate
        maintenance_task = StudyFolderMaintenanceTask(
            study_id,
            study_status,
            public_release_date,
            submission_date,
            obfuscationcode=study.obfuscationcode,
            task_name=None,
            cluster_execution_mode=False,
        )
        return maintenance_task.create_audit_folder(
            metadata_files_path=metadata_files_path,
            audit_folder_root_path=audit_folder_root_path,
            folder_name=folder_name,
            stage=None,
        )

    @staticmethod
    def create_audit_folder_with_study_id(study_id: str):
        with DBManager.get_instance().session_maker() as db_session:
            try:
                db_study: Study = (
                    db_session.query(Study).filter(Study.acc == study_id).first()
                )
                StudyFolderService.create_audit_folder(db_study)

            except Exception as ex:
                raise ex
