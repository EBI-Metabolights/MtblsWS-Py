import glob
import json
import os.path
import logging
from pathlib import Path
import re
import shutil

from app.config import get_settings
from app.tasks.common_tasks.basic_tasks.email import send_email_on_public
from app.ws.db.models import StudyRevisionModel
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
import datetime
from typing import List, Union
from app.utils import MetabolightsException, current_time, current_utc_time_without_timezone
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, StudyRevision
from app.ws.db.types import StudyRevisionStatus, StudyStatus
from app.ws.isaApiClient import IsaApiClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_revision_service import StudyRevisionService
from app.ws.study.study_service import StudyService
from isatools import model

from app.ws.study.user_service import UserService


logger = logging.getLogger("wslog")

def prepare_revisions():
        completed_at = current_time()
        user: User = None
        with DBManager.get_instance().session_maker() as db_session:
            try:
                # db_session.query(StudyRevision).delete()
                # db_session.commit()
                result = db_session.query(Study.acc, Study.revision_number, Study.revision_datetime, Study.status).all()
                studies = []
                if result:
                    studies = list(result)
                    studies.sort(key=lambda x: int(x["acc"].replace("MTBLS", "").replace("REQ", "")))
                
                audit_folder_root_path = get_settings().study.mounted_paths.study_audit_files_root_path
                user_token = get_settings().auth.service_account.api_token
                user = UserService.get_instance().get_db_user_by_user_token(user_token) 
   
            except Exception as e:
                db_session.rollback()
                raise e
        studies = [x for x in studies if int(x["acc"].replace("MTBLS", "").replace("REQ", "")) > 998]
        for study in studies:
            study_id = study["acc"]
            
            print(study_id)
            audit_root_path = os.path.join(audit_folder_root_path, study_id, "audit")
            latest_revision = 0
            revisions_root_path = os.path.join(audit_root_path, "PUBLIC_METADATA")
            metadata_revisions_path = os.path.join(revisions_root_path, "METADATA_REVISIONS")
            
            public_version_1_folder_path = os.path.join(audit_root_path , "PUBLIC_VERSION_1.0")
            public_version_2_folder_path = os.path.join(audit_root_path , "PUBLIC_VERSION_2.0")
            with DBManager.get_instance().session_maker() as db_session:
                try:

                    
                    db_study: Study = db_session.query(Study).filter(Study.acc == study_id).first()
                    study_status = StudyStatus(db_study.status)
                    if study_status == StudyStatus.PUBLIC:
                        # settings = get_settings().study
                        # audit_folder_root_path = settings.mounted_paths.study_audit_files_root_path
                        folder_name = "PUBLIC_VERSION_1.0"
                        if os.path.exists(public_version_1_folder_path):
                            folder_name = "PUBLIC_VERSION_2.0"
                            
                        audit_folder_path = os.path.join(audit_root_path, folder_name)
                        if not os.path.exists(audit_folder_path):
                            StudyRevisionService.create_audit_folder(db_study, folder_name=folder_name)
                    
                    if os.path.exists(revisions_root_path):
                        shutil.rmtree(revisions_root_path)
                    
                    for idx, audit_path in enumerate([public_version_1_folder_path, public_version_2_folder_path]):
                        if os.path.exists(audit_path):
                            hashes_path = os.path.join(audit_path, "HASHES")
                            os.makedirs(hashes_path, exist_ok=True)
                            search_pattern = os.path.join(audit_path, "*")
                            for file in glob.glob(search_pattern, recursive=False):
                                file_path = Path(file)
                                if file_path.name.startswith("metadata_sha256"):
                                    target_file = os.path.join(hashes_path, "metadata_sha256.json")
                                    shutil.move(file, target_file)
                                if file_path.name.startswith("data_sha256"):
                                    target_file = os.path.join(hashes_path, "data_sha256.json")
                                    shutil.move(file, target_file)
                                elif file_path.name.endswith("_signature.txt"):
                                        target_file = os.path.join(hashes_path, file_path.name)
                                        shutil.move(file, target_file)
                            revision_folder_name = f"{study_id}_{(idx + 1):02d}"
                            revision_path = os.path.join(metadata_revisions_path, revision_folder_name)
                            os.makedirs(metadata_revisions_path, exist_ok=True)
                            shutil.copytree(audit_path, revision_path)
                        
                    if os.path.exists(public_version_1_folder_path):
                        db_study.revision_datetime = db_study.first_public_date
                        db_study.revision_number = 1
                        query = db_session.query(StudyRevision)
                        study_revision: StudyRevision = query.filter(StudyRevision.accession_number == study_id, 
                                                                    StudyRevision.revision_number == db_study.revision_number).first()
                        if not study_revision:
                            revision = StudyRevision(accession_number=study_id, 
                                                revision_number=db_study.revision_number,
                                                revision_datetime=db_study.revision_datetime,
                                                revision_comment="Initial revision.",
                                                created_by=user.email,
                                                status=StudyRevisionStatus.COMPLETED.value,
                                                task_started_at=completed_at,
                                                task_completed_at=completed_at,
                                                task_message="Bulk update to enable study revisions."
                                                )
                            db_session.add(revision)
                        else:
                            study_revision.revision_datetime = db_study.revision_datetime
                            study_revision.revision_comment = "Initial study revision."
                            study_revision.created_by = user.email
                            study_revision.task_started_at = completed_at
                            study_revision.task_completed_at = completed_at
                            study_revision.status = StudyRevisionStatus.COMPLETED.value
                            study_revision.task_message = "Bulk update to enable study revisions."
                    if os.path.exists(public_version_2_folder_path):
                        db_study.revision_datetime = completed_at
                        db_study.revision_number = 2
                        query = db_session.query(StudyRevision)
                        study_revision: StudyRevision = query.filter(StudyRevision.accession_number == study_id, 
                                                                    StudyRevision.revision_number == db_study.revision_number).first()
                        if not study_revision:
                            revision = StudyRevision(accession_number=study_id, 
                                                revision_number=db_study.revision_number,
                                                revision_datetime=db_study.revision_datetime,
                                                revision_comment="Updated revision.",
                                                created_by=user.email,
                                                status=StudyRevisionStatus.COMPLETED.value,
                                                task_started_at=completed_at,
                                                task_completed_at=completed_at,
                                                task_message="Bulk update to enable study revisions."
                                                )
                            db_session.add(revision)
                        else:
                            study_revision.revision_datetime = db_study.revision_datetime
                            study_revision.revision_comment = "Updated study revision."
                            study_revision.created_by = user.email
                            study_revision.task_started_at = completed_at
                            study_revision.task_completed_at = completed_at
                            study_revision.status = StudyRevisionStatus.COMPLETED.value
                            study_revision.task_message = "Bulk update to enable study revisions."
                            
                    if not os.path.exists(public_version_1_folder_path) and not os.path.exists(public_version_2_folder_path):
                        db_study.revision_datetime = None
                        db_study.revision_number = 0
                        study_revision: StudyRevision = query.filter(StudyRevision.accession_number == study_id, 
                                                                    StudyRevision.revision_number == db_study.revision_number).first()
                        if study_revision:
                            db_session.delete(study_revision)
                    
                    query.filter(StudyRevision.accession_number == study_id, StudyRevision.revision_number > db_study.revision_number).delete()
                    latest_revision = db_study.revision_number
                    db_session.commit()
                    
                except Exception as e:
                    db_session.rollback()
                    raise e
            
            with DBManager.get_instance().session_maker() as db_session:
                try:
                    db_study: Study = db_session.query(Study).filter(Study.acc == study_id).first()
                    study_status = StudyStatus(db_study.status)
                    if study_status == StudyStatus.PUBLIC:
                        
                        
                        revision  = StudyRevisionService.increment_study_revision(study_id, revision_comment="Revision enabled study metadata.", created_by=user.email)
                        
                        StudyRevisionService.update_investigation_file_for_revision(study_id)
                        db_session.refresh(db_study)
                        latest_revision = db_study.revision_number
                        folder_status, source_path, created_path = StudyRevisionService.create_revision_folder(db_study)
                        # data_files_root_path = settings.mounted_paths.study_readonly_files_actual_root_path 
                        # study_data_files_path = os.path.join(data_files_root_path, study_id)
                        
                        
                        # revisions_root_hash_path = os.path.join(audit_folder_root_path, study_id, "audit", "PUBLIC_METADATA", "HASHES")
                        # StudyRevisionService.create_data_file_hashes(db_study, search_path=study_data_files_path, copy_paths=[revisions_root_hash_path])
                        

                        # StudyRevisionService.check_dataset_integrity(study_id, metadata_files_path=created_path, data_files_path=study_data_files_path)
                    print(created_path)
                    if os.path.exists(revisions_root_path):
                        files = os.listdir(revisions_root_path)
                        if not files:
                            shutil.rmtree(revisions_root_path)

                except Exception as e:
                    db_session.rollback()
                    raise e
            
            revision_folder_name = f"{study_id}_{latest_revision:02}"   
            latest_revision_path = os.path.join(metadata_revisions_path, revision_folder_name)
            search_pattern = os.path.join(latest_revision_path, "*")
            for file in glob.glob(search_pattern, recursive=False):
                file_path = Path(file)
                if re.match(r"[asim]_.*\.t*", file_path.name):
                    target_file = os.path.join(revisions_root_path, file_path.name)
                    if os.path.exists(target_file):
                        os.unlink(target_file)
                    shutil.copy2(file, target_file)
                elif file_path.name == "HASHES":
                    target_file = os.path.join(revisions_root_path, file_path.name)
                    if os.path.exists(target_file):
                        shutil.rmtree(target_file)
                    shutil.copytree(file, target_file)
            print(f"{study_id} ended.")

if __name__ == "__main__":
    prepare_revisions()