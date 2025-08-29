import glob
import json
import os.path
import logging
from pathlib import Path
import shutil

from app.tasks.common_tasks.basic_tasks.elasticsearch import reindex_study
from app.tasks.common_tasks.basic_tasks.send_email import send_email_on_public
from app.tasks.datamover_tasks.basic_tasks.ftp_operations import (
    sync_private_ftp_data_files,
)
from app.ws.db.models import StudyRevisionModel
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
import datetime
from typing import List, Union
from app.utils import (
    MetabolightsException,
    current_time,
    current_utc_time_without_timezone,
)
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, StudyRevision
from app.ws.db.types import StudyRevisionStatus, StudyStatus
from app.ws.isaApiClient import IsaApiClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService
from isatools import model


logger = logging.getLogger("wslog")


class StudyRevisionService:
    @staticmethod
    def check_dataset_integrity(
        study_id: str, metadata_files_path: str, data_files_path: str
    ):
        study = StudyService.get_instance().get_study_by_acc(study_id)
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
        metadata_signature = Path(
            f"{metadata_files_path}/HASHES/metadata_files_signature.txt"
        )
        if metadata_signature.exists():
            expected_metadata_signature = (
                Path(f"{metadata_files_path}/HASHES/metadata_files_signature.txt")
                .read_text()
                .strip()
            )
            metadata_files_signature, metadata_files_hashes = (
                maintenance_task.calculate_metadata_files_hash(
                    search_path=metadata_files_path
                )
            )
            if metadata_files_signature != expected_metadata_signature:
                message = f"Signature of study {study_id} revision {study.revision_number} metadata files does not match: current: {metadata_files_signature} != expected: {expected_metadata_signature}"
                logger.error(message)
                return False
            else:
                message = f"Signature of study {study_id} revision {study.revision_number} metadata files matches: {metadata_files_signature}"
                logger.info(message)
        else:
            logger.warning(
                f"metadata file signature file does not exist: {metadata_signature}"
            )

        data_files_signature = Path(
            f"{metadata_files_path}/HASHES/data_files_signature.txt"
        )
        if data_files_signature.exists():
            expected_data_file_signature = (
                Path(f"{metadata_files_path}/HASHES/data_files_signature.txt")
                .read_text()
                .strip()
            )
            data_files_signature, data_files_hashes, _ = (
                StudyRevisionService.create_data_file_hashes(
                    study, search_path=data_files_path
                )
            )

            if data_files_signature != expected_data_file_signature:
                message = f"Signature of study {study_id} revision {study.revision_number} data files does not match: current: {data_files_signature} != expected: {expected_data_file_signature}"
                logger.error(message)

                return False
            else:
                message = f"Signature of study {study_id} revision {study.revision_number} data files matches: {data_files_signature}"
                logger.info(message)
        else:
            logger.warning(
                f"data files signature file does not exist: {data_files_signature}"
            )

    @staticmethod
    def create_data_file_hashes(
        study: Study,
        data_files_hashes_file: str = None,
        data_files_signature_file: str = None,
        search_path=None,
        copy_paths: Union[None, List[str]] = None,
    ):
        """
        Create revision folder for study
        """
        try:
            study_id = study.acc
            study_status = StudyStatus(study.status)
            public_release_date = study.releasedate
            submission_date = study.submissiondate
            revision_number = study.revision_number
            maintenance_task = StudyFolderMaintenanceTask(
                study_id,
                study_status,
                public_release_date,
                submission_date,
                obfuscationcode=study.obfuscationcode,
                task_name=None,
                cluster_execution_mode=False,
            )
            revisions_root_path = os.path.join(
                maintenance_task.study_internal_files_path, "PUBLIC_METADATA"
            )
            revision_folder_name = f"{study_id}_{revision_number:02d}"
            metadata_revisions_path = os.path.join(
                revisions_root_path, "METADATA_REVISIONS"
            )
            revision_folder_path = os.path.join(
                metadata_revisions_path, revision_folder_name
            )
            if data_files_hashes_file or data_files_signature_file:
                if not os.path.exists(revision_folder_path):
                    raise Exception(
                        f"Revision folder does not exists: {revision_folder_name}"
                    )
            parent = os.path.join(
                metadata_revisions_path, revision_folder_name, "HASHES"
            )
            os.makedirs(parent, exist_ok=True)
            if not data_files_hashes_file:
                data_files_hashes_file = os.path.join(parent, "data_sha256.json")
            if not data_files_signature_file:
                data_files_signature_file = os.path.join(
                    parent, "data_files_signature.txt"
                )
            logger.info("Calculate data file hashes.")
            folder_hash, data_file_hashes, search_path = (
                maintenance_task.calculate_data_file_hashes(search_path=search_path)
            )

            with open(data_files_signature_file, "w") as f:
                f.write(f"{folder_hash}")

            with open(data_files_hashes_file, "w") as f:
                f.write(f"{json.dumps(data_file_hashes, indent=4)}")
            if copy_paths:
                for copy_path in copy_paths:
                    data_files_hashes_file = os.path.join(copy_path, "data_sha256.json")
                    data_files_signature_file = os.path.join(
                        copy_path, "data_files_signature.txt"
                    )
                    with open(data_files_signature_file, "w") as f:
                        f.write(f"{folder_hash}")

                    with open(data_files_hashes_file, "w") as f:
                        f.write(f"{json.dumps(data_file_hashes, indent=4)}")
            logger.info("Data file hashes are calculated.")
            return folder_hash, data_file_hashes, search_path
        except Exception as ex:
            raise ex

    def create_audit_folder(
        study: Study,
        folder_name: str,
        metadata_files_path: str = None,
        audit_folder_root_path: str = None,
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
    def update_investigation_file_for_revision(study_id: str):
        study = StudyService.get_instance().get_study_by_acc(study_id)
        revision = StudyRevisionService.get_study_revision(
            study_id=study.acc, revision_number=study.revision_number
        )
        iac = IsaApiClient()

        study_metadata_location = os.path.join(
            get_study_settings().mounted_paths.study_metadata_files_root_path, study_id
        )
        isa_study_input, isa_inv_input, std_path = iac.get_isa_study(
            study_id=study_id,
            api_key=None,
            skip_load_tables=True,
            study_location=study_metadata_location,
        )
        isa_study: model.Study = isa_study_input
        # if study.revision_number > 0:
        isa_study.identifier = study.acc
        isa_inv_input.identifier = study.acc
        if not study.first_private_date:
            submission_date = study.first_private_date.strftime("%Y-%m-%d")
            isa_study.submission_date = submission_date
            isa_inv_input.submission_date = submission_date
        else:
            submission_date = study.submissiondate.strftime("%Y-%m-%d")
            isa_study.submission_date = submission_date
            isa_inv_input.submission_date = submission_date
        isa_study.public_release_date = study.first_public_date.strftime("%Y-%m-%d")
        isa_inv_input.public_release_date = study.first_public_date.strftime("%Y-%m-%d")
        # else:
        #     isa_study.submission_date = study.submissiondate.strftime("%Y-%m-%d")
        #     isa_study.public_release_date = study.releasedate.strftime("%Y-%m-%d")
        #     isa_inv_input.submission_date = study.submissiondate.strftime("%Y-%m-%d")
        #     isa_inv_input.public_release_date = study.releasedate.strftime("%Y-%m-%d")

        revision_comments = [
            c
            for c in isa_study.comments
            if c.name.strip().lower() in {"revision", "dataset revision"}
        ]
        revision_datetimes = [
            c
            for c in isa_study.comments
            if c.name.strip().lower() in {"revision date", "dataset revision date"}
        ]
        revision_logs = [
            c
            for c in isa_study.comments
            if c.name.strip().lower() in {"revision log", "dataset revision log"}
        ]
        revision_comments.extend(revision_datetimes)
        revision_comments.extend(revision_logs)

        if study.revision_number > 0:
            comment = model.Comment(name="Revision", value=str(study.revision_number))
            isa_study.comments.append(comment)
            revision_datetime = ""
            if study.revision_datetime:
                revision_datetime = study.revision_datetime.strftime("%Y-%m-%d")
            comment = model.Comment(name="Revision Date", value=revision_datetime)
            isa_study.comments.append(comment)
            log = revision.revision_comment or ""
            log = log.strip().replace("\t", " ").replace("\n", " ")
            comment = model.Comment(name="Revision Log", value=log)
            isa_study.comments.append(comment)

        for comment in revision_comments:
            isa_study.comments.remove(comment)

        StudyRevisionService.update_license(study, isa_study)
        iac.write_isa_study(
            isa_inv_input, None, std_path, save_investigation_copy=False
        )

    @staticmethod
    def update_license(study: Study, isa_study: model.Study):
        license_name = study.dataset_license if study.dataset_license else ""
        updated_comments = []
        license_updated = False
        for comment in isa_study.comments:
            if comment.name.lower() != "license":
                updated_comments.append(comment)
            elif not license_updated:
                comment.value = license_name
                updated_comments.append(comment)
                license_updated = True
        if not license_updated:
            updated_comments.append(model.Comment(name="License", value=license_name))
        isa_study.comments = updated_comments

    @staticmethod
    def start_study_revision_task(
        study_id: str,
        revision_number: int,
        task_started_at: Union[None, datetime.datetime] = None,
        task_message: Union[None, str] = None,
    ):
        with DBManager.get_instance().session_maker() as db_session:
            try:
                query = db_session.query(StudyRevision)
                study_revision: StudyRevision = query.filter(
                    StudyRevision.accession_number == study_id,
                    StudyRevision.revision_number == revision_number,
                ).first()
                if not task_started_at:
                    task_started_at = current_time()
                study_revision.task_started_at = task_started_at
                study_revision.task_message = task_message
                study_revision.status = StudyRevisionStatus.IN_PROGRESS.value
                db_session.commit()
                logger.info(
                    f"New revision task started: ({study_id} Revision {revision_number}) "
                )
            except Exception as e:
                db_session.rollback()
                raise e

    @staticmethod
    def get_all_non_started_study_revision_tasks() -> list[StudyRevisionModel]:
        with DBManager.get_instance().session_maker() as db_session:
            try:
                ten_minutes_ago = datetime.datetime.now(
                    datetime.timezone.utc
                ) - datetime.timedelta(minutes=10)

                query = (
                    db_session.query(StudyRevision)
                    .join(Study, StudyRevision.accession_number == Study.acc)
                    .filter(
                        Study.revision_number == StudyRevision.revision_number,
                        StudyRevision.status in (StudyRevisionStatus.INITIATED.value, StudyRevisionStatus.FAILED.value,),
                        StudyRevision.revision_datetime <= ten_minutes_ago,
                    )
                )
                study_revisions: list[StudyRevision] = query.all()
                models = [
                    StudyRevisionModel.model_validate(
                        x, from_attributes=True
                    ) for x in study_revisions
                ]
                return models
                    
            except Exception as e:
                db_session.rollback()
                raise e
            finally:
                db_session.rollback()

    @staticmethod
    def update_study_revision_task_status(
        study_id: str,
        revision_number: int,
        status: StudyRevisionStatus,
        user_token: str,
        revision_comment: Union[None, str] = None,
        created_by: Union[None, str] = None,
        task_completed_at: Union[None, datetime.datetime] = None,
        task_started_at: Union[None, datetime.datetime] = None,
        task_message: Union[None, str] = None,
    ):
        with DBManager.get_instance().session_maker() as db_session:
            try:
                query = db_session.query(Study)
                study: Study = query.filter(Study.acc == study_id).first()
                study_status = StudyStatus(study.status)

                query = db_session.query(StudyRevision)
                study_revision: StudyRevision = query.filter(
                    StudyRevision.accession_number == study_id,
                    StudyRevision.revision_number == revision_number,
                ).first()
                if not study_revision:
                    raise MetabolightsException(
                        http_code=401, message="Revision not found."
                    )

                if study_revision.status == StudyRevisionStatus.COMPLETED.value:
                    if status != StudyRevisionStatus.COMPLETED:
                        raise MetabolightsException(
                            http_code=401, message="Revision task is already completed."
                        )

                study_revision.status = status.value
                if task_started_at:
                    study_revision.task_started_at = task_started_at
                if task_completed_at:
                    study_revision.task_completed_at = task_completed_at
                study_revision.task_message = task_message if task_message else None
                if revision_comment:
                    study_revision.revision_comment = revision_comment
                if created_by:
                    study_revision.created_by = created_by

                logger.info(
                    f"Revision task status updated : ({study_id} Revision {revision_number}), Status: {status.name}"
                )
                revision_model = StudyRevisionModel.model_validate(
                    study_revision, from_attributes=True
                )

                if status == StudyRevisionStatus.COMPLETED:
                    if (
                        study_status != StudyStatus.PUBLIC
                        and study.revision_number == study_revision.revision_number
                    ):
                        study.status = StudyStatus.PUBLIC.value
                        study.updatedate = task_completed_at.replace(
                            tzinfo=None
                        ) or current_time().replace(tzinfo=None)

                        sync_private_ftp_data_files(
                            study_id=study_id, obfuscation_code=study.obfuscationcode
                        )
                        if study.revision_number == 1:
                            inputs = {
                                "user_token": user_token,
                                "study_id": study_id,
                                "release_date": study.first_public_date.strftime(
                                    "%Y-%m-%d"
                                ),
                            }
                            send_email_on_public.apply_async(kwargs=inputs)

                db_session.commit()
                try:
                    inputs = {"user_token": user_token, "study_id": study_id}
                    reindex_task = reindex_study.apply_async(kwargs=inputs, expires=60)
                    logger.info(
                        "%s study reindex task started with task id %s",
                        study_id,
                        reindex_task.id,
                    )
                except Exception as e:
                    logger.error(f"Error while reindexing study {study_id}: {str(e)}")
                return revision_model
            except Exception as e:
                db_session.rollback()
                raise e

    @staticmethod
    def get_study_revisions(study_id: str):
        with DBManager.get_instance().session_maker() as db_session:
            try:
                query = db_session.query(StudyRevision)
                result = (
                    query.filter(StudyRevision.accession_number == study_id)
                    .order_by(StudyRevision.revision_number.desc())
                    .all()
                )
                revisions = []
                for item in result:
                    study_revision: StudyRevision = item
                    model = StudyRevisionModel.model_validate(
                        study_revision, from_attributes=True
                    )
                    revisions.append(model)
                return revisions
            except Exception as e:
                raise e

    @staticmethod
    def get_study_revision(study_id: str, revision_number: int):
        with DBManager.get_instance().session_maker() as db_session:
            try:
                query = db_session.query(StudyRevision)
                result = (
                    query.filter(
                        StudyRevision.accession_number == study_id,
                        StudyRevision.revision_number == revision_number,
                    )
                    .order_by(StudyRevision.revision_number.desc())
                    .first()
                )
                if result:
                    model = StudyRevisionModel.model_validate(
                        result, from_attributes=True
                    )
                    return model
                return None
            except Exception as e:
                raise e

    @staticmethod
    def increment_study_revision(
        study_id: str,
        revision_comment: str,
        created_by: str,
        revision_datetime: datetime.datetime = None,
        revision_status: StudyRevisionStatus = StudyRevisionStatus.INITIATED,
    ):
        with DBManager.get_instance().session_maker() as db_session:
            try:
                if not revision_datetime:
                    revision_datetime = current_time()

                query = db_session.query(Study)
                study: Study = query.filter(Study.acc == study_id).first()

                if study.status not in {
                    StudyStatus.PRIVATE.value,
                    StudyStatus.INREVIEW.value,
                    StudyStatus.PUBLIC.value,
                }:
                    raise MetabolightsException(
                        http_code=401, message="Study is not public"
                    )
                study.revision_number += 1
                study.revision_datetime = revision_datetime
                study.updatedate = revision_datetime.replace(tzinfo=None)
                if study.first_private_date:
                    study.submissiondate = study.first_private_date.replace(tzinfo=None)
                if not study.first_public_date:
                    study.first_public_date = revision_datetime
                    study.releasedate = revision_datetime.replace(tzinfo=None)

                revision = StudyRevision(
                    accession_number=study_id,
                    revision_number=study.revision_number,
                    revision_datetime=revision_datetime,
                    revision_comment=revision_comment,
                    created_by=created_by,
                    status=revision_status.value,
                )
                db_session.add(revision)
                db_session.commit()
                db_session.refresh(revision)
                es_service = ElasticsearchService.get_instance()
                es_service.reindex_study_with_task(
                    study_id=study_id,
                    user_token=None,
                    include_validation_results=False,
                    sync=False,
                )
                logger.info(
                    f"New revision (Revision {study.revision_number}) is created for study {study_id}"
                )
            except Exception as e:
                db_session.rollback()
                raise e
            revision_model = StudyRevisionModel.model_validate(
                revision, from_attributes=True
            )
            return revision_model

    @staticmethod
    def create_revision_folder(study: Study):
        """
        Create revision folder for study
        """
        try:
            study_id = study.acc
            study_status = StudyStatus(study.status)
            public_release_date = study.releasedate
            submission_date = study.submissiondate
            revision_number = study.revision_number
            maintenance_task = StudyFolderMaintenanceTask(
                study_id,
                study_status,
                public_release_date,
                submission_date,
                obfuscationcode=study.obfuscationcode,
                task_name=None,
                cluster_execution_mode=False,
            )
            revisions_root_path = os.path.join(
                maintenance_task.study_internal_files_path, "PUBLIC_METADATA"
            )
            revision_folder_name = f"{study_id}_{revision_number:02d}"
            metadata_revisions_path = os.path.join(
                revisions_root_path, "METADATA_REVISIONS"
            )
            revisions_path = os.path.join(metadata_revisions_path, revision_folder_name)
            if os.path.exists(revisions_path):
                shutil.rmtree(revisions_path)

            os.makedirs(metadata_revisions_path, exist_ok=True)

            dest_path = maintenance_task.create_audit_folder(
                audit_folder_root_path=metadata_revisions_path,
                folder_name=revision_folder_name,
                stage=None,
            )

            # delete previous version files on PUBLIC_METADATA top folder
            search_pattern = os.path.join(revisions_root_path, "*")
            for file in glob.glob(search_pattern, recursive=False):
                file_path = Path(file)
                if file_path.is_file():
                    file_path.unlink()
                elif file_path.name == "HASHES":
                    shutil.rmtree(file)
            # copy latest version files on to PUBLIC_METADATA top folder
            search_pattern = os.path.join(revisions_path, "*")
            for file in glob.glob(search_pattern, recursive=False):
                file_path = Path(file)
                if file_path.is_file():
                    basename = os.path.basename(file)
                    target_file = os.path.join(revisions_root_path, basename)
                    shutil.copy2(file, target_file)
                elif file_path.is_dir() and file_path.name == "HASHES":
                    target_file = os.path.join(revisions_root_path, file_path.name)
                    # os.makedirs(target_file, exist_ok=True)
                    if os.path.exists(target_file):
                        shutil.rmtree(target_file)
                    shutil.copytree(file, target_file)
            # mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
            # files_path = os.path.join(mounted_paths.cluster_study_readonly_files_actual_root_path, study_id)
            # revisions_files_path = os.path.join(revisions_root_path, "FILES")
            # if os.path.exists(revisions_files_path):
            #     os.unlink(revisions_files_path)
            # os.symlink(files_path, revisions_files_path)
            logger.info("Revision folder created: %s", dest_path)
            return True, maintenance_task.study_metadata_files_path, dest_path
        except Exception as ex:
            logger.error("Error in creating revision folder: %s", str(ex))
            return False, None, None

    @staticmethod
    def _delete_revision_folder(study: Study):
        """
        Create revision folder for study
        """
        try:
            study_id = study.acc
            study_status = StudyStatus(study.status)
            public_release_date = study.releasedate
            submission_date = study.submissiondate
            revision_number = study.revision_number
            maintenance_task = StudyFolderMaintenanceTask(
                study_id,
                study_status,
                public_release_date,
                submission_date,
                obfuscationcode=study.obfuscationcode,
                task_name=None,
                cluster_execution_mode=False,
            )
            revisions_root_path = os.path.join(
                maintenance_task.study_internal_files_path, "PUBLIC_METADATA"
            )
            metadata_revisions_root_path = os.path.join(
                revisions_root_path, "METADATA_REVISIONS"
            )
            revision_folder_name = f"{study_id}_{revision_number:02d}"

            revision_path = os.path.join(
                metadata_revisions_root_path, revision_folder_name
            )
            if not os.path.exists(revision_path) or not os.path.isdir(revision_path):
                return revision_path

            shutil.rmtree(revision_path)
            # delete previous version files on PUBLIC_METADATA top folder
            search_pattern = os.path.join(revisions_root_path, "*")
            for file in glob.glob(search_pattern, recursive=False):
                file_path = Path(file)
                if file_path.is_file():
                    file_path.unlink()
                elif file_path.is_dir() and file_path.name == "HASHES":
                    shutil.rmtree(file)

            if revision_number > 1:
                previous_revision_path = os.path.join(
                    metadata_revisions_root_path,
                    f"{study_id}_{(revision_number - 1):02d}",
                )
                if os.path.exists(previous_revision_path):
                    # copy latest version files on to PUBLIC_METADATA top folder
                    search_pattern = os.path.join(previous_revision_path, "*")
                    for file in glob.glob(search_pattern, recursive=False):
                        file_path = Path(file)
                        if file_path.is_file():
                            target_file = os.path.join(
                                revisions_root_path, file_path.name
                            )
                            shutil.copy2(file, target_file)
                        elif file_path.is_dir() and file_path.name == "HASHES":
                            target_file = os.path.join(
                                revisions_root_path, file_path.name
                            )
                            shutil.copytree(file, target_file)
            return revision_path
        except Exception as ex:
            logger.error("Error in creating revision folder: %s", str(ex))
            return None

    @staticmethod
    def delete_study_revision(study_id: str, revision_number: int):
        with DBManager.get_instance().session_maker() as db_session:
            try:
                query = db_session.query(Study)
                study: Study = query.filter(Study.acc == study_id).first()

                if int(study.revision_number) != revision_number:
                    raise MetabolightsException(
                        http_code=401,
                        message=f"Only the latest revision can be deleted. Current revision number: {study.revision_number}",
                    )

                query = db_session.query(StudyRevision)
                study_revision: StudyRevision = query.filter(
                    StudyRevision.accession_number == study_id,
                    StudyRevision.revision_number == revision_number,
                ).first()
                if study_revision.status == StudyRevisionStatus.IN_PROGRESS.value:
                    raise MetabolightsException(
                        http_code=401,
                        message="Revision task is in progress, cannot be deleted.",
                    )

                StudyRevisionService._delete_revision_folder(study)

                db_session.delete(study_revision)
                study.updatedate = current_utc_time_without_timezone()
                if revision_number == 1:
                    study.revision_number = 0
                    study.revision_datetime = None
                    study.releasedate = study.first_private_date.replace(
                        tzinfo=None
                    ) + datetime.timedelta(days=365)
                    if study.status != StudyStatus.PRIVATE.value:
                        study.status = StudyStatus.PRIVATE.value
                    ElasticsearchService.get_instance()._delete_study_index(
                        study_id=study_id, ignore_errors=True
                    )
                else:
                    previous_revision: StudyRevision = query.filter(
                        StudyRevision.accession_number == study_id,
                        StudyRevision.revision_number == (revision_number - 1),
                    ).first()
                    if previous_revision:
                        study.revision_number = previous_revision.revision_number
                        study.revision_datetime = previous_revision.revision_datetime
                    else:
                        study.revision_number -= 1

                db_session.commit()
                StudyRevisionService.update_investigation_file_for_revision(study_id)
                logger.info(
                    f"Study {study_id} revision ({study.revision_number + 1}) is deleted."
                )
            except Exception as e:
                db_session.rollback()
                raise e
            return StudyRevisionModel.model_validate(
                study_revision, from_attributes=True
            )


# if __name__ == "__main__":

#     study_id = "MTBLS20008905"
#     revision  = StudyRevisionService.increment_study_revision(study_id, revision_comment="Test revision", created_by="Test User")
#     study: Study = StudyService.get_instance().get_study_by_acc(study_id)
#     user_token = get_settings().auth.service_account.api_token
#     StudyRevisionService.update_investigation_file_for_revision(study_id)

#     status, source_path, created_path = StudyRevisionService.create_revision_folder(study)

#     data_files_root_path = get_settings().study.mounted_paths.study_readonly_files_actual_root_path
#     study_data_files_path = os.path.join(data_files_root_path, study_id)
#     study_internal_files_path = get_settings().study.mounted_paths.study_internal_files_path
#     revisions_root_path = os.path.join(study_internal_files_path, study_id, "PUBLIC_METADATA")
#     StudyRevisionService.create_data_file_hashes(study, search_path=study_data_files_path, copy_paths=[revisions_root_path])

#     now = current_time()
#     StudyRevisionService.start_study_revision_task(study.acc,
#                                                    revision_number=study.revision_number,
#                                                    task_started_at=now, task_message="FTP Sync task started")
#     StudyRevisionService.update_study_revision_task_status(study.acc,
#                                                            revision_number=study.revision_number,
#                                                            status=StudyRevisionStatus.FAILED,
#                                                            user_token=user_token,
#                                                            task_completed_at=now, task_message="FTP Sync task failed")

#     StudyRevisionService.update_study_revision_task_status(study.acc,
#                                                            revision_number=study.revision_number,
#                                                            status=StudyRevisionStatus.COMPLETED,
#                                                            user_token=user_token,
#                                                            task_completed_at=now, task_message="FTP Sync task completed")

#     StudyRevisionService.check_dataset_integrity(study_id, metadata_files_path=created_path, data_files_path=study_data_files_path)

#     StudyRevisionService.delete_study_revision(study_id, revision.revision_number)
