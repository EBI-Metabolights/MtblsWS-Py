import glob
import os.path
import logging
from pathlib import Path
import re
import shutil

from app.config import get_settings
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus
from app.ws.study.study_revision_service import StudyRevisionService

from app.ws.study.user_service import UserService


logger = logging.getLogger("wslog")


def prepare_revisions():
    user: User = None

    studies = []
    with DBManager.get_instance().session_maker() as db_session:
        try:
            # db_session.query(StudyRevision).delete()
            # db_session.commit()
            result = db_session.query(
                Study.acc, Study.revision_number, Study.revision_datetime, Study.status
            ).all()
            if result:
                studies = list(result)
                studies.sort(
                    key=lambda x: int(x["acc"].replace("MTBLS", "").replace("REQ", ""))
                )

            audit_folder_root_path = (
                get_settings().study.mounted_paths.study_audit_files_root_path
            )
            internal_files_root_path = (
                get_settings().study.mounted_paths.study_internal_files_root_path
            )
            user_token = get_settings().auth.service_account.api_token
            user = UserService.get_instance().get_db_user_by_user_token(user_token)

        except Exception as e:
            db_session.rollback()
            raise e
    studies = [
        x["acc"]
        for x in studies
        if int(x["acc"].replace("MTBLS", "").replace("REQ", "")) >= 4381
    ]
    # studies = ["MTBLS9"]
    for study_id in studies:
        study_internal_files_path = os.path.join(internal_files_root_path, study_id)
        revisions_root_path = os.path.join(study_internal_files_path, "PUBLIC_METADATA")
        metadata_revisions_path = os.path.join(
            revisions_root_path, "METADATA_REVISIONS"
        )
        study_audit_root_path = os.path.join(audit_folder_root_path, study_id, "audit")
        folder_name = "2025_06_04_BEFORE_NEW_WORKFLOW"
        latest_revision = 0
        updated = False
        with DBManager.get_instance().session_maker() as db_session:
            try:
                db_study: Study = (
                    db_session.query(Study).filter(Study.acc == study_id).first()
                )
                study_status = StudyStatus(db_study.status)
                audit_folder_path = os.path.join(study_audit_root_path, folder_name)
                if not os.path.exists(audit_folder_path):
                    StudyRevisionService.create_audit_folder(
                        db_study, folder_name=folder_name
                    )
                if study_status != StudyStatus.PUBLIC:
                    continue
                latest_revision = db_study.revision_number
                if db_study.revision_number == 0 and study_status == StudyStatus.PUBLIC:
                    if os.path.exists(revisions_root_path):
                        shutil.rmtree(revisions_root_path)
                    revision = StudyRevisionService.increment_study_revision(
                        study_id,
                        revision_comment="Initial study revision.",
                        revision_datetime=db_study.first_public_date,
                        created_by=user.email,
                    )
                    db_session.refresh(db_study)
                    db_study.revision_datetime = revision.revision_datetime
                    latest_revision = db_study.revision_number
                    StudyRevisionService.update_investigation_file_from_db(study_id)
                    folder_status, source_path, created_path = (
                        StudyRevisionService.create_revision_folder(db_study)
                    )
                    logger.info(f"Folder created: {created_path}")

                    # data_files_root_path = settings.mounted_paths.study_readonly_files_actual_root_path
                    # study_data_files_path = os.path.join(data_files_root_path, study_id)

                    # revisions_root_hash_path = os.path.join(audit_folder_root_path, study_id, "audit", "PUBLIC_METADATA", "HASHES")
                    # StudyRevisionService.create_data_file_hashes(db_study, search_path=study_data_files_path, copy_paths=[revisions_root_hash_path])

                    # StudyRevisionService.check_dataset_integrity(study_id, metadata_files_path=created_path, data_files_path=study_data_files_path)
                    print("Folder created: " + created_path)
                    updated = True
                    if os.path.exists(revisions_root_path):
                        files = os.listdir(revisions_root_path)
                        if not files:
                            shutil.rmtree(revisions_root_path)
                    db_session.commit()

            except Exception as e:
                db_session.rollback()
                raise e
        # if folder_created:
        revision_folder_name = f"{study_id}_{latest_revision:02}"
        latest_revision_path = os.path.join(
            metadata_revisions_path, revision_folder_name
        )
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

        mounted_paths = get_settings().study.mounted_paths

        # data_files_link_path = os.path.join(mounted_paths.study_metadata_files_root_path, study_id, "FILES")
        # os.unlink(data_files_link_path)

        audit_files_path = os.path.join(
            mounted_paths.study_audit_files_root_path, study_id, "audit"
        )
        audit_files_link_path = os.path.join(
            mounted_paths.study_metadata_files_root_path, study_id, "AUDIT_FILES"
        )
        os.makedirs(audit_files_path, exist_ok=True)
        os.unlink(audit_files_link_path)
        os.symlink(audit_files_path, audit_files_link_path)
        try:
            archived_audit_files_link_path = os.path.join(
                audit_files_path, "ARCHIVED_AUDIT_FILES"
            )
            os.unlink(archived_audit_files_link_path)
        except Exception as ex:
            # print(str(ex))
            pass

        internal_files_path = os.path.join(
            mounted_paths.study_internal_files_root_path, study_id
        )
        internal_files_link_path = os.path.join(
            mounted_paths.study_metadata_files_root_path, study_id, "INTERNAL_FILES"
        )
        os.unlink(internal_files_link_path)
        os.makedirs(internal_files_path, exist_ok=True)
        os.symlink(internal_files_path, internal_files_link_path)
        if updated:
            print(f"{study_id} metadata is updated.")
        else:
            print(f"{study_id} skipped.")


if __name__ == "__main__":
    prepare_revisions()
