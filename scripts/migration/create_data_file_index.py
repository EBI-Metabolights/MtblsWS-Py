import json
import logging
from app.tasks.datamover_tasks.basic_tasks.ftp_operations import index_study_data_files
import os
from app.config import get_settings
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.types import StudyRevisionStatus, StudyStatus

from app.ws.study.study_revision_service import StudyRevisionService
from app.ws.study.study_service import StudyService

logger = logging.getLogger("wslog")


if __name__ == "__main__":
    user_token = get_settings().auth.service_account.api_token
    # user = UserService.get_instance().get_db_user_by_user_token(user_token)

    studies = []
    with DBManager.get_instance().session_maker() as db_session:
        try:
            # db_session.query(StudyRevision).delete()
            # db_session.commit()
            result = db_session.query(
                Study.acc,
                Study.revision_number,
                Study.obfuscationcode,
                Study.status,
                Study.studysize,
            ).all()
            if result:
                studies = list(result)
                studies.sort(
                    key=lambda x: int(x["acc"].replace("MTBLS", "").replace("REQ", ""))
                )

        except Exception as e:
            db_session.rollback()
            raise e
    selected_studies = [
        (x["acc"], x["obfuscationcode"])
        for x in studies
        # if int(x["acc"].replace("MTBLS", "").replace("REQ", "")) >= 10000
    ]
    # selected_studies.sort(key=lambda x: x[1])
    studies = [x[0] for x in selected_studies]
    mounted_paths = get_settings().study.mounted_paths
    # studies = ["MTBLS1"]
    for study_id in studies:
        target_root_path = os.path.join(
            mounted_paths.study_internal_files_root_path, study_id, "DATA_FILES"
        )
        target_path = os.path.join(target_root_path, "data_file_index.json")
        study: Study = StudyService.get_instance().get_study_by_acc(study_id)
        study_status = StudyStatus(study.status)
        if os.path.exists(target_path):
            try:
                with open(target_path) as f:
                    data = json.load(f)
                    if "revision_number_on_public_ftp" in data:
                        continue
            except Exception as ex:
                pass
        if study_status in {StudyStatus.PUBLIC}:
            revision = StudyRevisionService.get_study_revision(
                study.acc, study.revision_number
            )
            if revision.status in {StudyRevisionStatus.COMPLETED}:
                result = index_study_data_files(
                    study_id=study_id, obfuscation_code=study.obfuscationcode
                )
                print(f"{result}")
        elif study_status in {StudyStatus.INREVIEW, StudyStatus.PRIVATE}:
            result = index_study_data_files(
                study_id=study_id, obfuscation_code=study.obfuscationcode
            )
            print(f"{result}")
