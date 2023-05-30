
import os
import time
from typing import Dict, List


from app.tasks.worker import get_flask_app
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import MaintenanceActionLog, StudyFolderMaintenanceTask
from app.ws.settings.study import StudySettings
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService

def maintain_study_folders(
    study_id_list: List[str],
    recycle_bin_folder_name: str = None,
    settings: StudySettings = None,
    delete_unreferenced_metadata_files=True,
    flask_app=None,
):
    if not recycle_bin_folder_name:
        date_format = "%Y%m%d%H%M%S"
        recycle_bin_folder_name = time.strftime(date_format)

    maintenance_task_list: Dict[str, StudyFolderMaintenanceTask] = {}
    if not settings:
        settings = get_study_settings()
    if not flask_app:
        flask_app = get_flask_app()
    action_log_file_path = './study_folder_maintenance_log.tsv'
    future_action_log_file_path = './study_folder_future_maintenance_log.tsv'
    with flask_app.app_context():
        with open(action_log_file_path, "w") as f:
            with open(future_action_log_file_path, "w") as fa:
                header = f"STUDY_ID\tSTUDY STATUS\tSUCCESS\tACTION\tITEM\tMESSAGE\tPARAMETERS\n"
                f.writelines([header])
                f.writelines([header])
                for study_id in study_id_list:
                    study: Study = StudyService.get_instance(flask_app).get_study_by_acc(study_id=study_id)
                    study_status = StudyStatus(study.status)
                    public_release_date = study.releasedate
                    submission_date = study.submissiondate
                    maintenance_task = StudyFolderMaintenanceTask(
                        study_id,
                        study_status,
                        public_release_date,
                        submission_date,
                        recycle_bin_folder_name=recycle_bin_folder_name,
                        settings=settings,
                        delete_unreferenced_metadata_files=delete_unreferenced_metadata_files,
                        force_to_maintain=True
                    )

                    maintenance_task_list[study_id] = maintenance_task
                    try:
                        maintenance_task.maintain_study_rw_storage_folders()
                        maintenance_task.maintain_study_data_files()
                    except Exception as ex:
                        print(f"{str(ex)}")
                    finally:
                        write_actions(f, maintenance_task.actions, study_id, study_status.name)
                        write_actions(fa, maintenance_task.future_actions, study_id, study_status.name)
                        
                        print(study_id, end=" ")                                                

    return maintenance_task_list

def write_actions(f, actions: List[MaintenanceActionLog], study_id, study_status_name):
    rows = []
    for action in actions:
        success = action.successful
        action_name = action.action.name
        item = action.item
        message = action.message
        parameters = action.parameters

        rows.append(f"{study_id}\t{study_status_name}\t{success}\t{action_name}\t{item}\t{message}\t{parameters}\n")
    f.writelines(rows)                      
    f.flush()  

# if __name__ == "__main__":
#     flask_app = get_flask_app()

#     def sort_by_study_id(key: str):
#         if key:
#             val = os.path.basename(key).upper().replace("MTBLS", "")
#             if val.isnumeric():
#                 return int(val)
#         return -1

#     with flask_app.app_context():
#         # studies = StudyService.get_instance(flask_app).get_all_study_ids()
#         study_ids = ["MTBLS2"]
#         # study_ids = [study[0] for study in studies if study[0]]
#         study_ids.sort(key=sort_by_study_id)
#         results = maintain_study_folders(study_ids, flask_app=flask_app)
        
#     print("end")


if __name__ == "__main__":
    flask_app = get_flask_app()

    def sort_by_study_id(key: str):
        if key:
            val = os.path.basename(key).upper().replace("MTBLS", "")
            if val.isnumeric():
                return int(val)
        return -1

    with flask_app.app_context():
        studies = StudyService.get_instance(flask_app).get_all_study_ids()
        skip_study_ids = [f"MTBLS{(i + 1)}" for i in range(501)]
        study_ids = [study[0] for study in studies if study[0] and study[0] not in skip_study_ids]
        study_ids.sort(key=sort_by_study_id)
        results = maintain_study_folders(study_ids, flask_app=flask_app)
    print("end")