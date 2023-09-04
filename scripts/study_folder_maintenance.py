import os
import sys
import time
from typing import Dict, List

from app.tasks.worker import get_flask_app
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import (MaintenanceActionLog,
                                       MaintenanceException,
                                       StudyFolderMaintenanceTask)
from app.config.model.study import StudySettings
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService


def maintain_folders(
    study_id_list: List[str],
    target: str = None,
    task_name: str = None,
    settings: StudySettings = None,
    output_summary_report=None,
    delete_unreferenced_metadata_files=False,
    apply_future_actions=False,
):
    if target.lower().startswith("metadata"):
        target = "metadata"
    elif target.lower().startswith("data"):
        target = "data"
    elif target.lower().startswith("private-ftp"):
        target = "private-ftp"

    if not study_id_list:
        raise MaintenanceException(message="At least one study should be selected")
    if not target:
        raise MaintenanceException(message="target should be 'metadata', 'data' or 'private-ftp")


    maintenance_task_list: Dict[str, StudyFolderMaintenanceTask] = {}
    if not settings:
        settings = get_study_settings()
    if not output_summary_report:
        output_summary_report = "./study_folder_future_maintenance_log.tsv"
    future_action_log_file_path = output_summary_report

    with open(future_action_log_file_path, "w") as fa:
        header = f"STUDY_ID\tSTUDY STATUS\tSTATUS\tCOMMAND\tACTION\tITEM\tMESSAGE\tPARAMETERS\n"
        fa.writelines([header])
        for study_id in study_id_list:
            study: Study = StudyService.get_instance().get_study_by_acc(study_id=study_id)
            study_status = StudyStatus(study.status)
            public_release_date = study.releasedate
            submission_date = study.submissiondate
            maintenance_task = StudyFolderMaintenanceTask(
                study_id,
                study_status,
                public_release_date,
                submission_date,
                obfuscationcode=study.obfuscationcode,
                task_name=task_name,
                delete_unreferenced_metadata_files=delete_unreferenced_metadata_files,
                settings=settings,
                apply_future_actions=apply_future_actions,
                force_to_maintain=True,
            )

            maintenance_task_list[study_id] = maintenance_task

            if target == "data":
                try:
                    maintenance_task.create_maintenance_actions_for_study_data_files()
                except Exception as ex:
                    print(f"Maintain task could not be completed for {study_id}. {str(ex)}")
                finally:
                    if apply_future_actions:
                        write_actions(fa, maintenance_task.actions, study_id, study_status.name)
                    else:
                        write_actions(fa, maintenance_task.future_actions, study_id, study_status.name)
            elif target == "metadata":
                try:
                    maintenance_task.maintain_study_rw_storage_folders()
                except Exception as ex:
                    print(f"Maintain task could not be completed for {study_id}. {str(ex)}")
                finally:
                    write_actions(fa, maintenance_task.actions, study_id, study_status.name)
            elif target == "private-ftp":
                try:
                    maintenance_task.create_maintenace_actions_for_study_private_ftp_folder()
                except Exception as ex:
                    print(f"Maintain task could not be completed for {study_id}. {str(ex)}")
                finally:
                    if apply_future_actions:
                        write_actions(fa, maintenance_task.actions, study_id, study_status.name)
                    else:
                        write_actions(fa, maintenance_task.future_actions, study_id, study_status.name)

    return maintenance_task_list


def write_actions(f, actions: List[MaintenanceActionLog], study_id, study_status_name):
    rows = []
    for action in actions:
        success = action.successful
        action_name = action.action.name
        item = action.item
        message = action.message
        parameters = action.parameters
        command = action.command
        rows.append(
            f"{study_id}\t{study_status_name}\t{success}\t{command}\t{action_name}\t{item}\t{message}\t{parameters}\n"
        )
    f.writelines(rows)
    f.flush()


# if __name__ == "__main__":
    

#     def sort_by_study_id(key: str):
#         if key:
#             val = os.path.basename(key).upper().replace("MTBLS", "")
#             if val.isnumeric():
#                 return int(val)
#         return -1

#     study_ids = []
#     if len(sys.argv) > 1 and sys.argv[1]:
#         study_ids = [sys.argv[1]]

#     target = None
#     if len(sys.argv) > 2 and sys.argv[2]:
#         target = sys.argv[2]

#     output_summary_report = None
#     if len(sys.argv) > 3 and sys.argv[3]:
#         output_summary_report = sys.argv[3]

#     task_name = None
#     if len(sys.argv) > 4 and sys.argv[4]:
#         task_name = sys.argv[4]

#     apply_future_actions = False
#     if len(sys.argv) > 5 and sys.argv[5]:
#         apply_future_actions = True if sys.argv[5].lower().startswith("apply") else False

#     if not study_ids:
#         studies = StudyService.get_instance().get_all_study_ids()
#         skip_study_ids = []
#         study_ids = [study[0] for study in studies if study[0] and study[0] not in skip_study_ids]
        
#     study_ids.sort(key=sort_by_study_id)
#     results = maintain_folders(
#         study_ids,
#         target=target,
#         output_summary_report=output_summary_report,
#         task_name=task_name,
#         apply_future_actions=apply_future_actions,
#     )

#     print("end")


if __name__ == "__main__":
    flask_app = get_flask_app()

    def sort_by_study_id(key: str):
        if key:
            val = os.path.basename(key).upper().replace("MTBLS", "")
            if val.isnumeric():
                return int(val)
        return -1

    # with flask_app.app_context():
    # studies = StudyService.get_instance().get_all_study_ids()
    # skip_study_ids = [f"MTBLS{(i + 1)}" for i in range(501)]
    study_ids = ["MTBLS20001054"]
    # study_ids.sort(key=sort_by_study_id)
    results = maintain_folders(study_ids, target="metadata")
    print("end")
