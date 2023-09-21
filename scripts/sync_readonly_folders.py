import datetime
import glob
import os
from pathlib import Path
import sys
import time
from typing import Dict, List
from app.config import get_settings
from app.tasks.bash_client import BashClient
from app.tasks.hpc_rsync_worker import HpcRsyncWorker

from app.tasks.worker import get_flask_app
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import (MaintenanceActionLog,
                                       MaintenanceException,
                                       StudyFolderMaintenanceTask)
from app.config.model.study import StudySettings
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService

def get_files(search_path, patterns: List[str], recursive: bool = False):
    files = []
    if not os.path.exists(search_path):
        return files
    for pattern in patterns:
        files.extend(glob.glob(os.path.join(search_path, pattern), recursive=recursive))
    files.sort()
    return files


def update_readonly_storage_folders(
    study_id_list: List[str],
    target: str = None,
    task_name: str = None,
    settings: StudySettings = None,
    output_summary_report=None,
    delete_unreferenced_metadata_files=False,
    apply_future_actions=False,
    cluster_mode:bool=False,
):
    if not study_id_list:
        raise MaintenanceException(message="At least one study should be selected")

    maintenance_task_list: Dict[str, StudyFolderMaintenanceTask] = {}
    start = time.time() 
    start_time_str = datetime.datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Maintenance task started at {start_time_str}")
    if not settings:
        settings = get_study_settings()
    if not output_summary_report:
        output_summary_report = "./readonly_study_folder_updates_log.tsv"
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
                cluster_execution_mode=cluster_mode
            )
                    
            
            maintenance_task_list[study_id] = maintenance_task
            try: 
                maintenance_task.sync_audit_folders_to_readonly_storage()
                maintenance_task.sync_metadata_files_to_readonly_storage()
                maintenance_task.sync_metadata_public_versions_to_readonly_storage()
            finally:
                if maintenance_task.actions:
                    write_actions(fa, maintenance_task.actions, study_id, study_status.name)
                if maintenance_task.future_actions:
                    write_actions(fa, maintenance_task.future_actions, study_id, study_status.name)
                task_done_time = time.time()
                print(f"{study_id} readonly storage maintenance has been completed. Elapsed time in seconds: {int((task_done_time - start)*100)/100.0} " )
                    
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


if __name__ == "__main__":
    

    def sort_by_study_id(key: str):
        if key:
            val = os.path.basename(key).upper().replace("MTBLS", "")
            if val.isnumeric():
                return int(val)
        return -1

    study_ids = []
    if len(sys.argv) > 1 and sys.argv[1]:         
        study_ids = sys.argv[1].split(',')

    target = None
    if len(sys.argv) > 2 and sys.argv[2]:
        target = sys.argv[2]

    output_summary_report = None
    if len(sys.argv) > 3 and sys.argv[3]:
        output_summary_report = sys.argv[3]

    task_name = None
    if len(sys.argv) > 4 and sys.argv[4]:
        task_name = sys.argv[4]

    apply_future_actions = False
    if len(sys.argv) > 5 and sys.argv[5]:
        apply_future_actions = True if sys.argv[5].lower().startswith("apply") else False

    cluster_mode = False
    if len(sys.argv) > 6 and sys.argv[6]:
        cluster_mode = True if sys.argv[6].lower().startswith("cluster") else False
        
    items = set()
    if not study_ids:
        studies = StudyService.get_instance().get_all_study_ids()
        skip_study_ids = []
        study_ids = [study[0] for study in studies if study[0] and study[0] not in skip_study_ids]
    else:
        study_status_map = {data.name.upper():data.value for data in StudyStatus}
        for item in study_ids:
            if item and item.upper().startswith("MTBLS"):
                items.add(item)
            elif item and item.upper() in study_status_map:
                status = StudyStatus(study_status_map[item.upper()])
                study_id_result = StudyService.get_instance().get_study_ids_with_status(status)
                for study in study_id_result:
                    items.add(study[0])
        study_ids = list(items)
    study_ids.sort(key=sort_by_study_id)
    results = update_readonly_storage_folders(
        study_ids,
        target=target,
        output_summary_report=output_summary_report,
        task_name=task_name,
        apply_future_actions=apply_future_actions,
        cluster_mode=cluster_mode
    )

    print("end")