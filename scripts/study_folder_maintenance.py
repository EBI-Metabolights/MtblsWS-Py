import datetime
import glob
import json
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

def are_metadata_files_sync(maintenance_task: StudyFolderMaintenanceTask, check_ftp_folders: bool =True):
    study_id = maintenance_task.study_id
    study_settings = get_settings().study
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    private_ftp_folder_name = f"{study_id.lower()}-{maintenance_task.obfuscationcode}"
    private_ftp_path =  os.path.join(mounted_paths.cluster_private_ftp_root_path, private_ftp_folder_name)
    
    legacy_study_path =  os.path.join(mounted_paths.cluster_legacy_study_files_root_path, study_id) 
    audit_path =  os.path.join(mounted_paths.cluster_study_audit_files_root_path, study_id, study_settings.audit_folder_name) 
    
    metadata_file_patterns = ["[ais]_*.txt", "m_*.tsv"]
    backup_folder_search_pattern ="STORAGE_MIGRATION_*"
    saved_private_ftp_path =None
    saved_audit_path = None
    saved_private_ftp_path_hash = ""
    saved_audit_path_hash = ""
    if os.path.exists(audit_path):
        hash_file_name = study_settings.metadata_files_signature_file_name
        files = get_files(audit_path,  [f"FTP_FOLDER_{backup_folder_search_pattern}"])
        if files:
            saved_private_ftp_path = files[0] if len(files) == 1 else files[-1]
        saved_private_ftp_path_hash = maintenance_task.read_hash_file(metadata_files_signature_root_path=saved_private_ftp_path, hash_file_name=hash_file_name)
        print(f"Saved FTP folder metadata hash: {saved_private_ftp_path_hash}")
        files = get_files(audit_path, [backup_folder_search_pattern])
        if files:
            saved_audit_path = files[0] if len(files) == 1 else files[-1]
        saved_audit_path_hash = maintenance_task.read_hash_file(metadata_files_signature_root_path=saved_audit_path, hash_file_name=hash_file_name)
        print(f"Saved Audit folder metadata hash: {saved_audit_path_hash}")
    else:
        print(f"{study_id}: Audit folders do not exist.")
        return False
    
    if not saved_audit_path or not saved_audit_path_hash:
        print(f"{study_id}: Study folder audit folder does not exist.")
        return False
    
    legacy_study_path_hash, _ = maintenance_task.calculate_metadata_files_hash(search_path=legacy_study_path)
    print(f"Calculated study folder metadata hash: {legacy_study_path_hash}")
    study_folder_sync = True if saved_audit_path_hash == legacy_study_path_hash else False
    print(f"{study_id}: Study folder sync status: {study_folder_sync}, Folder exists: {True if saved_private_ftp_path else False}")
    
    if check_ftp_folders:
        ftp_path_sync = False
        if saved_private_ftp_path:
            private_ftp_path_hash, _ = maintenance_task.calculate_metadata_files_hash(search_path=private_ftp_path)
            print(f"Calculated FTP folder metadata hash: {private_ftp_path_hash}")
            ftp_path_sync = True if private_ftp_path_hash == saved_private_ftp_path_hash else False
        else:
            private_ftp_metadata_files = get_files(private_ftp_path, patterns=metadata_file_patterns)
            print(f"There is no FTP folder backup folder. Number of metadata files on FTP folder: {len(private_ftp_metadata_files)}")
            ftp_path_sync = True if not private_ftp_metadata_files else False
    
            print(f"{study_id}: FTP sync status: {ftp_path_sync}, Study folder sync status: {study_folder_sync}, Folder exists: {True if saved_private_ftp_path else False}")
        
        if ftp_path_sync and study_folder_sync:    
            return True
    else:
        return study_folder_sync
    

def maintain_folders(
    study_id_list: List[str],
    target: str = None,
    task_name: str = None,
    settings: StudySettings = None,
    output_summary_report=None,
    delete_unreferenced_metadata_files=False,
    apply_future_actions=False,
    cluster_mode:bool=False,
    skip_if_metadata_is_sync:bool=True,
    check_ftp_folders=True
):
    if not target:
        raise MaintenanceException(message="target should be 'metadata', 'data', 'private-ftp or combination of them with , char. Examples: 'metadata,data', 'metadata,data,private-ftp'")
    
    target_list: List[str] = [x.strip().lower() for x in target.split(',')]
    

    if not study_id_list:
        raise MaintenanceException(message="At least one study should be selected")
    
    # mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths

    # for study_id in study_id_list:
    #     study: Study = StudyService.get_instance().get_study_by_acc(study_id=study_id)
    #     private_ftp_folder_name = f"{study_id.lower()}-{study.obfuscationcode}"
    #     private_ftp_path =  os.path.join(mounted_paths.cluster_private_ftp_root_path, private_ftp_folder_name)
    #     private_ftp_metadata_files = get_files(private_ftp_path, patterns = ["[asi]_*.txt", "m_*.tsv"])
    #     if private_ftp_metadata_files:
    #         print(f"{study_id}")
    
    maintenance_task_list: Dict[str, StudyFolderMaintenanceTask] = {}
    start = time.time() 
    start_time_str = datetime.datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Maintenance task started at {start_time_str}")
    if not settings:
        settings = get_study_settings()
    if not output_summary_report:
        output_summary_report = "./study_folder_maintenance_log.tsv"
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
            study_settings = get_settings().study
            released_version ="PUBLIC_VERSION_1.0"
            mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
            audit_folder_root_path =  os.path.join(mounted_paths.cluster_study_audit_files_root_path, study_id, study_settings.audit_folder_name)
            released_version_path = os.path.join(audit_folder_root_path, released_version)
            
            maintenance_task_list[study_id] = maintenance_task
            try:
                
                
                legacy_study_path =  os.path.join(mounted_paths.cluster_legacy_study_files_root_path, study_id) 
                
                study_id = maintenance_task.study_id
                legacy_metadata_path = os.path.join(mounted_paths.cluster_legacy_study_files_root_path, study_id)
                new_metadata_path  = os.path.join(mounted_paths.cluster_study_metadata_files_root_path, study_id)
                new_metadata_folder_exists = True if os.path.exists(new_metadata_path) else False
                if cluster_mode and "data" in target_list:
                    maintenance_task.calculate_readonly_study_folders_future_actions()
                if cluster_mode and "private-ftp" in target_list:
                    maintenance_task.create_maintenace_actions_for_study_private_ftp_folder()
                maintenance_task.maintain_rw_storage_folders()
                folders_are_sync = False
                audit_path = os.path.join(audit_folder_root_path , f"{maintenance_task.task_name}_BACKUP")
                if not new_metadata_folder_exists:
                    maintenance_task.create_audit_folder(metadata_files_path=legacy_study_path, 
                                                    audit_folder_root_path=audit_folder_root_path, 
                                                    metadata_files_signature_root_path=audit_path, 
                                                    folder_name=maintenance_task.task_name)
                    
                    if os.path.exists(legacy_metadata_path):
                        include_list = ["[asi]_*.txt", "m_*.tsv"]
                        exclude_list = ["*"]
                        command = HpcRsyncWorker.build_rsync_command(
                            legacy_metadata_path, new_metadata_path, include_list, exclude_list, rsync_arguments='-auv'
                        )
                        BashClient.execute_command(command)
                else:                   
                    if  skip_if_metadata_is_sync:
                        folders_are_sync = are_metadata_files_sync(maintenance_task, check_ftp_folders)
                        if folders_are_sync:
                            print(f"{study_id} metadata folder is sync. Skip this folder")
                            continue
                        else:
                            maintenance_task.create_audit_folder()
                            maintenance_task.create_audit_folder(metadata_files_path=legacy_study_path, 
                                                         audit_folder_root_path=audit_folder_root_path, 
                                                         metadata_files_signature_root_path=audit_path, 
                                                         folder_name=maintenance_task.task_name)
                            
                            study_id = maintenance_task.study_id
                            mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
                            source_path = os.path.join(mounted_paths.cluster_legacy_study_files_root_path, study_id)
                            target_path  = os.path.join(mounted_paths.cluster_study_metadata_files_root_path, study_id)
                            files = maintenance_task.get_all_metadata_files()
                            for file in files:
                                maintenance_task.backup_file(file, reason="Replaced from legacy storage", keep_file_on_folder=False)
                            include_list = ["[asi]_*.txt", "m_*.tsv"]
                            exclude_list = ["*"]
                            command = HpcRsyncWorker.build_rsync_command(
                                source_path, target_path, include_list, exclude_list, rsync_arguments='-auv'
                            )
                            result = BashClient.execute_command(command)
                    else:                        
                        if not folders_are_sync:
                            maintenance_task.create_audit_folder()
                        

                
                if study_status == StudyStatus.PUBLIC and not os.path.exists(released_version_path) and os.path.exists(legacy_study_path):
                    os.makedirs(released_version_path, exist_ok=True)
                    maintenance_task.create_audit_folder(metadata_files_path=legacy_study_path, 
                                                         audit_folder_root_path=audit_folder_root_path, 
                                                         metadata_files_signature_root_path=released_version_path, 
                                                         folder_name=released_version, stage=None)
                    
                if cluster_mode and "private-ftp" in target_list:         
                    maintenance_task.backup_study_private_ftp_metadata_files = False if folders_are_sync else True
                    try:
                        if maintenance_task.backup_study_private_ftp_metadata_files:
                            maintenance_task.backup_private_ftp_metadata_files()
                    except Exception as ex:
                        print(f"Maintain task could not be completed for {study_id}. {str(ex)}")
            
                try:
                    maintenance_task.maintain_study_rw_storage_folders(create_audit_folder=False)
                except Exception as ex:
                    print(f"Maintain task could not be completed for {study_id}. {str(ex)}")
                        
                # if "private-ftp" in target_list:
                #     try:
                #         if maintenance_task.backup_study_private_ftp_metadata_files:
                #             maintenance_task.backup_private_ftp_metadata_files()
                #     except Exception as ex:
                #         print(f"Maintain task could not be completed for {study_id}. {str(ex)}")

                # if "metadata" in target_list:
                #     try:
                #         maintenance_task.maintain_study_rw_storage_folders(create_audit_folder=False)
                #     except Exception as ex:
                #         print(f"Maintain task could not be completed for {study_id}. {str(ex)}")
                
                # if "data" in target_list:
                #     try:
                #         pass
                #         # updated_file_names = maintenance_task.create_maintenance_actions_for_study_data_files()
                        
                #         # if updated_file_names:
                #         #     command_actions = [x for x in maintenance_task.future_actions if x.command]
                #         #     data_files_update_summary = {"updates": updated_file_names, "actions": [x.dict() for x in command_actions]}
                #         #     os.makedirs(maintenance_task.task_temp_path, exist_ok=True)
                #         #     summary_file = os.path.join(maintenance_task.task_temp_path, f"{study_id}_data_files_update_summary.json")
                            
                #         #     update_script = os.path.join(maintenance_task.task_temp_path, f"{study_id}_data_files_update.sh")
                #         #     with open(summary_file, "w") as f:
                #         #         f.write(f"{json.dumps(data_files_update_summary, indent = 4)}")
                #         #     with open(update_script, "w") as f:
                #         #         for action in command_actions:
                #         #             f.write(f"{action.command}\n")
                #             # Update all assay files 
                #             # all_data_columns = [column for column in assay_df.columns if " Data File" in column]
                #             # files = maintenance_task.get_all_metadata_files()
                #             # for file in files:
                #             #     basename: str = os.path.basename(file)
                #             #     if basename.startswith("a_") and basename.endswith(".txt"):
                #             #         assay_df = maintenance_task.read_tsv_file(file)
                #             #     if assay_df is not None:
                #             #         for col in all_data_columns:
                #             #             assay_df[col] = assay_df[col].apply(lambda x: updated_file_names[x])
                               
        
                #     except Exception as ex:
                #         print(f"Maintain task could not be completed for {study_id}. {str(ex)}")
                print(f"{str(study_id)} maintenance is completed.")
            except Exception as ex:
                print(str(ex))
            finally:
                maintenance_task.save_actions()
                if maintenance_task.actions:
                    write_actions(fa, maintenance_task.actions, study_id, study_status.name)
                if maintenance_task.future_actions:
                    write_actions(fa, maintenance_task.future_actions, study_id, study_status.name)
                task_done_time = time.time()
                print(f"{study_id} maintenance has been completed. Elapsed time in seconds: {int((task_done_time - start)*100)/100.0} " )
                    
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

    check_ftp_folders = True
    if len(sys.argv) > 7 and sys.argv[7]:
        cluster_mode = False if sys.argv[6].lower().startswith("false") else True
            
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
    study_ids.sort(key=sort_by_study_id, reverse=True)
    results = maintain_folders(
        study_ids,
        target=target,
        output_summary_report=output_summary_report,
        task_name=task_name,
        apply_future_actions=apply_future_actions,
        cluster_mode=cluster_mode,
        check_ftp_folders=check_ftp_folders
    )

    print("end")
