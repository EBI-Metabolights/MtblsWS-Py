import glob
import hashlib
import logging
import os
import pathlib
import re
import shutil
import time
from datetime import datetime
from enum import Enum
from typing import Dict, List, Set

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from isatools import isatab
from isatools import model as isatools_model
from pydantic import BaseModel

from app.config.model.hpc_cluster import HpcClusterConfiguration
from app.config.model.study import StudySettings
from app.file_utils import make_dir_with_chmod
from app.services.storage_service.acl import Acl
from app.services.storage_service.mounted.local_file_manager import \
    MountedVolumeFileManager
from app.services.storage_service.remote_worker.remote_file_manager import \
    RemoteFileManager
from app.utils import INVESTIGATION_FILE_ROWS_LIST, INVESTIGATION_FILE_ROWS_SET
from app.ws.db.types import StudyStatus
from app.ws.settings.utils import get_cluster_settings, get_study_settings

logger = logging.getLogger("wslog")


RAW_FILE_EXTENSIONS = {".d", ".raw", ".d.zip", ".raw.zip", ".wiff", ".wiff.scan"}
DERIVED_FILE_EXTENSIONS = {".mzml", ".mzdata", ".mzxml"}


class MaintenanceException(Exception):
    def __init__(self, message="") -> None:
        self.message = message

    def __str__(self) -> str:
        return self.message


class MaintenanceAction(str, Enum):
    INFO_MESSAGE = "INFO_MESSAGE"
    ERROR_MESSAGE = "ERROR_MESSAGE"
    WARNING_MESSAGE = "WARNING_MESSAGE"
    SUMMARY_MESSAGE = "SUMMARY_MESSAGE"
    CREATE = "CREATE"
    COPY = "COPY"
    MOVE = "MOVE"
    RENAME = "RENAME"
    DELETE = "DELETE"
    COMPRESS = "COMPRESS"
    RECOMPRESS = "RECOMPRESS"

    UPDATE_CONTENT = "UPDATE_CONTENT"
    UPDATE_FILE_PERMISSION = "UPDATE_FILE_PERMISSION"


class StudyFolders(BaseModel):
    study_metadata_files_root_path: str = ""
    study_internal_files_root_path: str = ""
    study_audit_files_root_path: str = ""

    study_readonly_files_root_path: str = ""
    study_readonly_audit_files_root_path: str = ""
    study_readonly_metadata_files_root_path: str = ""
    study_readonly_public_metadata_versions_root_path: str = ""
    study_readonly_integrity_check_files_root_path: str = ""

    readonly_storage_recycle_bin_root_path: str = ""
    rw_storage_recycle_bin_root_path: str = ""

    cluster_private_ftp_root_path: str = ""
    cluster_public_ftp_root_path: str = ""
    cluster_private_ftp_recycle_bin_root_path: str = ""
    cluster_public_ftp_recycle_bin_root_path: str = ""    
    

class FolderRootPaths(object):
    def __init__(self, study_settings: StudySettings, cluster_settings: HpcClusterConfiguration, cluster_mode: False):
        self.study_settings = study_settings
        self.cluster_settings = cluster_mode
        self.cluster_mode = cluster_mode
        self.folders = StudyFolders()
        self.folders.study_metadata_files_root_path = self.study_settings.study_metadata_files_root_path
        
        if self.cluster_mode:
            self._update_folders_as_cluster_mode()
        else:
            self._update_folders()

        self.folders.cluster_private_ftp_root_path = self.study_settings.cluster_private_ftp_root_path
        self.folders.cluster_private_ftp_recycle_bin_root_path = self.study_settings.cluster_private_ftp_recycle_bin_root_path
        self.folders.cluster_public_ftp_root_path = self.study_settings.cluster_public_ftp_root_path
        self.folders.cluster_public_ftp_recycle_bin_root_path = self.study_settings.cluster_public_ftp_recycle_bin_root_path
        

    def _update_folders(self):
        self.folders.study_metadata_files_root_path = self.study_settings.study_metadata_files_root_path
        self.folders.study_internal_files_root_path = self.study_settings.study_internal_files_root_path
        self.folders.study_audit_files_root_path = self.study_settings.study_audit_files_root_path
        
        self.folders.study_readonly_files_root_path = self.study_settings.study_readonly_files_root_path
        self.folders.study_readonly_audit_files_root_path = self.study_settings.study_readonly_audit_files_root_path
        self.folders.study_readonly_metadata_files_root_path = self.study_settings.study_readonly_metadata_files_root_path
        self.folders.study_readonly_public_metadata_versions_root_path = self.study_settings.study_readonly_public_metadata_versions_root_path
        self.folders.study_readonly_integrity_check_files_root_path = self.study_settings.study_readonly_integrity_check_files_root_path
        
        self.folders.readonly_storage_recycle_bin_root_path = self.study_settings.readonly_storage_recycle_bin_root_path
        self.folders.rw_storage_recycle_bin_root_path = self.study_settings.rw_storage_recycle_bin_root_path
        

 
    def _update_folders_as_cluster_mode(self):
        self.folders.study_metadata_files_root_path = self.study_settings.cluster_study_metadata_files_root_path
        self.folders.study_internal_files_root_path = self.study_settings.cluster_study_internal_files_root_path
        self.folders.study_audit_files_root_path = self.study_settings.cluster_study_audit_files_root_path
        
        self.folders.study_readonly_files_root_path = self.study_settings.cluster_study_readonly_files_root_path
        self.folders.study_readonly_audit_files_root_path = self.study_settings.cluster_study_readonly_audit_files_root_path
        self.folders.study_readonly_metadata_files_root_path = self.study_settings.cluster_study_readonly_metadata_files_root_path
        self.folders.study_readonly_public_metadata_versions_root_path = self.study_settings.cluster_study_readonly_public_metadata_versions_root_path
        self.folders.study_readonly_integrity_check_files_root_path = self.study_settings.study_readonly_integrity_check_files_root_path
        
        self.folders.readonly_storage_recycle_bin_root_path = self.study_settings.cluster_readonly_storage_recycle_bin_root_path
        self.folders.rw_storage_recycle_bin_root_path = self.study_settings.cluster_rw_storage_recycle_bin_root_path
               

class MaintenanceActionLog(BaseModel):
    item: str = None
    action: MaintenanceAction = MaintenanceAction.INFO_MESSAGE
    parameters: Dict[str, str] = {}
    message: str = ""
    successful: bool = True
    command: str = ""


class StudyFolderMaintenanceTask(object):
    def __init__(
        self,
        study_id: str,
        study_status: StudyStatus,
        public_release_date: datetime,
        submission_date: datetime,
        obfuscationcode: str = None,
        settings: StudySettings = None,
        task_name=None,
        delete_unreferenced_metadata_files=True,
        fix_tsv_file_headers=True,
        fix_assay_file_column_values=True,
        fix_sample_file_column_values=True,
        fix_assignment_file_column_values=True,
        force_to_maintain=False,
        minimum_investigation_file_line=91,
        future_actions_sanitise_referenced_files=False,
        future_actions_create_subfolders=False,
        future_actions_compress_folders=False,
        future_actions_recompress_unexpected_acrhive_files=False,
        apply_future_actions=False,
        max_referenced_files_in_folder=200,
        create_data_files_maintenance_file=True,
        cluster_execution_mode=False,
    ) -> None:
        self.study_id = study_id
        self.obfuscationcode = obfuscationcode
        self.study_status = study_status
        self.public_release_date = public_release_date
        self.submission_date = submission_date
        self.delete_unreferenced_metadata_files = delete_unreferenced_metadata_files
        self.study_settings = settings
        if not self.study_settings:
            self.study_settings = get_study_settings()
        
        self.investigation_file_name = self.study_settings.investigation_file_name
        self.cluster_settings: HpcClusterConfiguration = get_cluster_settings()
        self.cluster_execution_mode = cluster_execution_mode
        
        self.paths: FolderRootPaths = FolderRootPaths(study_settings=self.study_settings, cluster_settings=self.cluster_settings, cluster_mode=self.cluster_execution_mode)
        self.settings = self.paths.folders
        self.task_name = task_name
        date_format = "%Y-%m-%d_%H-%M-%S"
        timestamp_str = time.strftime(date_format)
        if not self.task_name:
            self.recycle_bin_folder_name = f"{self.study_id}_{timestamp_str}"
            self.task_name = timestamp_str
        else:
            self.recycle_bin_folder_name = f"{self.study_id}_{self.task_name}_{timestamp_str}"
            self.task_name = f"{self.task_name}_{timestamp_str}"
            
        self.study_recycle_bin_path = os.path.join(
            self.settings.study_audit_files_root_path,
            study_id,
            self.study_settings.internal_backup_folder_name,
            self.recycle_bin_folder_name,
        )

        self.readonly_storage_recycle_bin_path = os.path.join(
            self.settings.readonly_storage_recycle_bin_root_path, self.recycle_bin_folder_name
        )
        self.rw_storage_recycle_bin_path = os.path.join(
            self.settings.rw_storage_recycle_bin_root_path, self.recycle_bin_folder_name
        )

        self.study_metadata_files_path = os.path.join(self.settings.study_metadata_files_root_path, study_id)
        self.study_readonly_files_path = os.path.join(self.settings.study_readonly_files_root_path, study_id)
        self.study_audit_files_path = os.path.join(
            self.settings.study_audit_files_root_path, study_id, self.study_settings.audit_folder_name
        )
        self.study_internal_files_path = os.path.join(self.settings.study_internal_files_root_path, study_id)

        self.actions: List[MaintenanceActionLog] = []
        self.future_actions: List[MaintenanceActionLog] = []
        self.referenced_metadata_file_list = []

        self.fix_tsv_file_headers = fix_tsv_file_headers
        self.fix_assay_file_column_values = fix_assay_file_column_values
        self.fix_sample_file_column_values = fix_sample_file_column_values
        self.fix_assignment_file_column_values = fix_assignment_file_column_values
        self.force_to_maintain = force_to_maintain
        self.minimum_investigation_file_line = minimum_investigation_file_line
        self.future_actions_create_subfolders = future_actions_create_subfolders
        self.future_actions_sanitise_referenced_files = future_actions_sanitise_referenced_files
        self.future_actions_create_subfolders = future_actions_create_subfolders
        self.future_actions_compress_folders = future_actions_compress_folders
        self.future_actions_recompress_unexpected_acrhive_files = future_actions_recompress_unexpected_acrhive_files
        self.max_referenced_files_in_folder = max_referenced_files_in_folder
        self.create_data_files_maintenance_file = create_data_files_maintenance_file
        self.apply_future_actions = apply_future_actions
        
        # self.file_manager = MountedVolumeFileManager(f"{self.study_id}_local_file_manager")
    
    def create_audit_folder(self, folder_name: str = None, stage: str = "BEFORE"):
        if os.path.exists(self.study_metadata_files_path):
            self.maintain_rw_storage_folders()
            metadata_files_list = self.get_all_metadata_files(recursive=False)
            metadata_files_list.sort()
            
            if metadata_files_list:
                if not folder_name:
                    date_format = "%Y-%m-%d_%H-%M-%S"
                    folder_name =  time.strftime(date_format)
                if stage:
                    folder_name = f"{folder_name}_{stage}"
                audit_folder_path = os.path.join(self.study_audit_files_path, folder_name)
                os.makedirs(audit_folder_path, exist_ok=True)
                
                for file in metadata_files_list:
                    basename = os.path.basename(file)
                    target_file = os.path.join(audit_folder_path, basename)
                    shutil.copy(file, target_file)
                
                self.update_study_folder_metadata_signature()
                metadata_files_signature_path = os.path.join(
                    self.study_internal_files_path, self.study_settings.metadata_files_signature_file_name
                )
                basename = os.path.basename(metadata_files_signature_path)
                target_file = os.path.join(audit_folder_path, basename)
                shutil.copy(metadata_files_signature_path, target_file)
                action_log = MaintenanceActionLog(
                    item=audit_folder_path,
                    action=MaintenanceAction.INFO_MESSAGE,
                    parameters={},
                    message=f"{self.study_id}: Audit folder {folder_name} was created.",
                    successful=True,
                )
                self.actions.append(action_log)
                    
    def execute_future_actions(self) -> Dict[str, str]:
        if self.cluster_execution_mode:
            success = True
            for action in self.future_actions:
                result = self.execute_action(action)
                if not result:
                    success = False
            return success
        return False
    
    def execute_action(self, action_log: MaintenanceActionLog):
        if action_log.action == MaintenanceAction.CREATE:
            permission = None
            try:
                permission = int(action_log.parameters["chmod"], 8)
            except Exception as ex:
                pass
            if permission:
                return make_dir_with_chmod(action_log.item, permission)
            else:
                return make_dir_with_chmod(action_log.item, 0o750)
            
        elif action_log.action == MaintenanceAction.MOVE or action_log.action == MaintenanceAction.RENAME:
            target_path = None
            if "target" in action_log.parameters and action_log.parameters["target"]:
                target_path = action_log.parameters["target"]
            if action_log.item and  target_path:
                try:
                    shutil.move(action_log.item, target_path)
                    return True
                except Exception as exc:
                    return False
        return False

    def _create_sanitise_file_name_actions(self, updated_file_names: Dict[str, str]) -> Dict[str, str]:
        for key in updated_file_names:
            file = updated_file_names[key]
            if file:
                new_basename = self.sanitise_filename(file)
                if new_basename != file:
                    current_path = os.path.join(self.study_metadata_files_path, file)
                    target_path = os.path.join(self.study_metadata_files_path, new_basename)

                    if os.path.exists(target_path):
                        renamed_path = f"{target_path}_{self.task_name}"
                        action_log = MaintenanceActionLog(
                            item=target_path,
                            action=MaintenanceAction.RENAME,
                            parameters={"target": target_path},
                            message=f"",
                            command=f"mv '{target_path}' '{renamed_path}'",
                        )
                        
                        if self.apply_future_actions:
                            shutil.move(target_path, renamed_path)
                            self.actions.append(action_log)
                        else:
                            self.future_actions.append(action_log)
                    if self.apply_future_actions:
                        shutil.move(target_path, renamed_path)
                    action_log = MaintenanceActionLog(
                        item=current_path,
                        action=MaintenanceAction.RENAME,
                        parameters={"target": target_path},
                        message=f"",
                        command=f"mv '{current_path}' '{target_path}'",
                    )
                    self.future_actions.append(action_log)
                    updated_file_names[key] = new_basename
        if self.create_data_files_maintenance_file:
            self._update_data_files_maintenance_file(updated_file_names)
        return updated_file_names

    def _create_compress_folder_actions(self, updated_file_names: Dict[str, str]) -> Dict[str, str]:
        for key in updated_file_names:
            file = updated_file_names[key]
            if file:
                current_path = os.path.join(self.study_metadata_files_path, file)
                if os.path.exists(current_path) and os.path.isdir(current_path):
                    new_basename = f"{file}.zip"
                    target_path = os.path.join(self.study_metadata_files_path, new_basename)
                    current_dirname = os.path.dirname(current_path)
                    current_basename = os.path.dirname(current_path)
                    action_log = MaintenanceActionLog(
                        item=current_path,
                        action=MaintenanceAction.COMPRESS,
                        parameters={"target": target_path},
                        message=f"",
                        command=f"cd '{current_dirname}' && zip -r '{target_path}' '{current_basename}' && cd -",
                    )
                    self.future_actions.append(action_log)
                    updated_file_names[key] = new_basename
        return updated_file_names

    def _create_recompress_folder_actions(self, updated_file_names: Dict[str, str]) -> Dict[str, str]:
        non_standard_compressed_file_extensions = {".rar", ".7z", ".z", ".g7z", ".arj", ".rar", ".bz2", ".war"}
        for key in updated_file_names:
            file = updated_file_names[key]
            if file:
                base = os.path.basename(file)
                basename, ext = os.path.splitext(base)
                if ext in non_standard_compressed_file_extensions:
                    current_path = os.path.join(self.study_metadata_files_path, file)
                    new_basename = f"{basename}.zip"
                    target_path = os.path.join(self.study_metadata_files_path, new_basename)
                    action_log = MaintenanceActionLog(
                        item=current_path,
                        action=MaintenanceAction.RECOMPRESS,
                        parameters={"target": target_path},
                        message=f"",
                    )
                    self.future_actions.append(action_log)
                    updated_file_names[key] = new_basename
        return updated_file_names

    def _create_subfolder_actions(self, updated_file_names: Dict[str, str]) -> Dict[str, str]:
        referenced_directories = {}
        for key in updated_file_names:
            file = updated_file_names[key]
            if file:
                file_path = os.path.join(self.study_metadata_files_path, file)
                dirname = os.path.dirname(file_path)
                if dirname not in referenced_directories:
                    referenced_directories[dirname] = set()
                referenced_directories[dirname].add(key)
        subfolders_map = {}
        maximum = self.max_referenced_files_in_folder
        for referenced_folder in referenced_directories:
            if len(referenced_directories[referenced_folder]) > maximum:
                files = list(referenced_directories[referenced_folder])
                extensions_map = {}
                for key in files:
                    ref_file = updated_file_names[key]
                    val = os.path.basename(ref_file).lower()
                    base, ext = os.path.splitext(val)
                    if ext not in extensions_map:
                        extensions_map[ext] = []
                    extensions_map[ext].append(key)
                for extension in extensions_map:
                    if len(extensions_map[extension]) > int(maximum / 2):
                        extension_file_count = len(extensions_map[extension])
                        folder_count = int(len(extensions_map[extension]) / maximum)
                        if extension_file_count % maximum > 0:
                            folder_count += 1
                        cleared_extension = extension.replace(".", "").upper()
                        prefix = f"{cleared_extension}_" if cleared_extension else ""

                        for i in range(folder_count):
                            subfolder_name = f"{prefix}{(i + 1):03}"
                            new_folder = os.path.join(referenced_folder, subfolder_name)
                            if os.path.exists(new_folder):
                                renamed_path = f"{new_folder}_{self.task_name}"
                                action_log = MaintenanceActionLog(
                                    item=new_folder,
                                    action=MaintenanceAction.RENAME,
                                    parameters={"target": renamed_path},
                                    message=f"{subfolder_name} folder will be renamed.",
                                    command=f"mv '{new_folder}' '{renamed_path}'",
                                )
                                
                                if self.apply_future_actions:
                                    shutil.move(new_folder, renamed_path)
                                    self.actions.append(action_log)
                                else:
                                    self.future_actions.append(action_log)
                                    
                            action_log = MaintenanceActionLog(
                                item=new_folder,
                                action=MaintenanceAction.CREATE,
                                parameters={},
                                message=f"{subfolder_name} folder will be created to split data files on {referenced_folder}.",
                                command=f"mkdir -p '{new_folder}'",
                            )
                            if self.apply_future_actions:
                                os.makedirs(new_folder, exist_ok=True)
                                self.actions.append(action_log)
                            else:
                                self.future_actions.append(action_log)
                            last = min(extension_file_count, (i + 1) * maximum)
                            for j in range(i * maximum, last):
                                key = extensions_map[extension][j]
                                subfolders_map[key] = subfolder_name
        for key in updated_file_names:
            file = updated_file_names[key]
            if key in subfolders_map:
                file_path = os.path.join(self.study_metadata_files_path, file)
                dirname = os.path.dirname(file)
                basename = os.path.basename(file)
                subfolder = subfolders_map[key]
                current_dir_name = os.path.join(dirname, subfolder, basename).replace(
                    f"{self.study_metadata_files_path}/", ""
                )
                target_path = os.path.join(self.study_metadata_files_path, current_dir_name)
                action_log = MaintenanceActionLog(
                    item=file_path,
                    action=MaintenanceAction.MOVE,
                    parameters={"target": target_path},
                    message=f"{file_path} will be moved to a subfolder",
                    command=f"mv '{file_path}' '{target_path}'",
                )
                if self.apply_future_actions:
                    shutil.move(file_path, target_path)
                    self.actions.append(action_log)
                else:
                    self.future_actions.append(action_log)
                updated_file_names[key] = current_dir_name

    def _update_data_files_maintenance_file(self, updated_file_names: Dict[str, str]):
        referenced_files_path = os.path.join(
            self.study_internal_files_path, self.study_settings.data_files_maintenance_file_name
        )
        data_files = []
        for file in updated_file_names:
            file_path = os.path.join(self.study_metadata_files_path, file)
            status = "NOT EXIST"
            type = "FILE"
            modified = 0
            if os.path.exists(file_path):
                status = "EXIST"
                modified = os.path.getmtime(file_path)
            if os.path.isdir(file_path):
                type = "DIRECTORY"
            updated_file_name = updated_file_names[file]
            modified_str = datetime.fromtimestamp(modified).isoformat()
            if updated_file_name != file:
                data_files.append(f"{type}\t{status}\t{modified_str}\t{file}\t{updated_file_names[file]}\n")
            else:
                data_files.append(f"{type}\t{status}\t{modified_str}\t{file}\t\n")

        with open(referenced_files_path, "w") as f:
            f.writelines(["TYPE\tSTATUS\tMODIFIED TIME\tREFERENCED FILE PATH\tRENAME TASK\n"])
            f.writelines(data_files)

    def create_maintenance_actions_for_study_data_files(self) -> List[MaintenanceActionLog]:
        referenced_file_set = self.get_all_referenced_data_files()
        updated_file_names = {}
        for item in referenced_file_set:
            updated_file_names[item] = item

        self.calculate_readonly_study_folders_future_actions()
        if self.future_actions_sanitise_referenced_files:
            self._create_sanitise_file_name_actions(updated_file_names)
        if self.future_actions_create_subfolders:
            self._create_subfolder_actions(updated_file_names)
        if self.future_actions_compress_folders:
            self._create_compress_folder_actions(updated_file_names)
        if self.future_actions_recompress_unexpected_acrhive_files:
            self._create_recompress_folder_actions(updated_file_names)

        return self.future_actions

    def create_maintenace_actions_for_study_private_ftp_folder(self) -> List[MaintenanceActionLog]:
        settings = self.settings
        study_id = self.study_id
        cluster_private_ftp_recycle_bin_root_path = self.settings.cluster_private_ftp_recycle_bin_root_path
        created_folders = {}
        deleted_folders = set()
        folder_name = f"{study_id.lower()}-{self.obfuscationcode}"
        private_ftp_root_path = os.path.join(settings.cluster_private_ftp_root_path, folder_name)
        permission = Acl.READ_ONLY.value

        if self.study_status == StudyStatus.INREVIEW or self.study_status == StudyStatus.INCURATION:
            permission = Acl.AUTHORIZED_READ.value
        elif self.study_status == StudyStatus.SUBMITTED:
            permission = Acl.AUTHORIZED_READ_WRITE.value
        
        create_folder = self.study_status == StudyStatus.INCURATION or self.study_status == StudyStatus.SUBMITTED or self.study_status == StudyStatus.INREVIEW
        if not os.path.exists(private_ftp_root_path):
            if create_folder:
                self._create_folder_future_actions(
                    private_ftp_root_path,
                    0o770,
                    cluster_private_ftp_recycle_bin_root_path,
                    created_folders,
                    deleted_folders,
                )

                sub_folder = os.path.join(private_ftp_root_path, "RAW_FILES")
                self._create_folder_future_actions(
                    sub_folder, 0o770, cluster_private_ftp_recycle_bin_root_path, created_folders, deleted_folders
                )

                sub_folder = os.path.join(private_ftp_root_path, "DERIVED_FILES")
                self._create_folder_future_actions(
                    sub_folder, 0o770, cluster_private_ftp_recycle_bin_root_path, created_folders, deleted_folders
                )
                self.update_permission(private_ftp_root_path, permission)
        else:
            if create_folder and self.study_status == StudyStatus.SUBMITTED:
                self.update_permission(private_ftp_root_path, 0o770)
                sub_folder = os.path.join(private_ftp_root_path, "RAW_FILES")
                self._create_folder_future_actions(
                    sub_folder, 0o770, cluster_private_ftp_recycle_bin_root_path, created_folders, deleted_folders
                )

                sub_folder = os.path.join(private_ftp_root_path, "DERIVED_FILES")
                self._create_folder_future_actions(
                    sub_folder, 0o770, cluster_private_ftp_recycle_bin_root_path, created_folders, deleted_folders
                )                
                         
            self.update_permission(private_ftp_root_path, permission)
        
        
    def sanitise(self, obj):
        message = str(obj)
        if message:
            return " ".join(message.splitlines())
        return ""

    def get_all_referenced_data_files(self) -> Set[str]:
        referenced_files_set = set()
        investigation = None
        investigation_file_path = os.path.join(self.study_metadata_files_path, self.investigation_file_name)
        try:
            investigation = self.load_investigation_file(investigation_file_path)
            if not investigation or not investigation.studies or not investigation.studies[0]:
                raise MaintenanceException(message=f"{self.investigation_file_name} is empty or has no study.")
        except Exception as ex:
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.ERROR_MESSAGE,
                parameters={},
                message=f"{self.study_id}: {self.investigation_file_name} could not be loaded. Error: {self.sanitise(ex)}",
                successful=False,
            )
            self.actions.append(action_log)
            raise ex
        try:
            study: isatools_model.Study = investigation.studies[0]
            if study.assays:
                for item in study.assays:
                    assay: isatools_model.Assay = item
                    assay_file_path = os.path.join(self.study_metadata_files_path, assay.filename)
                    if assay.filename and assay.filename.strip() and os.path.exists(assay_file_path):
                        assay_df = None
                        try:
                            assay_df = self.read_tsv_file(assay_file_path)
                            if assay_df is None:
                                raise MaintenanceException(message=f"{assay_file_path} is not valid.")
                            all_data_columns = []
                            for column in assay_df.columns:
                                if " Data File" in column:
                                    all_data_columns.append(column)
                            if all_data_columns:
                                for index, row in assay_df.iterrows():
                                    for i in range(len(all_data_columns)):
                                        file_name = row[all_data_columns[i]]
                                        if file_name and file_name.strip() and file_name not in referenced_files_set:
                                            current_file_name = file_name.strip()
                                            referenced_files_set.add(current_file_name)

                        except Exception as ex:
                            raise MaintenanceException(
                                message=f"{assay_file_path} could not be loaded. {self.sanitise(ex)}"
                            )
        except Exception as ex:
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.ERROR_MESSAGE,
                parameters={},
                message=f"{self.study_id}: {assay_file_path} could not be processed. Error: {self.sanitise(ex)}",
                successful=False,
            )
            self.actions.append(action_log)
            raise ex
        return referenced_files_set

    def maintain_study_rw_storage_folders(self, create_audit_folder=True) -> List[MaintenanceActionLog]:
        investigation = None
        investigation_file_path = os.path.join(self.study_metadata_files_path, self.investigation_file_name)
        update_metadata_files = False
        try:
            if create_audit_folder:
                self.create_audit_folder(self.task_name)
            self.maintain_rw_storage_folders()
            self.maintain_unwanted_files()
            self.maintain_metadata_file_types_and_permissions()
            same_metada_files = self.check_study_folder_metadata_signature()
            update_metadata_files = not same_metada_files or self.force_to_maintain
            if update_metadata_files:
                investigation = self.maintain_investigation_file()
                if investigation and investigation.studies and investigation.studies[0]:
                    self.maintain_investigation_file_content(investigation)
                    self.maintain_sample_file(investigation)
                    self.maintain_sample_file_content(investigation)
                    self.maintain_assay_files(investigation)
                    self.maintain_assay_file_content(investigation)
        finally:
            if update_metadata_files:
                if investigation and investigation.studies and investigation.studies[0]:
                    for action in self.actions:
                        if action.item == investigation_file_path and action.action == MaintenanceAction.UPDATE_CONTENT:
                            try:
                                self.save_investigation_file(investigation)
                                break
                            except Exception as ex:
                                action_log = MaintenanceActionLog(
                                    item=investigation_file_path,
                                    action=MaintenanceAction.ERROR_MESSAGE,
                                    parameters={},
                                    message=f"{self.study_id}: ERROR: investigation file save error {self.sanitise(ex)}.",
                                    successful=False,
                                )
                                self.actions.append(action_log)

                self.create_metadata_summary_file()
                self.update_study_folder_metadata_signature()
            actions_summary = {}
            for item in self.actions:
                if item.action.name not in actions_summary:
                    actions_summary[item.action.name] = 0
                actions_summary[item.action.name] += 1

            if not actions_summary:
                action_log = MaintenanceActionLog(
                    item=self.study_metadata_files_path,
                    action=MaintenanceAction.SUMMARY_MESSAGE,
                    parameters={},
                    message=f"{self.study_id}: There is no maintenance action on folder {self.study_metadata_files_path}.",
                )
                self.actions.append(action_log)
            else:
                actions_summary_message = ", ".join([f"{x}: {str(actions_summary[x])}" for x in actions_summary])
                success = True
                if MaintenanceAction.ERROR_MESSAGE.name in actions_summary:
                    success = False

                action_log = MaintenanceActionLog(
                    item=self.study_metadata_files_path,
                    action=MaintenanceAction.SUMMARY_MESSAGE,
                    parameters={"summary": actions_summary_message},
                    message=f"{self.study_id}: Maintenance task summary: {actions_summary_message}.",
                    successful=success,
                )
                self.actions.append(action_log)
            self.save_actions()

    def check_study_folder_metadata_signature(self):
        metadata_files_signature_path = os.path.join(
            self.study_internal_files_path, self.study_settings.metadata_files_signature_file_name
        )

        if os.path.exists(metadata_files_signature_path):
            with open(metadata_files_signature_path, "r") as f:
                current_signature_lines = f.readlines()
            if current_signature_lines:
                current_signature_line = current_signature_lines[0].strip()
                metadata_files_signature = self.calculate_metadata_files_hash()
                if current_signature_line == metadata_files_signature:
                    action_log = MaintenanceActionLog(
                        item=self.study_internal_files_path,
                        action=MaintenanceAction.INFO_MESSAGE,
                        parameters={},
                        message=f"{self.study_id}: There is no update in metadata files. Skipping maintenance.",
                        successful=True,
                    )
                    self.actions.append(action_log)
                    return True

        return False

    def update_study_folder_metadata_signature(self):
        metadata_files_signature_path = os.path.join(
            self.study_internal_files_path, self.study_settings.metadata_files_signature_file_name
        )
        current_signature = ""
        file_exists = False
        if os.path.exists(metadata_files_signature_path):
            file_exists = True
            with open(metadata_files_signature_path, "r") as f:
                current_signature_lines = f.readlines()
            if current_signature_lines:
                current_signature = current_signature_lines[0].strip()

        metadata_files_signature = self.calculate_metadata_files_hash()
        if current_signature != metadata_files_signature:
            with open(metadata_files_signature_path, "w") as f:
                f.write(f"{metadata_files_signature}")

                action_log = MaintenanceActionLog(
                    item=metadata_files_signature_path,
                    action=MaintenanceAction.CREATE if not file_exists else MaintenanceAction.UPDATE_CONTENT,
                    parameters={},
                    message=f"{self.study_id}: Metadata files signature updated to {metadata_files_signature}. Previous signature: from {current_signature}. The latest signature was stored in {metadata_files_signature_path}",
                    successful=True,
                )
                self.actions.append(action_log)
        else:
            action_log = MaintenanceActionLog(
                item=metadata_files_signature_path,
                action=MaintenanceAction.INFO_MESSAGE,
                parameters={},
                message=f"{self.study_id}: Metadata files signature was not changed. The latest signature : {metadata_files_signature}.",
                successful=True,
            )
            self.actions.append(action_log)

    def save_actions(self):
        rows = []

        rows.append(f"STUDY ID\tSTUDY STATUS\tSUCCESS\tACTION\tITEM\tMESSAGE\tPARAMETERS\n")
        for action in self.actions:
            success = action.successful
            action_name = action.action.name
            item = action.item
            message = action.message
            parameters = action.parameters

            rows.append(
                f"{self.study_id}\t{self.study_status.name}\t{success}\t{action_name}\t{item}\t{message}\t{parameters}\n"
            )
        action_log_file_path = os.path.join(
            self.study_internal_files_path,
            self.study_settings.internal_logs_folder_name,
            f"{self.task_name}_{self.study_settings.study_folder_maintenance_log_file_name}",
        )
        with open(action_log_file_path, "w") as f:
            f.writelines(rows)

    def create_metadata_summary_file(self, override=True):
        metadata_summary_file_path = os.path.join(
            self.study_internal_files_path, self.study_settings.metadata_summary_file_name
        )
        if not override and os.path.exists(metadata_summary_file_path):
            return
        all_metadata_files = self.get_all_metadata_files(recursive=False)
        all_metadata_file_names = [os.path.basename(x) for x in all_metadata_files]
        referenced_metadata_files = set(self.referenced_metadata_file_list)
        summary_list = list(set(all_metadata_file_names).union(referenced_metadata_files))
        summary_list.sort()

        rows = []
        rows.append(f"HASH:SHA256\tFILE TYPE\tEXISTENCE\tSTATUS\tMODIFIED\tFILE NAME\n")
        for file in summary_list:
            file_path = os.path.join(self.study_metadata_files_path, file)
            status = "REFERENCED" if file in referenced_metadata_files else "UNREFERENCED"
            type = "NOT METADATA"
            existence = "NOT EXIST"
            if os.path.exists(file_path):
                existence = "EXIST"
                m_time = os.path.getmtime(file_path)
            m_time = 0
            if file.startswith("i_"):
                type = "INVESTIGATION"
            elif file.startswith("a_"):
                type = "ASSAY FILE"
            elif file.startswith("s_"):
                type = "SAMPLE FILE"
            elif file.startswith("m_"):
                type = "METABOLITE"
            if type == "NOT METADATA":
                sha256_hash = self.sha256sum("")
            else:
                sha256_hash = self.sha256sum(file_path)

            modified = datetime.fromtimestamp(m_time).isoformat()
            rows.append(f"{sha256_hash}\t{type}\t{existence}\t{status}\t{modified}\t{file}\n")

        if os.path.exists(metadata_summary_file_path):
            self.backup_file(metadata_summary_file_path, prefix="before")

        with open(metadata_summary_file_path, "w") as f:
            f.writelines(rows)

    def calculate_metadata_files_hash(self):
        metadata_files_list = self.get_all_metadata_files(recursive=False)
        metadata_files_list.sort()
        hashes = []
        for file in metadata_files_list:
            hashes.append(self.sha256sum(file))

        final_hash = hashlib.sha256("".join(hashes).encode("utf-8")).hexdigest()
        return final_hash

    def sha256sum(self, filename):
        if not filename or not os.path.exists(filename):
            return hashlib.sha256("".encode()).hexdigest()

        sha256_hash = hashlib.sha256()
        with open(filename, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def get_all_metadata_files(self, recursive=False):
        metadata_files = []
        patterns = ["a_*.txt", "s_*.txt", "i_*.txt", "m_*.tsv"]
        for pattern in patterns:
            metadata_files.extend(glob.glob(os.path.join(self.study_metadata_files_path, pattern), recursive=recursive))
        return metadata_files

    def maintain_metadata_file_types_and_permissions(self, recursive=False):
        metadata_files = []
        metadata_files_list = self.get_all_metadata_files(recursive=False)
        for file in metadata_files_list:
            # if os.path.isfile(file) and not os.path.islink(file) and os.stat(file).st_size == 0:
            #     self.backup_file(file, reason="File size is empty.")
            if os.path.isdir(file):
                self.backup_file(file, reason="Path is a folder.")
            elif os.path.islink(file):
                self.backup_file(file, reason="Path is a symbolic link.")
            else:
                self.update_permission(file, chmod=0o644)
                metadata_files.append(file.replace(f"{self.study_metadata_files_path}/", ""))

        return metadata_files

    def maintain_unwanted_files(self, recursive=False):
        if recursive:
            file_refs = pathlib.Path(self.study_metadata_files_path).rglob(".*")
        else:
            file_refs = pathlib.Path(self.study_metadata_files_path).glob(".*")

        for file in file_refs:
            if file.name.startswith(".nfs"):
                continue
            self.backup_file(file, reason="Hidden file", force_delete=True)
            type = "folder" if file.is_dir() else "file"
            action_log = MaintenanceActionLog(
                item=str(file),
                action=MaintenanceAction.MOVE,
                parameters={},
                message=f"{self.study_id}: {str(file)} is hidden {type}. It was removed.",
            )
            self.actions.append(action_log)

    def update_permission(self, file_path, chmod=0o644):
        octal_value = oct(os.stat(file_path).st_mode & 0o777)
        octal_value_str = octal_value.replace("0o", "")
        chmod_str = oct(chmod & 0o777).replace("0o", "")

        current_chmod = int(octal_value, 8)

        if current_chmod != int(chmod & 0o777):
            os.chmod(file_path, mode=chmod)
            action_log = MaintenanceActionLog(
                item=file_path,
                action=MaintenanceAction.UPDATE_FILE_PERMISSION,
                parameters={"old_value": octal_value_str, "new_value": chmod_str},
                message=f"{self.study_id}: {file_path} file permission was updated from {octal_value_str} to {chmod_str}.",
            )
            self.actions.append(action_log)

    def _create_folder_future_actions(
        self,
        file_path: str,
        mode: int,
        backup_path: str,
        created_folders: Dict[str, int],
        deleted_folders: Set[str],
        backup_path_permission=0o750,
    ):
        study_id = self.study_id
        chmod_str = oct(mode).replace("0o", "")
        if os.path.exists(file_path) and not os.path.isdir(file_path):
            file_basename = os.path.basename(file_path)
            backup_path_permission_str = oct(backup_path_permission_str).replace("0o", "")
            if not os.path.exists(backup_path) and backup_path not in created_folders:
                action_log = MaintenanceActionLog(
                    item=backup_path,
                    action=MaintenanceAction.CREATE,
                    parameters={"chmod": backup_path_permission_str},
                    message=f"{study_id}: {backup_path} folder will be created.",
                    command=f"mkdir -p -m {backup_path_permission_str} '{backup_path}'",
                )
                
                if self.apply_future_actions:
                    os.makedirs(backup_path, mode=mode, exist_ok=True)
                    self.actions.append(action_log)
                else:
                    self.future_actions.append(action_log)
                created_folders[backup_path] = backup_path_permission_str

            target_path = os.path.join(backup_path, file_basename)
            # shutil.move(file_path, target_path)

            action_log = MaintenanceActionLog(
                item=file_path,
                action=MaintenanceAction.MOVE,
                parameters={"target": target_path},
                message=f"{study_id}: {file_path} is not folder. Existing will be moved to {target_path}",
                command=f"mv '{file_path}' '{target_path}'",
            )
            if self.apply_future_actions:
                shutil.move(file_path, target_path)
                self.actions.append(action_log)
            else:
                self.future_actions.append(action_log)
            created_folders[target_path] = chmod_str
            deleted_folders.add(file_path)
        elif os.path.exists(file_path) and os.path.isdir(file_path):
            if file_path not in created_folders:
                octal_value = oct(os.stat(file_path).st_mode & 0o777)
            else:
                octal_value = created_folders[file_path]

            current_chmod = int(octal_value, 8)
            if current_chmod != int(mode & 0o777):
                octal_value_str = octal_value.replace("0o", "")
                # os.chmod(file_path, mode=mode)

                if self.apply_future_actions:
                    self.update_permission(file_path, mode)
                else:
                    action_log = MaintenanceActionLog(
                        item=file_path,
                        action=MaintenanceAction.UPDATE_FILE_PERMISSION,
                        parameters={"chmod": chmod_str},
                        message=f"{self.study_id}: {file_path} file permission will be updated from {octal_value_str} to {chmod_str}.",
                        command=f"chmod {chmod_str} '{file_path}'",
                    )
                    self.future_actions.append(action_log)

        if (not os.path.exists(file_path) or file_path in deleted_folders) and file_path not in created_folders:
            action_log = MaintenanceActionLog(
                item=file_path,
                action=MaintenanceAction.CREATE,
                parameters={"chmod": chmod_str},
                message=f"{study_id}: {file_path} folder will be created.",
                command=f"mkdir -p -m {chmod_str} '{file_path}'",
            )
            if self.apply_future_actions:
                os.makedirs(file_path, mode)
                self.actions.append(action_log)
            else:
                self.future_actions.append(action_log)
            created_folders[file_path] = chmod_str

    def calculate_readonly_study_folders_future_actions(self):
        settings = self.settings
        study_id = self.study_id
        readonly_storage_recycle_bin_path = self.readonly_storage_recycle_bin_path
        created_folders = {}
        deleted_folders = set()
        readonly_files_path = os.path.join(settings.study_readonly_files_root_path, study_id)
        self._create_folder_future_actions(
            readonly_files_path, 0o750, readonly_storage_recycle_bin_path, created_folders, deleted_folders
        )

        readonly_metadata_path = os.path.join(settings.study_readonly_metadata_files_root_path, study_id)
        self._create_folder_future_actions(
            readonly_metadata_path, 0o750, readonly_storage_recycle_bin_path, created_folders, deleted_folders
        )

        readonly_audit_path = os.path.join(settings.study_readonly_audit_files_root_path, study_id)
        self._create_folder_future_actions(
            readonly_audit_path, 0o750, readonly_storage_recycle_bin_path, created_folders, deleted_folders
        )

        readonly_public_metadata_versions_path = os.path.join(
            settings.study_readonly_public_metadata_versions_root_path, study_id
        )
        self._create_folder_future_actions(
            readonly_public_metadata_versions_path,
            0o750,
            readonly_storage_recycle_bin_path,
            created_folders,
            deleted_folders,
        )

        readonly_integrity_check_files_path = os.path.join(
            settings.study_readonly_integrity_check_files_root_path, study_id
        )
        self._create_folder_future_actions(
            readonly_integrity_check_files_path,
            0o750,
            readonly_storage_recycle_bin_path,
            created_folders,
            deleted_folders,
        )

    def maintain_rw_storage_folders(self):
        settings = self.settings
        study_id = self.study_id
        study_settings = self.study_settings
        rw_storage_recycle_bin_path = os.path.join(self.rw_storage_recycle_bin_path, self.study_id)
        study_recycle_bin_path = self.study_recycle_bin_path

        audit_path = os.path.join(settings.study_audit_files_root_path, study_id)
        self._create_rw_storage_folder(audit_path, 0o755, rw_storage_recycle_bin_path)

        internal_file_path = os.path.join(settings.study_internal_files_root_path, study_id)
        self._create_rw_storage_folder(internal_file_path, 0o755, rw_storage_recycle_bin_path)

        metadata_path = os.path.join(settings.study_metadata_files_root_path, study_id)
        self._create_rw_storage_folder(metadata_path, 0o755, rw_storage_recycle_bin_path)

        internal_backup_folder_path = os.path.join(audit_path, study_settings.internal_backup_folder_name)
        self._create_rw_storage_folder(internal_backup_folder_path, 0o755, rw_storage_recycle_bin_path)

        study_audit_folder_path = os.path.join(audit_path, study_settings.audit_folder_name)
        self._create_rw_storage_folder(study_audit_folder_path, 0o755, study_recycle_bin_path)

        log_path = os.path.join(internal_file_path, study_settings.internal_logs_folder_name)
        self._create_rw_storage_folder(log_path, 0o777, study_recycle_bin_path)

        read_only_files_path = os.path.join(settings.study_readonly_files_root_path, study_id)
        readonly_files_symbolic_link_path = os.path.join(
            settings.study_metadata_files_root_path, study_id, study_settings.readonly_files_symbolic_link_name
        )
        audit_folder_symbolic_link_path: str = os.path.join(
            settings.study_metadata_files_root_path, study_id, study_settings.audit_files_symbolic_link_name
        )
        internal_file_symbolic_link_path: str = os.path.join(
            settings.study_metadata_files_root_path, study_id, study_settings.internal_files_symbolic_link_name
        )

        self.maintain_study_symlinks(read_only_files_path, readonly_files_symbolic_link_path)
        self.maintain_study_symlinks(study_audit_folder_path, audit_folder_symbolic_link_path)
        self.maintain_study_symlinks(internal_file_path, internal_file_symbolic_link_path)

    def maintain_study_symlinks(self, target_path, link_path):
        study_id = self.study_id
        if os.path.islink(link_path):
            current_target = os.readlink(link_path)
            if current_target == target_path:
                return
            else:
                os.unlink(link_path)

        if os.path.exists(link_path):
            self.backup_file(link_path, reason=f"{link_path} is not symbolic link.")

            os.symlink(target_path, link_path)
            action_log = MaintenanceActionLog(
                item=link_path,
                action=MaintenanceAction.CREATE,
                parameters={"target": target_path},
                message=f"{study_id}: {link_path} symbolic link is created to point {target_path} folder.",
            )
            self.actions.append(action_log)
        else:
            os.symlink(target_path, link_path)
            action_log = MaintenanceActionLog(
                item=link_path,
                action=MaintenanceAction.CREATE,
                parameters={"target": target_path},
                message=f"{study_id}: {link_path} symbolic link is created to point {target_path} folder.",
            )
            self.actions.append(action_log)

    def _create_rw_storage_folder(self, file_path: str, mode: int, backup_path: str):
        study_id = self.study_id

        if os.path.exists(file_path) and not os.path.isdir(file_path):
            file_basename = os.path.basename(file_path)
            if not os.path.exists(backup_path):
                os.makedirs(backup_path, exist_ok=True)
                action_log = MaintenanceActionLog(
                    item=backup_path,
                    action=MaintenanceAction.CREATE,
                    parameters={},
                    message=f"{study_id}: {backup_path} folder was created.",
                )
                self.actions.append(action_log)

            target_path = os.path.join(backup_path, file_basename)
            shutil.move(file_path, target_path)

            action_log = MaintenanceActionLog(
                item=file_path,
                action=MaintenanceAction.MOVE,
                parameters={"target": target_path},
                message=f"{study_id}: {file_path} is not folder. Existing file was moved to {target_path}",
            )
            self.actions.append(action_log)

        if not os.path.exists(file_path):
            os.makedirs(file_path, mode=mode, exist_ok=True)
            action_log = MaintenanceActionLog(
                item=file_path,
                action=MaintenanceAction.CREATE,
                parameters={},
                message=f"{study_id}: {file_path} folder was created.",
            )
            self.actions.append(action_log)
        self.update_permission(file_path, chmod=mode)

    def maintain_investigation_file(self):
        study_id = self.study_id
        study_settings = self.study_settings
        investigation_file_path = os.path.join(self.study_metadata_files_path, study_settings.investigation_file_name)
        temaplate_investigation_file_path = os.path.join(
            study_settings.study_default_template_path, study_settings.investigation_file_name
        )
        investigation_file_candidates = glob.glob(os.path.join(self.study_metadata_files_path, "i_*.txt"))

        selected_investigation_file = None
        if os.path.exists(investigation_file_path) and os.stat(investigation_file_path).st_size == 0:
            self.backup_file(investigation_file_path, reason="Investigation file size is empty.")

        if os.path.exists(investigation_file_path):
            selected_investigation_file = investigation_file_path
        else:
            for file in investigation_file_candidates:
                basename = os.path.basename(file)
                # select first case insensitive investigation file
                if basename.lower() == study_settings.investigation_file_name.lower():
                    selected_investigation_file = file
                    shutil.move(file, investigation_file_path)
                    action_log = MaintenanceActionLog(
                        item=file,
                        action=MaintenanceAction.RENAME,
                        parameters={"target": investigation_file_path},
                        message=f"{study_id}: Investifation file {file} was selected and renamed as {investigation_file_path}",
                    )
                    self.actions.append(action_log)
                    break

        if selected_investigation_file:
            if selected_investigation_file in investigation_file_candidates:
                investigation_file_candidates.remove(selected_investigation_file)
            # if self.study_status == StudyStatus.INREVIEW or self.study_status == StudyStatus.PUBLIC:
            #     for file in investigation_file_candidates:
            #         self.backup_file(file, reason="Investigation file name is not invalid.")
        else:
            shutil.copy(temaplate_investigation_file_path, investigation_file_path)
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.COPY,
                parameters={"from": temaplate_investigation_file_path},
                message=f"{study_id}: {study_settings.investigation_file_name} does not exist. File was copied from {temaplate_investigation_file_path}",
            )
            self.actions.append(action_log)

        try:
            investigation = self.load_investigation_file(investigation_file_path)

            if investigation and investigation.studies and investigation.studies[0]:
                self.referenced_metadata_file_list.append(self.investigation_file_name)
                return investigation
            else:
                raise MaintenanceException(message=f"{study_settings.investigation_file_name} is empty or has no study.")
        except Exception as ex:
            if isinstance(ex, MaintenanceException):
                message = ex.message
            else:
                message = (
                    f"{study_id}: {study_settings.investigation_file_name} could not be loaded. Error: {self.sanitise(ex)}"
                )
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.ERROR_MESSAGE,
                parameters={},
                message=message,
                successful=False,
            )
            self.actions.append(action_log)
            raise ex

    def maintain_investigation_file_content(self, investigation: isatools_model.Investigation):
        investigation_file_path = os.path.join(self.study_metadata_files_path, self.investigation_file_name)
        study: isatools_model.Study = investigation.studies[0]

        # patch for isaatools
        for item in study.factors:
            factor: isatools_model.StudyFactor = item
            if not factor.factor_type:
                factor.factor_type = isatools_model.OntologyAnnotation(term=factor.name)
                action_log = MaintenanceActionLog(
                    item=investigation_file_path,
                    action=MaintenanceAction.UPDATE_CONTENT,
                    parameters={"field": "study factor term", "old_value": "", "new_value": factor.name},
                    message=f"{self.study_id}: {investigation_file_path} study identifer was updated from '' to '{factor.name}'",
                )
                self.actions.append(action_log)

        if self.study_id != study.identifier:
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={"field": "identifier", "old_value": study.identifier, "new_value": self.study_id},
                message=f"{self.study_id}: {investigation_file_path} study identifer was updated from '{study.identifier}' to '{self.study_id}'",
            )
            self.actions.append(action_log)
            study.identifier = self.study_id  # Adding the study identifier

        if self.study_id != investigation.identifier:
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={
                    "field": "investigation identifier",
                    "old_value": investigation.identifier if investigation.identifier else "",
                    "new_value": self.study_id,
                },
                message=f"{self.study_id}: {investigation_file_path} investigation identifer was updated from '{investigation.identifier}' to '{self.study_id}'",
            )
            self.actions.append(action_log)
            investigation.identifier = self.study_id  # Adding the study identifier

        old_value = study.public_release_date
        new_value = self.public_release_date.strftime("%Y-%m-%d")
        if old_value != new_value:
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={
                    "field": "public_release_date",
                    "old_value": study.public_release_date,
                    "new_value": new_value,
                },
                message=f"{self.study_id}: {investigation_file_path} study public release date was updated from '{study.public_release_date}' to '{new_value}'",
            )
            self.actions.append(action_log)
            study.public_release_date = new_value

        old_value = investigation.public_release_date
        if old_value != new_value:
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={
                    "field": "investigation public_release_date",
                    "old_value": investigation.public_release_date,
                    "new_value": new_value,
                },
                message=f"{self.study_id}: {investigation_file_path} investigation public release date was updated from '{investigation.public_release_date}' to '{new_value}'",
            )
            self.actions.append(action_log)
            investigation.public_release_date = new_value

        old_value = study.submission_date
        new_value = self.submission_date.strftime("%Y-%m-%d")
        if old_value != new_value:
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={"field": "submission_date", "old_value": study.submission_date, "new_value": new_value},
                message=f"{self.study_id}: {investigation_file_path} study submission date was updated from '{study.submission_date}' to '{new_value}'",
            )
            self.actions.append(action_log)
            study.submission_date = new_value

        old_value = investigation.submission_date
        if old_value != new_value:
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={
                    "field": "investigation submission_date",
                    "old_value": investigation.submission_date,
                    "new_value": new_value,
                },
                message=f"{self.study_id}: {investigation_file_path} investigation submission date was updated from '{investigation.submission_date}' to {new_value}",
            )
            self.actions.append(action_log)
            investigation.submission_date = new_value

        old_value = study.title
        new_value = StudyFolderMaintenanceTask.clear_html_text(study.title)
        if old_value != new_value:
            old_value_to_print = old_value.replace("\n", "<newline>")
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={"field": "title", "old_value": old_value_to_print, "new_value": new_value},
                message=f"{self.study_id}: {investigation_file_path} study title was updated from '{old_value_to_print}' to '{new_value}'",
            )
            self.actions.append(action_log)
            study.title = new_value

        old_value = investigation.title
        new_value = StudyFolderMaintenanceTask.clear_html_text(investigation.title)
        if old_value != new_value:
            old_value_to_print = old_value.replace("\n", "<newline>")
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={"field": "investigation title", "old_value": old_value_to_print, "new_value": new_value},
                message=f"{self.study_id}: {investigation_file_path} study title was updated from '{old_value_to_print}' to '{new_value}'",
            )
            self.actions.append(action_log)
            investigation.title = new_value

    @staticmethod
    def clear_html_text(text):
        if not text:
            return ""

        cleantext = BeautifulSoup(text, "lxml").text
        cleantext = " ".join(cleantext.split())
        return cleantext.strip()

    def backup_file(self, file_path, reason=None, prefix=None, backup_path=None, force_delete=True):
        if os.path.exists(file_path):
            basename = os.path.basename(file_path)
            if self.delete_unreferenced_metadata_files or force_delete:
                # move if target file exist before renaming other file
                if not backup_path:
                    backup_path = self.study_recycle_bin_path
                if os.path.exists(backup_path) and not os.path.isdir(backup_path):
                    basename = os.path.basename(backup_path)
                    dirname = os.path.basename(backup_path)
                    renamed_basename = f"{self.task_name}_{basename}"
                    renamed_backup_path = os.path.join(dirname, renamed_basename)
                    shutil.move(backup_path, renamed_backup_path)
                    action_log = MaintenanceActionLog(
                        item=backup_path,
                        action=MaintenanceAction.RENAME,
                        parameters={"target": renamed_backup_path},
                        message=f"{self.study_id}: {backup_path} is not folder. Current file was renamed to {renamed_backup_path}.",
                    )
                    self.actions.append(action_log)

                if not os.path.exists(backup_path):
                    os.makedirs(backup_path, exist_ok=True)
                    action_log = MaintenanceActionLog(
                        item=backup_path,
                        action=MaintenanceAction.CREATE,
                        parameters={},
                        message=f"{self.study_id}: {backup_path} folder was created.",
                    )
                    self.actions.append(action_log)
                target_path = os.path.join(backup_path, basename)
                target_dir = os.path.dirname(target_path)
                os.makedirs(target_dir, exist_ok=True)
                shutil.move(file_path, target_path)  # move current assay file
                action_log = MaintenanceActionLog(
                    item=file_path,
                    action=MaintenanceAction.MOVE,
                    parameters={"target": target_path},
                    message=f"{self.study_id}: Current path {file_path} was moved to {target_path}{(' ' + reason) if reason else ''}",
                )
                self.actions.append(action_log)
            else:
                # rename if target file exist before renaming other file
                if prefix:
                    renamed_basename = f"{prefix}_{self.task_name}_{basename}"
                else:
                    renamed_basename = f"{self.task_name}_{basename}"
                dirname = os.path.dirname(file_path)
                target_path = os.path.join(dirname, renamed_basename)
                shutil.move(file_path, target_path)  # move current assay file

                action_log = MaintenanceActionLog(
                    item=file_path,
                    action=MaintenanceAction.RENAME,
                    parameters={"target": target_path},
                    message=f"{self.study_id}: Current path {file_path} was renamed as {renamed_basename}.{(' ' + reason) if reason else ''}",
                )
                self.actions.append(action_log)

    def maintain_sample_file(self, investigation: isatools_model.Investigation):
        study_id = self.study_id
        study_template_path = self.study_settings.study_default_template_path
        short_sample_file_name = "s_" + study_id.upper() + ".txt"
        default_sample_file_path = os.path.join(self.study_metadata_files_path, short_sample_file_name)
        investigation_file_path = os.path.join(self.study_metadata_files_path, self.investigation_file_name)
        temaplate_sample_file_path = os.path.join(study_template_path, self.study_settings.template_sample_file_name)
        if investigation and investigation.studies and investigation.studies[0]:
            study: isatools_model.Study = investigation.studies[0]
            sample_file_path = os.path.join(self.study_metadata_files_path, study.filename)
            if (
                study.filename.strip()
                and os.path.exists(sample_file_path)
                and sample_file_path != default_sample_file_path
            ):
                if os.path.exists(default_sample_file_path):
                    self.backup_file(
                        default_sample_file_path,
                        reason=f"Other sample file will be renamed to {short_sample_file_name}",
                    )

                shutil.move(sample_file_path, default_sample_file_path)  # Rename the sample file
                action_log = MaintenanceActionLog(
                    item=sample_file_path,
                    action=MaintenanceAction.RENAME,
                    parameters={"target": default_sample_file_path},
                    message=f"{study_id}: study file was renamed to {default_sample_file_path}",
                )
                self.actions.append(action_log)

            if study.filename != short_sample_file_name:
                action_log = MaintenanceActionLog(
                    item=investigation_file_path,
                    action=MaintenanceAction.UPDATE_CONTENT,
                    parameters={
                        "field": "study_filename",
                        "old_value": study.filename,
                        "new_value": short_sample_file_name,
                    },
                    message=f"{self.study_id}: {investigation_file_path} study filename was updated from {study.filename} to {short_sample_file_name}",
                )
                self.actions.append(action_log)
                study.filename = short_sample_file_name

        sample_files_list = glob.glob(os.path.join(self.study_metadata_files_path, "s_*.txt"))

        if os.path.exists(default_sample_file_path):
            size = os.stat(default_sample_file_path).st_size
            if size <= 200:
                self.backup_file(
                    default_sample_file_path,
                    reason=f" {short_sample_file_name} file size is less than minimum sample file size. Current file size in byte: {size}.",
                )

        if not os.path.exists(default_sample_file_path):
            shutil.copy(temaplate_sample_file_path, default_sample_file_path)  # copy from template
            action_log = MaintenanceActionLog(
                item=default_sample_file_path,
                action=MaintenanceAction.COPY,
                parameters={"from": temaplate_sample_file_path},
                message=f"{study_id}: study file was copied from  {temaplate_sample_file_path}",
            )
            self.actions.append(action_log)
        self.referenced_metadata_file_list.append(short_sample_file_name)

        if default_sample_file_path in sample_files_list:
            sample_files_list.remove(default_sample_file_path)
        # if self.study_status == StudyStatus.INREVIEW or self.study_status == StudyStatus.PUBLIC:
        #     for file in sample_files_list:
        #         self.backup_file(file, reason="Sample file is not referenced.")

    def maintain_sample_file_content(self, investigation: isatools_model.Investigation):
        filename = investigation.studies[0].filename
        sample_file_path = os.path.join(self.study_metadata_files_path, filename)
        sample_df = None
        if not os.path.exists(sample_file_path):
            return

        try:
            sample_df = self.read_tsv_file(sample_file_path)
            if self.fix_tsv_file_headers:
                if sample_df is not None and not sample_df.empty:
                    organism_part_column_name = "Characteristics[Organism part]"
                    variant_column_name = "Characteristics[Variant]"
                    sample_type_column_name = "Characteristics[Sample type]"
                    self.insert_ontology_column(
                        sample_file_path, sample_df, organism_part_column_name, variant_column_name
                    )
                    self.insert_ontology_column(
                        sample_file_path, sample_df, variant_column_name, sample_type_column_name
                    )
        finally:
            if sample_df is not None:
                self.write_tsv_file(sample_df, sample_file_path)

    def insert_ontology_column(self, file_path, df: pd.DataFrame, previous_column_name, new_column_name):
        term_source_ref = "Term Source REF"
        term_acession_number = "Term Accession Number"

        term_source_ref_column_count = 0
        term_acession_number_count = 0
        for column in df.columns:
            if column.startswith(term_source_ref):
                term_source_ref_column_count += 1
            elif column.startswith(term_acession_number):
                term_acession_number_count += 1
        max_number = max(term_acession_number_count, term_source_ref_column_count)

        if new_column_name not in df.columns:
            if previous_column_name in df.columns:
                idx = df.columns.get_loc(previous_column_name)
                if len(df.columns) > (idx + 2):
                    if df.columns[idx + 1].startswith(term_source_ref) and df.columns[idx + 2].startswith(
                        term_acession_number
                    ):
                        df.insert(idx + 3, new_column_name, "")
                        df.insert(idx + 4, f"{term_source_ref}.{max_number}", "")
                        df.insert(idx + 5, f"{term_acession_number}.{(max_number)}", "")
                        action_log = MaintenanceActionLog(
                            item=file_path,
                            action=MaintenanceAction.UPDATE_CONTENT,
                            parameters={"new_column": new_column_name},
                            message=f"{self.study_id}: New column {new_column_name} was added into {file_path}",
                        )
                        self.actions.append(action_log)
                        return True
        return False

    def sanitise_filename(self, filename: str):
        if not filename or not filename.strip():
            return ""
        result = re.sub("[^/a-zA-Z0-9_.-]", "_", filename.strip())
        return result

    def write_tsv_file(self, dataframe: pd.DataFrame, file_name):
        try:
            # Remove all ".n" numbers at the end of duplicated column names
            dataframe.rename(columns=lambda x: re.sub(r"\.[0-9]+$", "", x), inplace=True)

            # Write the new row back in the file
            dataframe.to_csv(file_name, sep="\t", encoding="utf-8", index=False)
        except Exception as ex:
            logger.error(f"Error: Could not write/update the file {file_name}. {self.sanitise(ex)}")
            action_log = MaintenanceActionLog(
                item=file_name,
                action=MaintenanceAction.ERROR_MESSAGE,
                parameters={},
                message=f"{self.study_id}: {file_name} could not saved. {self.sanitise(ex)}",
                successful=False,
            )
            self.actions.append(action_log)
            raise ex

    def sanitise_metadata_filename(self, study_id, filename, prefix: str = "a_"):
        new_name = self.sanitise_filename(filename)
        if study_id.lower() in new_name:
            new_name = new_name.replace(study_id.lower(), study_id)
        if not new_name.startswith(f"{prefix}{study_id}"):
            new_name = new_name[len(prefix) :]
            parse = new_name.split("_")
            will_be_deleted = []
            for i in range(len(parse)):
                if parse[i].lower().startswith("mtbl"):
                    will_be_deleted.append(parse[i])
            for item in will_be_deleted:
                parse.remove(item)
            new_name = f"{prefix}{study_id}_{'_'.join(parse)}"
        return new_name

    def maintain_assay_files(self, investigation: isatools_model.Investigation):
        study_id = self.study_id

        if investigation and investigation.studies and investigation.studies[0]:
            study: isatools_model.Study = investigation.studies[0]
            assay_file_list = glob.glob(os.path.join(self.study_metadata_files_path, "a_*.txt"))
            referenced_assays = {}
            investigation_file_path = os.path.join(
                self.study_metadata_files_path, self.investigation_file_name
            )

            if study.assays:
                invalid_assays = []
                for item in study.assays:
                    assay: isatools_model.Assay = item
                    if not assay.filename or not assay.filename.strip():
                        action_log = MaintenanceActionLog(
                            item=investigation_file_path,
                            action=MaintenanceAction.UPDATE_CONTENT,
                            parameters={},
                            message=f"{study_id}: Study has an assay with empty filename. Empty assay was removed from study in {self.investigation_file_name}.",
                        )
                        self.actions.append(action_log)
                        invalid_assays.append(assay)

                for invalid_assay in invalid_assays:
                    study.assays.remove(invalid_assay)

                for item in study.assays:
                    assay: isatools_model.Assay = item

                    sanitized_filename = self.sanitise_metadata_filename(study_id, assay.filename, prefix="a_")

                    assay_file_path = os.path.join(self.study_metadata_files_path, assay.filename)
                    sanitized_file_path = os.path.join(self.study_metadata_files_path, sanitized_filename)
                    referenced_assays[assay.filename] = assay_file_path
                    if (
                        sanitized_filename
                        and os.path.exists(assay_file_path)
                        and assay_file_path != sanitized_file_path
                    ):
                        if os.path.exists(sanitized_file_path):
                            self.backup_file(
                                sanitized_file_path,
                                reason=f"Other assay file will be renamed to {os.path.basename(sanitized_file_path)}.",
                            )

                        shutil.move(assay_file_path, sanitized_file_path)  # Rename assay file name
                        action_log = MaintenanceActionLog(
                            item=assay_file_path,
                            action=MaintenanceAction.RENAME,
                            parameters={"target": sanitized_file_path},
                            message=f"{study_id}: {assay_file_path} file was renamed to {sanitized_file_path}",
                        )
                        self.actions.append(action_log)

                        action_log = MaintenanceActionLog(
                            item=investigation_file_path,
                            action=MaintenanceAction.UPDATE_CONTENT,
                            parameters={
                                "field": "assay file name",
                                "old_value": assay.filename,
                                "new_value": sanitized_filename,
                            },
                            message=f"{self.study_id}: {investigation_file_path} study assay file name was updated: Field: 'assay file name' old: {assay.filename} new: {sanitized_filename}",
                        )
                        self.actions.append(action_log)
                        assay.filename = sanitized_filename
                    self.referenced_metadata_file_list.append(assay.filename)

            for referenced_assay in referenced_assays:
                assay_file_path = referenced_assays[referenced_assay]
                if assay_file_path in assay_file_list:
                    assay_file_list.remove(assay_file_path)

        # if self.study_status == StudyStatus.INREVIEW or self.study_status == StudyStatus.PUBLIC:
        #     for file in assay_file_list:
        #         basename = os.path.basename(file)
        #         self.backup_file(file, reason=f"Assay file {basename} is not referenced.")

    def maintain_assay_file_data_columns(self, investigation, assay_file_path, assay_df: pd.DataFrame):
        scan_polarity_name = "Parameter Value[Scan polarity]"

        updates = {}

        def update_scan_polarity(x):
            if not x:
                return ""
            if x.lower().startswith("neg"):
                if x != "negative":
                    updates[x] = "negative"
                return "negative"
            elif x.lower().startswith("pos"):
                if x != "positive":
                    updates[x] = "positive"
                return "negative"
            elif x.lower().startswith("alt"):
                if x != "alternating":
                    updates[x] = "alternating"
                return "alternating"
            return x

        if scan_polarity_name in assay_df.columns:
            assay_df[scan_polarity_name] = assay_df[scan_polarity_name].apply(lambda x: update_scan_polarity(x))

        if updates:
            updated_values = "; ".join([f"'{x}'->'{updates[x]}'" for x in updates])
            action_log = MaintenanceActionLog(
                item=assay_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={"updates": updated_values},
                message=f"{self.study_id}: {assay_file_path} file column '{scan_polarity_name}' values are updated: '{updated_values}'",
            )
            self.actions.append(action_log)

        non_empty_columns = {
            "Data Transformation Name",
            "Extract Name",
            "MS Assay Name",
            "NMR Assay Name",
            "Normalization Name",
        }
        sample_name = "Sample Name"
        if sample_name in assay_df.columns:
            empty_values = assay_df[assay_df[sample_name] == ""].index
            # non_values = assay_df[assay_df[sample_name] == ''].index
            uniques = pd.unique(assay_df[sample_name])
            if len(assay_df.index) == uniques.size and empty_values.size == 0:
                print("al")

                for col in non_empty_columns:
                    if col in assay_df.columns:
                        values = assay_df[assay_df[col] == ""].index

                        if values.size > 0:
                            assay_df[col] = assay_df[sample_name]

    def maintain_maf_file_content(self, investigation, assignment_file_path, maf_df: pd.DataFrame):
        pass

    def maintain_assay_file_content(self, investigation):
        study_id = self.study_id
        assignment_column_name = "Metabolite Assignment File"
        if investigation and investigation.studies and investigation.studies[0]:
            study: isatools_model.Study = investigation.studies[0]
            referenced_assignment_file_paths = set()
            referenced_assignment_files = set()
            if study.assays:
                for item in study.assays:
                    assay: isatools_model.Assay = item
                    assay_file_path = os.path.join(self.study_metadata_files_path, assay.filename)
                    if not assay.filename or not assay.filename.strip() or not os.path.exists(assay_file_path):
                        error = True
                    else:
                        assay_df = None
                        try:
                            assay_df = self.read_tsv_file(assay_file_path)
                            if assay_df is not None and not assay_df.empty:
                                if self.fix_assay_file_column_values:
                                    self.maintain_assay_file_data_columns(investigation, assay_file_path, assay_df)

                                if assignment_column_name in assay_df.columns:
                                    assay_assignment_files = assay_df[assignment_column_name].unique()
                                    for assignment_file in assay_assignment_files:
                                        if assignment_file:
                                            if assignment_file.startswith("m_") and assignment_file.endswith(".tsv"):
                                                (
                                                    assignment_file_path,
                                                    new_assignment_file,
                                                ) = self.maintain_metabolite_assignment_file_column_in_assay(
                                                    assay_file_path, assay_df, assignment_column_name, assignment_file
                                                )
                                                referenced_assignment_file_paths.add(assignment_file_path)
                                                referenced_assignment_files.add(new_assignment_file)
                                            else:
                                                action_log = MaintenanceActionLog(
                                                    item=assay_file_path,
                                                    action=MaintenanceAction.ERROR_MESSAGE,
                                                    parameters={},
                                                    message=f"{study_id}: ERROR: 'Metabolite Assignment File' {assignment_file} in assay file {assay_file_path} is not valid.",
                                                    successful=False,
                                                )
                                                self.actions.append(action_log)
                                else:
                                    action_log = MaintenanceActionLog(
                                        item=assay_file_path,
                                        action=MaintenanceAction.ERROR_MESSAGE,
                                        parameters={},
                                        message=f"{study_id}: ERROR: Assay file {assay_file_path} does not have  {assignment_column_name} column.",
                                        successful=False,
                                    )
                                    self.actions.append(action_log)
                                self.maintain_referenced_data_file_column_values(assay_file_path, assay_df)
                        finally:
                            if assay_df is not None and not assay_df.empty:
                                self.write_tsv_file(assay_df, assay_file_path)

            for assignment_file_path in referenced_assignment_file_paths:
                if not os.path.exists(assignment_file_path):
                    error = True
                else:
                    try:
                        if os.path.exists(assignment_file_path):
                            maf_df = self.read_tsv_file(assignment_file_path)
                            if maf_df is not None and not maf_df.empty:
                                self.maintain_maf_file_content(investigation, assignment_file_path, maf_df)
                    finally:
                        if maf_df is not None and not maf_df.empty:
                            self.write_tsv_file(maf_df, assignment_file_path)
            for assignment_file in referenced_assignment_files:
                self.referenced_metadata_file_list.append(assignment_file)

            # if not error:
            #     if self.study_status == StudyStatus.INREVIEW or self.study_status == StudyStatus.PUBLIC:
            #         assignment_file_list = glob.glob(os.path.join(self.study_metadata_files_path, "m_*.tsv"))
            #         for file in assignment_file_list:
            #             if file not in referenced_assignment_file_paths:
            #                 basename = os.path.basename(file)
            #                 self.backup_file(file, reason=f"Metabolite Assignment File {basename} was not referenced.")

    def read_tsv_file(self, file_path) -> pd.DataFrame:
        message = ""

        df = pd.DataFrame()  # Empty file
        try:
            # Enforce str datatype for all columns we read from ISA-Tab tables
            col_names = pd.read_csv(file_path, sep="\t", nrows=0).columns
            types_dict = {col: str for col in col_names}
            try:
                if os.path.getsize(file_path) == 0:  # Empty file
                    logger.error("File is empty " + file_path)
                else:
                    df = pd.read_csv(file_path, sep="\t", header=0, encoding="utf-8", dtype=types_dict)
            except Exception as e:  # Todo, should check if the file format is Excel. ie. not in the exception handler
                if os.path.getsize(file_path) > 0:
                    df = pd.read_csv(
                        file_path, sep="\t", header=0, encoding="ISO-8859-1", dtype=types_dict
                    )  # Excel format
                    logger.info(
                        "File was opened with 'ISO-8859-1' encoding: " + file_path + ".\nPrevious error: " + str(e)
                    )
                else:
                    raise e
        except Exception as ex:
            logger.error("Could not read file " + file_path + ". " + self.sanitise(ex))
            message = self.sanitise(ex)

        if df is None:
            action_log = MaintenanceActionLog(
                item=file_path,
                action=MaintenanceAction.ERROR_MESSAGE,
                parameters={},
                message=f"{self.study_id}: ERROR: {file_path} tsv file could not be loaded or its content is not valid. {message}",
                successful=False,
            )
            self.actions.append(action_log)
            return None

        df = df.replace(np.nan, "", regex=True)  # Remove NaN

        if not self.fix_tsv_file_headers:
            return df

        updates = {}

        def trim_column_name(column_name):
            new_value = self.trim_parameter_column_name(column_name)
            if column_name != new_value:
                action_log = MaintenanceActionLog(
                    item=file_path,
                    action=MaintenanceAction.UPDATE_CONTENT,
                    parameters={"old_value": f"{column_name}", "new_value": f"{new_value}"},
                    message=f"{self.study_id}: {file_path} file column '{column_name}' is renamed to '{new_value}'",
                )
                self.actions.append(action_log)
            if new_value != column_name:
                updates[column_name] = new_value
            return new_value

        df.rename(columns=lambda x: trim_column_name(x), inplace=True)
        if updates:
            updated_values = "; ".join([f"'{x}'->'{updates[x]}'" for x in updates])
            action_log = MaintenanceActionLog(
                item=file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={"updates": updated_values},
                message=f"{self.study_id}: {file_path} file column names are updated: '{updated_values}'",
            )
            self.actions.append(action_log)

        return df

    def trim_parameter_column_name(self, name: str):
        name, ext = os.path.splitext(name)

        param = name.split("[")
        if len(param) > 1:
            name = f"{param[0].strip()}[{param[1].replace(']', '').strip()}]"
        else:
            name = param[0].strip()
        new_value = f"{name}{ext}"
        return new_value

    def maintain_referenced_data_file_column_values(self, assay_file_path, assay_df: pd.DataFrame):
        raw_spectral_column_name = "Raw Spectral Data File"
        derived_spectral_column_name = "Derived Spectral Data File"

        other_referenced_file_column_names = []
        raw_spectral_column_names = []
        derived_spectral_column_names = []
        all_data_columns = []
        for column in assay_df.columns:
            if column.startswith(raw_spectral_column_name):
                raw_spectral_column_names.append(column)
                all_data_columns.append(column)
            elif column.startswith(derived_spectral_column_name):
                derived_spectral_column_names.append(column)
                all_data_columns.append(column)
            elif " Data File" in column:
                other_referenced_file_column_names.append(column)
                all_data_columns.append(column)

        # if raw or derived file column does not exist, only add files prefix to column values
        if not raw_spectral_column_names or not derived_spectral_column_names or not self.fix_assay_file_column_values:
            for col in all_data_columns:
                assay_df[col] = assay_df[col].apply(lambda x: self.refactor_referenced_filename(x))
            return

        # detect invalid raw/derived file extensions and swap them. if needed, add new columns.
        move_to_derived_file_column_list = set()
        for raw_file_column in raw_spectral_column_names:
            raw_file_names = assay_df[raw_file_column].unique()
            for file_name in raw_file_names:
                for ext in DERIVED_FILE_EXTENSIONS:
                    if file_name.lower().endswith(ext):
                        move_to_derived_file_column_list.add(file_name)

        move_to_raw_file_column_list = set()
        for derived_column in derived_spectral_column_names:
            derived_file_names = assay_df[derived_column].unique()
            for derived_file_name in derived_file_names:
                for ext in RAW_FILE_EXTENSIONS:
                    if derived_file_name.lower().endswith(ext):
                        move_to_raw_file_column_list.add(derived_file_name)

        file_map = {}
        for index, row in assay_df.iterrows():
            raw_file_candidates = []

            derived_file_candidates = []
            moved_row_files = []

            moved_derived_files = []
            file_map[index] = (raw_file_candidates, derived_file_candidates)
            for column in derived_spectral_column_names:
                file_name = row[column]
                if file_name and file_name.strip():
                    if file_name in move_to_raw_file_column_list:
                        moved_row_files.append(file_name)
                    else:
                        derived_file_candidates.append(file_name)

            for column in raw_spectral_column_names:
                file_name = row[column]
                if file_name and file_name.strip():
                    if file_name in move_to_derived_file_column_list:
                        moved_derived_files.append(file_name)
                    else:
                        raw_file_candidates.append(file_name)
            derived_file_candidates.extend(moved_derived_files)
            raw_file_candidates.extend(moved_row_files)
        max_raw_column_count = 0
        max_derived_column_count = 0

        for index in file_map:
            (raw, derived) = file_map[index]

            if len(raw) > max_raw_column_count:
                max_raw_column_count = len(raw)
            if len(derived) > max_derived_column_count:
                max_derived_column_count = len(derived)

        if max_raw_column_count > len(raw_spectral_column_names):
            column_count = max_raw_column_count - len(raw_spectral_column_names)
            last_column_name = raw_spectral_column_names[-1]
            last_index = assay_df.columns.get_loc(last_column_name)
            for i in range(column_count):
                new_index = last_index + 1 + i
                name = f"{raw_spectral_column_name}.{(len(raw_spectral_column_names) + column_count + i - 1)}"
                assay_df.insert(new_index, name, "")
                raw_spectral_column_names.append(name)

        if max_derived_column_count > len(derived_spectral_column_names):
            column_count = max_derived_column_count - len(derived_spectral_column_names)
            last_column_name = derived_spectral_column_names[-1]
            last_index = assay_df.columns.get_loc(last_column_name)
            for i in range(column_count):
                new_index = last_index + 1 + i
                name = f"{derived_spectral_column_name}.{(len(derived_spectral_column_names) + column_count + i - 1)}"
                assay_df.insert(new_index, name, "")
                derived_spectral_column_names.append(name)

        for index, row in assay_df.iterrows():
            raw_file_candidates = file_map[index][0]
            derived_file_candidates = file_map[index][1]

            for i in range(len(other_referenced_file_column_names)):
                row[other_referenced_file_column_names[i]] = self.refactor_referenced_filename(
                    row[other_referenced_file_column_names[i]]
                )

            for i in range(len(raw_spectral_column_names)):
                if i < len(raw_file_candidates):
                    row[raw_spectral_column_names[i]] = self.refactor_referenced_filename(raw_file_candidates[i])
                else:
                    row[raw_spectral_column_names[i]] = ""

            for i in range(len(derived_spectral_column_names)):
                if i < len(derived_file_candidates):
                    row[derived_spectral_column_names[i]] = self.refactor_referenced_filename(
                        derived_file_candidates[i]
                    )
                else:
                    row[derived_spectral_column_names[i]] = ""

    def refactor_referenced_filename(self, filename: str):
        if not filename or not filename.strip():
            return ""
        # Absolute path is not allowed
        referenced_filename = filename.strip().strip("/")
        if referenced_filename.startswith(f"{self.study_settings.readonly_files_symbolic_link_name}/"):
            return referenced_filename
        else:
            return f"{self.study_settings.readonly_files_symbolic_link_name}/{referenced_filename}"

    def maintain_metabolite_assignment_file_column_in_assay(
        self, assay_file_path, assay_df, column_name, referenced_file_name, new_referenced_filename=None
    ):
        study_id = self.study_id
        basename = os.path.basename(referenced_file_name)
        dirname = os.path.dirname(referenced_file_name)
        if not new_referenced_filename:
            sanitized_file_basename = self.sanitise_metadata_filename(study_id, basename, prefix="m_")
            sanitized_filename = os.path.join(dirname, sanitized_file_basename)
        else:
            sanitized_filename = new_referenced_filename

        assignment_file_path = os.path.join(self.study_metadata_files_path, referenced_file_name)
        sanitized_file_path = os.path.join(self.study_metadata_files_path, sanitized_filename)

        if sanitized_filename and os.path.exists(assignment_file_path) and assignment_file_path != sanitized_file_path:
            if os.path.exists(sanitized_file_path):
                basename = os.path.basename(sanitized_file_path)
                self.backup_file(
                    sanitized_file_path, reason=f"Other metabolite assignment file will be renamed to {basename}"
                )

            shutil.move(assignment_file_path, sanitized_file_path)  # Rename assay file name
            action_log = MaintenanceActionLog(
                item=assignment_file_path,
                action=MaintenanceAction.RENAME,
                parameters={"target": sanitized_file_path},
                message=f"{study_id}: {assignment_file_path} file was renamed to {sanitized_file_path}",
            )
            self.actions.append(action_log)
            action_log = MaintenanceActionLog(
                item=assay_file_path,
                action=MaintenanceAction.UPDATE_CONTENT,
                parameters={
                    "field": column_name,
                    "old_value": referenced_file_name,
                    "new_value": sanitized_filename,
                },
                message=f"{self.study_id}: {assay_file_path} assay file content was updated: Field: {column_name}, old: {referenced_file_name} new: {sanitized_filename}",
            )
            self.actions.append(action_log)
            assay_df.loc[assay_df[column_name] == referenced_file_name, column_name] = sanitized_filename

        return sanitized_file_path, sanitized_filename

    def load_investigation_file(self, investigation_file_path: str, skip_load_tables=True, try_to_fix=True):
        i_filename = investigation_file_path
        try:
            with open(i_filename, encoding="utf-8", errors="ignore") as fp:
                # loading tables also load Samples and Assays
                isa_inv = isatab.load(fp, skip_load_tables)

                return isa_inv

        except Exception as e:
            with open(i_filename, "r") as fp:
                data = fp.readlines()
                current_data_line_count = 0
                expected_rows = {}
                unexpected_rows = {}

                line_number = 0
                for line in data:
                    line_number += 1
                    if not line.startswith("Comment") and line.strip():
                        current_data_line_count += 1
                        split = line.strip().split("\t")
                        if split and split[0]:
                            row_header = split[0]
                            if row_header not in INVESTIGATION_FILE_ROWS_SET:
                                if row_header not in unexpected_rows:
                                    unexpected_rows[row_header] = [line_number]
                                else:
                                    unexpected_rows[row_header].append(line_number)
                            else:
                                if row_header not in expected_rows:
                                    expected_rows[row_header] = [line_number]
                                else:
                                    expected_rows[row_header].append(line_number)
            unexpected_rows_messages = []
            if unexpected_rows:
                unexpected_rows_messages = [
                    f"Unexpected row '{x}' at line(s) {' '.join(str(unexpected_rows[x]))}" for x in unexpected_rows
                ]
            multiple_rows_messages = []
            if expected_rows:
                multiple_rows_messages = [
                    f"Multiple row '{x}'  at lines: {' '.join(str(expected_rows[x]))}"
                    for x in expected_rows
                    if len(expected_rows[x]) > 1
                ]
            missing = []
            for item in INVESTIGATION_FILE_ROWS_LIST:
                if item not in expected_rows:
                    missing.append(item)
            missing_item_messages = []
            if missing:
                missing_item_messages = [
                    f"Some expected rows were not found in investigation file: {', '.join(missing)}"
                ]
            all_messages = unexpected_rows_messages
            all_messages.extend(multiple_rows_messages)
            all_messages.extend(missing_item_messages)
            if all_messages:
                message = f"Error in {self.investigation_file_name} file. {'.'.join(all_messages)}"
                raise MaintenanceException(message=message)

            if try_to_fix:
                data = b""
                with open(i_filename, "rb") as fp:
                    data = fp.read()
                if b"\x00" in data or b"\xff" in data:
                    self.backup_file(i_filename, reason="Remove null bytes from file")
                    with open(i_filename, "wb") as fp:
                        new_data_bytes = data.replace(b"\x00", b"")
                        new_data_bytes = new_data_bytes.replace(b"\xff", b"")
                        fp.write(new_data_bytes)
                    action_log = MaintenanceActionLog(
                        item=investigation_file_path,
                        action=MaintenanceAction.UPDATE_CONTENT,
                        parameters={},
                        message=f"{self.study_id}: NULL values are removed from {self.investigation_file_name}.",
                    )
                    self.actions.append(action_log)

                    return self.load_investigation_file(investigation_file_path, try_to_fix=False)
                else:
                    with open(i_filename, "r") as fp:
                        lines = fp.readlines()
                    updated = False
                    updates = {}
                    for i in range(len(lines)):
                        line = lines[i]
                        if line.startswith("Investigation Identifier\t") or line.startswith("Study Identifier\t"):
                            split = line.strip().split("\t")

                            if len(split) > 1 and split[1].strip("'").strip('"') != self.study_id:
                                updates[split[0]] = f"'{split[1]}'->{self.study_id}"
                                split[1] = self.study_id
                                lines[i] = "\t".join(split) + "\n"
                                updated = True
                    if updated:
                        self.backup_file(
                            i_filename, reason=f"Identifiers in {self.investigation_file_name} will be fixed."
                        )
                        with open(i_filename, "w") as fp:
                            fp.writelines(lines)
                        updated_values = ", ".join([f"{x}: {updates[x]}" for x in updates])
                        action_log = MaintenanceActionLog(
                            item=investigation_file_path,
                            action=MaintenanceAction.UPDATE_CONTENT,
                            parameters={},
                            message=f"{self.study_id}: Identifier are updated: {updated_values} in {self.investigation_file_name}.",
                        )
                        self.actions.append(action_log)
                        return self.load_investigation_file(investigation_file_path, try_to_fix=False)
                    else:
                        logger.exception("Failed to find Investigation file %s ", investigation_file_path)
                        logger.error(str(e))
                        raise e
            else:
                logger.exception("Failed to find Investigation file %s ", investigation_file_path)
                logger.error(str(e))
                raise e

    def save_investigation_file(self, investigation, skip_load_tables=True):
        try:
            isatab.dump(
                investigation,
                self.study_metadata_files_path,
                i_file_name=self.investigation_file_name,
                skip_dump_tables=skip_load_tables,
            )
        except Exception as ex:
            investigation_file_path = os.path.join(
                self.study_metadata_files_path, self.investigation_file_name
            )
            action_log = MaintenanceActionLog(
                item=investigation_file_path,
                action=MaintenanceAction.ERROR_MESSAGE,
                parameters={},
                message=f"{self.study_id}: ERROR: investigation file could not saved {self.sanitise(ex)}.",
                successful=False,
            )
            self.actions.append(action_log)

            raise ex
