import datetime
from enum import Enum
import os
from typing import List
from pydantic import BaseModel
from app.config import get_settings
from app.services.storage_service.models import SyncCalculationTaskResult, SyncTaskResult
from app.tasks.hpc_rsync_worker import HpcRsyncWorker

from app.utils import MetabolightsException


class StudyFolderLocation(str, Enum):
    PRIVATE_FTP_STORAGE = "PRIVATE_FTP_STORAGE"
    PUBLIC_FTP_STORAGE = "PUBLIC_FTP_STORAGE"
    READONLY_STUDY_STORAGE = "READONLY_STUDY_STORAGE"
    RW_STUDY_STORAGE = "RW_STUDY_STORAGE"


class StudyFolderType(str, Enum):
    METADATA = "METADATA"
    DATA = "DATA"
    AUDIT = "AUDIT"
    INTERNAL = "INTERNAL"
    PUBLIC_METADATA_VERSIONS = "PUBLIC_METADATA_VERSIONS"
    INTEGRITY_CHECK = "INTEGRITY_CHECK"


class StudyFolder(BaseModel):
    location: StudyFolderLocation
    folder_type: StudyFolderType


VALID_FOLDERS = {
    StudyFolderLocation.RW_STUDY_STORAGE: {
        StudyFolderType.METADATA,
        StudyFolderType.AUDIT,
        StudyFolderType.INTERNAL,
        StudyFolderType.INTEGRITY_CHECK,
    },
    StudyFolderLocation.READONLY_STUDY_STORAGE: {
        StudyFolderType.METADATA,
        StudyFolderType.AUDIT,
        StudyFolderType.DATA,
        StudyFolderType.PUBLIC_METADATA_VERSIONS,
        StudyFolderType.INTEGRITY_CHECK,
    },
    StudyFolderLocation.PUBLIC_FTP_STORAGE: {
        StudyFolderType.METADATA,
        StudyFolderType.DATA,
        StudyFolderType.PUBLIC_METADATA_VERSIONS,
        StudyFolderType.INTEGRITY_CHECK,
    },
    StudyFolderLocation.PRIVATE_FTP_STORAGE: {StudyFolderType.METADATA, StudyFolderType.DATA, StudyFolderType.INTERNAL},
}

ALLOWED_STAGING_AREA_DIRECTIONS = {
    StudyFolderLocation.PRIVATE_FTP_STORAGE: {
        StudyFolderLocation.READONLY_STUDY_STORAGE: {StudyFolderType.DATA},
        StudyFolderLocation.RW_STUDY_STORAGE: {StudyFolderType.METADATA, StudyFolderType.INTERNAL},
    },
    StudyFolderLocation.RW_STUDY_STORAGE: {
        StudyFolderLocation.READONLY_STUDY_STORAGE: {
            StudyFolderType.METADATA,
            StudyFolderType.AUDIT,
            StudyFolderType.INTEGRITY_CHECK,
        },
        StudyFolderLocation.PRIVATE_FTP_STORAGE: {StudyFolderType.METADATA, StudyFolderType.INTERNAL},
    },
    StudyFolderLocation.PUBLIC_FTP_STORAGE: {
        StudyFolderLocation.READONLY_STUDY_STORAGE: {
            StudyFolderType.METADATA,
            StudyFolderType.DATA,
            StudyFolderType.INTEGRITY_CHECK,
            StudyFolderType.PUBLIC_METADATA_VERSIONS,
        },
    },
    StudyFolderLocation.READONLY_STUDY_STORAGE: {
        StudyFolderLocation.PUBLIC_FTP_STORAGE: {
            StudyFolderType.METADATA,
            StudyFolderType.DATA,
            StudyFolderType.INTEGRITY_CHECK,
            StudyFolderType.PUBLIC_METADATA_VERSIONS,
        },
        StudyFolderLocation.PRIVATE_FTP_STORAGE: {StudyFolderType.DATA},
        StudyFolderLocation.RW_STUDY_STORAGE: {
            StudyFolderType.METADATA,
            StudyFolderType.INTEGRITY_CHECK,
            StudyFolderType.AUDIT,
        },
    },
}


class StudyRsyncClient:
    def __init__(self, study_id: str, obfuscation_code: str = None) -> None:
        self.study_id = study_id
        self.obfuscation_code = obfuscation_code
        self.settings = get_settings()
        mounted_paths = self.settings.hpc_cluster.datamover.mounted_paths
        self.log_path = os.path.join(
            mounted_paths.cluster_study_internal_files_root_path,
            self.study_id,
            self.settings.study.internal_logs_folder_name,
        )

    def rsync(
        self,
        source: StudyFolder,
        target: StudyFolder,
        status_check_only: bool = True,
        include_list: List[str] = None,
        exclude_list: List[str] = None,
    ) -> SyncTaskResult:
        self.validate_sync_direction(source, target)
        task_name = self.get_task_name(source, target, dry_run_mode=False)
        date_timestamp = datetime.datetime.now().strftime("%y-%m-%d-%m_%H:%M:%S")
        stdout_log_filename = f"{task_name}_{date_timestamp}.stdout"
        stderr_log_filename = f"{task_name}_{date_timestamp}.stderr"
        stdout_log_file_path = os.path.join(self.log_path, stdout_log_filename)
        stderr_log_file_path = os.path.join(self.log_path, stderr_log_filename)

        if status_check_only:
            result: SyncTaskResult = HpcRsyncWorker.get_rsync_status(task_name, self.study_id)
        else:
            if not include_list and not exclude_list:
                include_list, exclude_list = self.get_include_and_exlude_lists(source, target)
            rsync_arguments = "-auv"
            source_path = self.get_folder_path(source)
            target_path = self.get_folder_path(target)
            result: SyncTaskResult = HpcRsyncWorker.start_rsync(
                task_name,
                self.study_id,
                source_path,
                target_path,
                include_list=include_list,
                exclude_list=exclude_list,
                rsync_arguments=rsync_arguments,
                stdout_log_file_path=stdout_log_file_path,
                stderr_log_file_path=stderr_log_file_path,
            )
        return result

    def rsync_dry_run(
        self,
        source: StudyFolder,
        target: StudyFolder,
        status_check_only: bool = True,
        include_list: List[str] = None,
        exclude_list: List[str] = None,
    ) -> SyncCalculationTaskResult:
        self.validate_sync_direction(source, target)
        task_name = self.get_task_name(source, target, dry_run_mode=True)


        if status_check_only:
            result: SyncCalculationTaskResult = HpcRsyncWorker.get_rsync_dry_run_status(task_name, self.study_id)
        else:
            date_timestamp = datetime.datetime.now().strftime("%y-%m-%d-%m_%H:%M:%S")
            stdout_log_filename = f"{task_name}_{date_timestamp}.stdout"
            stderr_log_filename = f"{task_name}_{date_timestamp}.stderr"
            stdout_log_file_path = os.path.join(self.log_path, stdout_log_filename)
            stderr_log_file_path = os.path.join(self.log_path, stderr_log_filename)
            if not include_list and not exclude_list:
                include_list, exclude_list = self.get_include_and_exlude_lists(source, target)
            rsync_arguments = "-aunv"
            source_path = self.get_folder_path(source)
            target_path = self.get_folder_path(target)
            result: SyncCalculationTaskResult = HpcRsyncWorker.start_rsync_dry_run(
                task_name,
                self.study_id,
                source_path,
                target_path,
                include_list=include_list,
                exclude_list=exclude_list,
                rsync_arguments=rsync_arguments,
                stdout_log_file_path=stdout_log_file_path,
                stderr_log_file_path=stderr_log_file_path,
            )
        return result

    def get_folder_path(self, folder: StudyFolder):
        mounted_paths = self.settings.hpc_cluster.datamover.mounted_paths
        path = None
        if folder.location == StudyFolderLocation.RW_STUDY_STORAGE:
            if folder.folder_type == StudyFolderType.AUDIT:
                path = mounted_paths.cluster_study_audit_files_root_path
            elif folder.folder_type == StudyFolderType.INTERNAL:
                path = mounted_paths.cluster_study_internal_files_root_path
            elif folder.folder_type == StudyFolderType.METADATA:
                path = mounted_paths.cluster_study_metadata_files_root_path
            elif folder.folder_type == StudyFolderType.INTEGRITY_CHECK:
                path = mounted_paths.cluster_study_internal_files_root_path
            if path:
                return os.path.join(path, self.study_id)
        elif folder.location == StudyFolderLocation.READONLY_STUDY_STORAGE:
            if folder.folder_type == StudyFolderType.AUDIT:
                path = mounted_paths.cluster_study_readonly_audit_files_root_path
            elif folder.folder_type == StudyFolderType.DATA:
                path = mounted_paths.cluster_study_readonly_files_root_path
            elif folder.folder_type == StudyFolderType.METADATA:
                path = mounted_paths.cluster_study_readonly_metadata_files_root_path
            elif folder.folder_type == StudyFolderType.PUBLIC_METADATA_VERSIONS:
                path = mounted_paths.cluster_study_readonly_public_metadata_versions_root_path
            elif folder.folder_type == StudyFolderType.INTEGRITY_CHECK:
                path = mounted_paths.cluster_study_readonly_integrity_check_files_root_path
            if path:
                return os.path.join(path, self.study_id)
        elif folder.location == StudyFolderLocation.PUBLIC_FTP_STORAGE:
            if folder.folder_type == StudyFolderType.DATA:
                path = mounted_paths.cluster_public_ftp_root_path
            elif folder.folder_type == StudyFolderType.METADATA:
                path = mounted_paths.cluster_public_ftp_root_path
            elif folder.folder_type == StudyFolderType.PUBLIC_METADATA_VERSIONS:
                path = mounted_paths.cluster_public_ftp_root_path
            elif folder.folder_type == StudyFolderType.INTEGRITY_CHECK:
                path = mounted_paths.cluster_public_ftp_root_path
            if path:
                return os.path.join(path, self.study_id)
        elif folder.location == StudyFolderLocation.PRIVATE_FTP_STORAGE:
            if folder.folder_type == StudyFolderType.DATA:
                path = mounted_paths.cluster_private_ftp_root_path
            elif folder.folder_type == StudyFolderType.METADATA:
                path = mounted_paths.cluster_private_ftp_root_path
            elif folder.folder_type == StudyFolderType.INTERNAL:
                path = mounted_paths.cluster_private_ftp_root_path
            if path:
                return os.path.join(path, f"{self.study_id.lower()}-{self.obfuscation_code}")
        raise MetabolightsException(message=f"Folder path is not valid for {folder.dict()}")

    def get_include_and_exlude_lists(self, source: StudyFolder, target: StudyFolder):
        include_list = []
        exclude_list = []
        if source.folder_type == StudyFolderType.METADATA:
            include_list = ["[asi]_*.txt", "m_*.tsv"]
            exclude_list = ["*"]
        elif source.folder_type == StudyFolderType.DATA:
            exclude_list = ["[asi]_*.txt", "m_*.tsv"]
        elif source.folder_type == StudyFolderType.AUDIT:
            include_list = [self.settings.study.audit_folder_name]
            exclude_list = ["*"]
        elif source.folder_type == StudyFolderType.INTERNAL:
            if target.location == StudyFolderLocation.PRIVATE_FTP_STORAGE:
                include_list = [self.settings.study.chebi_annotation_sub_folder]
            elif target.location == StudyFolderLocation.READONLY_STUDY_STORAGE:
                include_list = ["metadata_summary.tsv", "data_files_summary.txt"]

            exclude_list = ["*"]

        return include_list, exclude_list

    def get_task_name(self, source: StudyFolder, target: StudyFolder, dry_run_mode: bool = False):
        mode = 'rsync_dry_run' if dry_run_mode else 'rsync'
        src_location = source.location.lower()
        tgt_location = target.location.lower()
        src_type = source.folder_type.lower()
        tgt_type = target.folder_type.lower()
        return f"{mode}:from:{src_location}_{src_type}:to:{tgt_location}_{tgt_type}"

    def validate_sync_direction(self, source: StudyFolder, target: StudyFolder):
        if not self.is_valid_study_folder(source):
            raise MetabolightsException(message="Source study folder is not valid.")

        if not self.is_valid_study_folder(target):
            raise MetabolightsException(message="Target study folder is not valid.")

        if source.location == target.location:
            raise MetabolightsException(message="Source and target should be different.")

        if target.location not in ALLOWED_STAGING_AREA_DIRECTIONS[source.location]:
            raise MetabolightsException(
                message="From selected source staging area to target staging area is not allowed."
            )
        if target.folder_type not in ALLOWED_STAGING_AREA_DIRECTIONS[source.location][target.location]:
            raise MetabolightsException(
                message="Selected study folder syncronisation is not allowed from source staging area to target staging area ."
            )

    def is_valid_study_folder(self, study_folder: StudyFolder):
        if not study_folder:
            return False
        if study_folder.location in VALID_FOLDERS and study_folder.folder_type in VALID_FOLDERS[study_folder.location]:
            return True
        return False
