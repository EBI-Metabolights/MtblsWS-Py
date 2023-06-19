from datetime import datetime
import os
from typing import List
from celery.result import AsyncResult
from app.config import get_settings
from app.services.storage_service.models import (
    SyncCalculationStatus,
    SyncCalculationTaskResult,
    SyncTaskResult,
    SyncTaskStatus,
)
from app.services.storage_service.remote_worker.remote_file_manager import RemoteFileManager
from app.services.storage_service.storage import Storage
from app.tasks.bash_client import BashExecutionResult
from app.tasks.hpc_rsync_worker import HpcRsyncWorker
from app.tasks.hpc_study_rsync_client import StudyFolder, StudyFolderLocation, StudyFolderType, StudyRsyncClient
from app.tasks.hpc_worker_bash_runner import HpcWorkerBashRunner, TaskDescription, BashExecutionTaskStatus
from app.tasks.worker import celery


class RemoteFtpStorage(Storage):
    def __init__(self, name, remote_folder):
        manager_name = name + "_remote_file_manager"

        remote_file_manager: RemoteFileManager = RemoteFileManager(manager_name, mounted_root_folder=remote_folder)
        self.remote_file_manager: RemoteFileManager = remote_file_manager

        super(RemoteFtpStorage, self).__init__(name=name, remote_file_manager=self.remote_file_manager)

    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        study_id = os.path.basename(source_local_folder)
        
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=None)
        source = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        target = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
        status = client.rsync(source, target, status_check_only=False)
        return status
        # study_id = UnmountedVolumeFileManager.get_study_id(target_folder)
        # if not study_id:
        #     raise MetabolightsException("Invalid study id")
        # remote_job_manager = DataMoverAvailableStorage("unmounted_storage", study_id, self.app)
        # meta_sync_status,files_sync_status,chebi_sync_status = remote_job_manager.sync_from_studies_folder(target_folder, ignore_list, **kwargs)
        # return meta_sync_status,files_sync_status,chebi_sync_status
        pass

    def sync_to_public_ftp(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        study_id = os.path.basename(source_local_folder)
        
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=None)
        source = StudyFolder(location=StudyFolderLocation.READONLY_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        target = StudyFolder(location=StudyFolderLocation.PUBLIC_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
        status = client.rsync(source, target, status_check_only=False)
        return status
        # study_id = target_folder
        # if not study_id:
        #     raise MetabolightsException("Invalid study id")
        # remote_job_manager = DataMoverAvailableStorage("unmounted_storage", study_id, self.app)
        # meta_public_sync_status,files_public_sync_status = remote_job_manager.sync_public_study_to_ftp(source_study_folder=source_local_folder, target_ftp_folder=target_folder, ignore_list=ignore_list, **kwargs)
        # return meta_public_sync_status,files_public_sync_status
        pass

    def get_private_ftp_root_path(self):
        settings = get_settings()
        return settings.study.mounted_paths.private_ftp_root_path

    def get_study_id_from_private_ftp_path(self, value: str):
        if not value:
            return None, None
        splitted_value = value.split("-")

        if len(splitted_value) > 1 and splitted_value[0] and splitted_value[1]:
            return splitted_value[0].upper(), splitted_value[1]
        return None, None

    def sync_from_storage(self, source: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        return self.sync_from_private_ftp_folder(source, target_local_path)

    def sync_from_private_ftp_folder(self, source, target_local_path):
        study_id, obfuscation_code = self.get_study_id_from_private_ftp_path(source)
        
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=obfuscation_code)
        source = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
        target = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        status = client.rsync(source, target, status_check_only=False)
        return status
        # task_name = f"sync_metadata_from_private_ftp_to_private_study_folder"
        # cluster_private_ftp_root_path = self.get_private_ftp_root_path()
        # ftp_folder_path = os.path.join(cluster_private_ftp_root_path, source)

        # include_list = ["[asi]_*.txt", "m_*.tsv"]
        # exclude_list = ["*"]
        # status = HpcRsyncWorker.start_rsync(
        #     task_name,
        #     study_id,
        #     ftp_folder_path,
        #     target_local_path,
        #     include_list=include_list,
        #     exclude_list=exclude_list,
        # )

        # return status

    def calculate_sync_status(
        self,
        study_id: str,
        obfuscation_code: str,
        target_local_path: str,
        force: bool = False,
        ignore_list: List = None,
    ) -> SyncCalculationTaskResult:
        status_check_only = False if force else True
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=obfuscation_code)
        source = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
        target = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        status = client.rsync_dry_run(source, target, status_check_only=status_check_only)
        return status
        # task_name = f"sync_metadata_from_private_ftp_to_private_study_folder"
        # cluster_private_ftp_root_path = self.get_private_ftp_root_path()
        # ftp_folder_path = os.path.join(cluster_private_ftp_root_path, f"{study_id.lower()}-{obfuscation_code}")
        # include_list = ["[asi]_*.txt", "m_*.tsv"]
        # exclude_list = ["*"]
        # status = HpcRsyncWorker.start_rsync_dry_run(
        #     task_name,
        #     study_id,
        #     ftp_folder_path,
        #     target_local_path,
        #     include_list=include_list,
        #     exclude_list=exclude_list,
        # )
        # return status

    def check_folder_sync_status(self, study_id: str, obfuscation_code: str, target_local_path: str) -> SyncTaskResult:
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=obfuscation_code)
        source = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
        target = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        status = client.rsync(source, target, status_check_only=True)
        return status
        # task_name = f"sync_metadata_from_private_ftp_to_private_study_folder"

        # return HpcRsyncWorker.get_rsync_status(task_name, study_id)
