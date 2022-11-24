from typing import List

from app.services.storage_service.models import SyncCalculationTaskResult, SyncTaskResult
from app.services.storage_service.storage import Storage
from app.services.storage_service.unmounted.data_mover_client import DataMoverAvailableStorage
from app.services.storage_service.unmounted.unmounted_file_manager import UnmountedVolumeFileManager
from app.utils import MetabolightsException


class UnmountedStorage(Storage):

    def __init__(self, name, app):
        self.app = app
        manager_name = name + '_mounted_volume_file_manager'
        self.remote_file_manager: UnmountedVolumeFileManager = UnmountedVolumeFileManager(manager_name, app)

        super(UnmountedStorage, self).__init__(name=name, remote_file_manager=self.remote_file_manager)

    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        study_id = UnmountedVolumeFileManager.get_study_id(target_folder)
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("sync_from_storage", study_id, self.app)
        status = remote_job_manager.sync_from_studies_folder(target_folder, ignore_list, **kwargs)
        return status

    def sync_from_storage(self, source: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        study_id = UnmountedVolumeFileManager.get_study_id(source)
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("sync_from_storage", study_id, self.app)
        if not ignore_list:
            ignore_list = []
        success = remote_job_manager.sync_from_ftp_folder(source, ignore_list, **kwargs)
        if not success:
            raise MetabolightsException(http_code=500, message=f"Sync job for {source} was failed.")
        return success, f"Sync job for {source} is started."

    def calculate_sync_status(self, study_id: str, obfuscation_code: str,
                              target_local_path: str, force: bool = False, ignore_list: List = None) -> SyncCalculationTaskResult:
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("sync_from_storage", study_id, self.app)
        ftp_folder_name = f"{study_id.lower()}-{obfuscation_code}"
        if not ignore_list:
            ignore_list = []
        result = remote_job_manager.check_calculate_sync_status(ftp_folder_name, force, ignore_list)
        return result

    def check_folder_sync_status(self, study_id: str, obfuscation_code: str, target_local_path: str) -> SyncTaskResult:
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("sync_from_storage", study_id, self.app)

        result = remote_job_manager.check_folder_sync_status()
        return result
