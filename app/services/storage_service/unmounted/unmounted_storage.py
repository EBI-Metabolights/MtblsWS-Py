from typing import List

from app.services.storage_service.file_manager import FileManager
from app.services.storage_service.mounted.local_file_manager import MountedVolumeFileManager
from app.services.storage_service.storage import Storage
from app.services.storage_service.sync_status import JobState
from app.services.storage_service.unmounted.data_mover_client import DataMoverAvailableStorage
from app.services.storage_service.unmounted.unmounted_file_manager import UnmountedVolumeFileManager
from app.utils import MetabolightsException


class UnmountedStorage(Storage):

    def __init__(self, name, app):

        manager_name = name + '_mounted_volume_file_manager'
        self.remote_file_manager: UnmountedVolumeFileManager = UnmountedVolumeFileManager(manager_name, app)

        super(UnmountedStorage, self).__init__(name=name, remote_file_manager=self.remote_file_manager)

    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        study_id = UnmountedVolumeFileManager.get_study_id(target_folder)
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("sync_from_storage", study_id)
        status = remote_job_manager.sync_from_studies_folder(target_folder, ignore_list, **kwargs)
        return status

    def sync_from_storage(self, source: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        study_id = UnmountedVolumeFileManager.get_study_id(source)
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("sync_from_storage", study_id)

        status = remote_job_manager.sync_from_ftp_folder(source, ignore_list, **kwargs)
        return status

    def get_folder_sync_status(self, study_id: str) -> JobState:
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("sync_from_storage", study_id)

        status = remote_job_manager.get_folder_sync_status(study_id)
        return status
