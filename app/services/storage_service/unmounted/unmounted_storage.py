from typing import List

from app.services.storage_service.models import SyncCalculationTaskResult, SyncTaskResult
from app.services.storage_service.storage import Storage
from app.services.storage_service.unmounted.data_mover_client import DataMoverAvailableStorage
from app.services.storage_service.unmounted.unmounted_file_manager import UnmountedVolumeFileManager
from app.utils import MetabolightsException
from app.services.storage_service.models import SyncTaskStatus


class UnmountedStorage(Storage):

    def __init__(self, name, app, remote_file_manager=None):
        self.app = app
        manager_name = name + '_mounted_volume_file_manager'
        self.remote_file_manager = remote_file_manager
        if not remote_file_manager:
            self.remote_file_manager: UnmountedVolumeFileManager = UnmountedVolumeFileManager(manager_name, app)

        super(UnmountedStorage, self).__init__(name=name, remote_file_manager=self.remote_file_manager)

    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        study_id = UnmountedVolumeFileManager.get_study_id(target_folder)
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("unmounted_storage", study_id, self.app)
        meta_sync_status,files_sync_status,chebi_sync_status = remote_job_manager.sync_from_studies_folder(target_folder, ignore_list, **kwargs)
        return meta_sync_status,files_sync_status,chebi_sync_status
    
    def sync_to_public_ftp(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        study_id = target_folder
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("unmounted_storage", study_id, self.app)
        meta_public_sync_status,files_public_sync_status = remote_job_manager.sync_public_study_to_ftp(source_study_folder=source_local_folder, target_ftp_folder=target_folder, ignore_list=ignore_list, **kwargs)
        return meta_public_sync_status,files_public_sync_status
    

    def sync_from_storage(self, source: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        study_id = UnmountedVolumeFileManager.get_study_id(source)
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("unmounted_storage", study_id, self.app)
        if not ignore_list:
            ignore_list = []
        meta_sync_status,rdfiles_sync_status = remote_job_manager.sync_from_ftp_folder(source, ignore_list, **kwargs)
        return meta_sync_status,rdfiles_sync_status


    def calculate_sync_status(self, study_id: str, obfuscation_code: str,
                              target_local_path: str, force: bool = False, ignore_list: List = None) -> SyncCalculationTaskResult:
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("unmounted_storage", study_id, self.app)
        ftp_folder_name = f"{study_id.lower()}-{obfuscation_code}"
        if not ignore_list:
            ignore_list = []
        meta_calc_result,rdfiles_calc_result = remote_job_manager.sync_anaysis_job_results(source_ftp_folder=ftp_folder_name, force=force, ignore_list=ignore_list)
        return meta_calc_result,rdfiles_calc_result

    def check_folder_sync_status(self, study_id: str, obfuscation_code: str, target_local_path: str) -> SyncTaskResult:
        if not study_id:
            raise MetabolightsException("Invalid study id")
        remote_job_manager = DataMoverAvailableStorage("unmounted_storage", study_id, self.app)

        sync_metafiles_result,sync_rdfiles_result = remote_job_manager.get_folder_sync_results()
        return sync_metafiles_result,sync_rdfiles_result
