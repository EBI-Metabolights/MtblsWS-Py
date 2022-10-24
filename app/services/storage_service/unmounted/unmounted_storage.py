from typing import List

from app.services.storage_service.file_manager import FileManager
from app.services.storage_service.mounted.local_file_manager import MountedVolumeFileManager
from app.services.storage_service.storage import Storage
from app.services.storage_service.unmounted.unmounted_file_manager import UnmountedVolumeFileManager


class UnmountedStorage(Storage):

    def __init__(self, name, app):

        manager_name = name + '_mounted_volume_file_manager'
        self.remote_file_manager: UnmountedVolumeFileManager = UnmountedVolumeFileManager(manager_name, app)

        super(UnmountedStorage, self).__init__(name=name, remote_file_manager=self.remote_file_manager)

    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        self.remote_file_manager.remote_job_manager.sync_from_studies_folder(source_local_folder, target_folder,
                                                                             ignore_list, **kwargs)

    def sync_from_storage(self, source: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        self.remote_file_manager.remote_job_manager.sync_from_ftp_folder(source, target_local_path,
                                                                         ignore_list, **kwargs)
