from app.services.storage_service.remote_worker.remote_file_manager import RemoteFileManager
from app.services.storage_service.unmounted.unmounted_storage import UnmountedStorage


class RemoteFtpStorage(UnmountedStorage):
    def __init__(self, name, remote_folder):
        manager_name = name + "_remote_file_manager"

        remote_file_manager: RemoteFileManager = RemoteFileManager(manager_name, mounted_root_folder=remote_folder)
        self.remote_file_manager: RemoteFileManager = remote_file_manager

        super(RemoteFtpStorage, self).__init__(name=name, remote_file_manager=self.remote_file_manager)
