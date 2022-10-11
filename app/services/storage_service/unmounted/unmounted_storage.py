import os.path
import random
import shutil
from distutils.dir_util import copy_tree
from typing import List

from dirsync import sync

from app.services.storage_service.exceptions import StorageServiceException
from app.services.storage_service.file_manager import FileManager
from app.services.storage_service.mounted.local_file_manager import MountedVolumeFileManager
from app.services.storage_service.storage import Storage
from app.services.storage_service.unmounted.ftp_client_file_manager import FtpClientFileManager


class UnmountedStorage(Storage):

    def __init__(self, name, relative_studies_root_path, local_folder):
        manager_name = name + '_local_file_manager'
        local_file_manager: FileManager = MountedVolumeFileManager(manager_name, mounted_root_folder=local_folder)

        manager_name = name + '_mounted_volume_file_manager'
        remote_file_manager: FileManager = FtpClientFileManager(manager_name, relative_studies_root_path=relative_studies_root_path)

        super(UnmountedStorage, self).__init__(name=name, local_file_manager=local_file_manager,
                                               remote_file_manager=remote_file_manager)

    def download_file(self, source_file: str, target_local_folder: str, new_name: str = None) -> str:
        pass

    def download_folder(self, source_folder: str, target_local_folder: str) -> str:
        pass

    def upload_file(self, source_file: str, target_remote_folder: str, target_file_name: str = None) -> str:
        pass

    def upload_files(self, source_files: List[str], target_parent_remote_folder: str) -> List[str]:
        pass

    def upload_folder(self, source_folder: str, target_remote_folder: str = None) -> str:
        pass

    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        pass

    def sync_from_storage(self, source: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        pass