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


class LocalStorage(Storage):

    def __init__(self, name, remote_folder, local_folder):
        manager_name = name + '_local_file_manager'
        local_file_manager: FileManager = MountedVolumeFileManager(manager_name, mounted_root_folder=local_folder)

        manager_name = name + '_mounted_volume_file_manager'
        mounted_volume_file_manager: FileManager = FtpClientFileManager(manager_name)
        remote_file_manager: FileManager = mounted_volume_file_manager

        super(LocalStorage, self).__init__(name=name, local_file_manager=local_file_manager,
                                                          remote_file_manager=remote_file_manager)

    def download_file(self, source_file: str, target_local_folder: str, new_name: str = None) -> str:
        if not source_file or not self.remote.is_file(source_file):
            message = 'source file is None or not a file'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        source_path = self.remote.get_uri(source_file)
        target_path_backup_id = str(random.randint(10000009, 99999999))
        target_file_backup_path = None
        target_file_path = None
        try:

            if not os.path.exists(target_local_folder):
                os.makedirs(target_local_folder, exist_ok=True)

            if new_name:
                target_file_path = os.path.join(target_local_folder, new_name)
            else:
                target_file_path = os.path.join(target_local_folder, source_file)

            target_dir_name = os.path.dirname(target_file_path)
            os.makedirs(target_dir_name, exist_ok=True)

            if os.path.exists(target_file_path):
                target_file_backup_path = source_path + "_" + target_path_backup_id
                os.rename(target_file_path, target_file_backup_path)

            shutil.copyfile(source_path, target_file_path)
            return target_file_path
        except Exception:
            if target_file_backup_path:
                if os.path.isfile(target_file_backup_path):
                    os.remove(target_file_path)
                    os.rename(target_file_backup_path, target_file_path)
                else:
                    shutil.rmtree(target_file_path, ignore_errors=True)
                    os.rename(target_file_backup_path, target_file_path)
            raise
        finally:
            if target_file_backup_path and os.path.exists(target_file_backup_path):
                if os.path.isfile(target_file_backup_path):
                    os.remove(target_file_backup_path)
                else:
                    shutil.rmtree(target_file_backup_path, ignore_errors=True)

    def download_folder(self, source_folder: str, target_local_folder: str) -> str:
        if not source_folder or not self.remote.is_folder(source_folder):
            message = 'source is None or not a folder'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        target_path_backup_id = str(random.randint(100000009, 999999999))
        target_file_backup_path = None
        target_file_path = target_local_folder
        try:
            source_path = self.remote.get_uri(source_folder)

            target_parent_dir = os.path.dirname(target_file_path)
            os.makedirs(target_parent_dir, exist_ok=True)
            if os.path.exists(target_file_path):
                target_file_backup_path = source_path + "_" + target_path_backup_id
                os.rename(target_file_path, target_file_backup_path)
            copy_tree(source_path, target_file_path)
            return target_file_path

        except Exception:
            if target_file_backup_path:
                if os.path.isfile(target_file_backup_path):
                    os.remove(target_file_path)
                    os.rename(target_file_backup_path, target_file_path)
                else:
                    shutil.rmtree(target_file_path, ignore_errors=True)
                    os.rename(target_file_backup_path, target_file_path)
            raise
        finally:
            if target_file_backup_path and os.path.exists(target_file_backup_path):
                if os.path.isfile(target_file_backup_path):
                    os.remove(target_file_backup_path)
                else:
                    shutil.rmtree(target_file_backup_path, ignore_errors=True)

    def upload_file(self, source_file: str, target_remote_folder: str, target_file_name: str = None) -> str:
        if not source_file or not self.local.is_folder(source_file):
            message = 'source is None or not a file'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        source_path = self.local.get_uri(source_file)

        target_path = self.remote.get_uri(target_remote_folder)
        if target_file_name:
            target_file_path = os.path.join(target_path, target_file_name)
        else:
            source_file_name = os.path.basename(source_path)
            target_file_path = os.path.join(target_path, source_file_name)
        copy_tree(source_path, target_file_path)
        return target_file_path

    def upload_files(self, source_files: List[str], target_parent_remote_folder: str) -> List[str]:
        if not source_files:
            message = 'source file list is list empty'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        file_list = []
        for source_file in source_files:
            file = self.upload_file(source_file, target_parent_remote_folder)
            relative_path = file.replace(self.local.get_uri(), '', 1)
            file_list.append(relative_path)

        return file_list

    def upload_folder(self, source_folder: str, target_remote_folder: str = None) -> str:
        if not source_folder or not self.local.is_folder(source_folder):
            message = 'source is None or not a folder'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        source_path = self.local.get_uri(source_folder)
        target_path = self.remote.get_uri(target_remote_folder)
        copy_tree(source_path, target_path)
        return target_path

    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        target_path = self.remote.get_uri(target_folder)
        sync(source_local_folder, target_path, 'sync', **kwargs)

    def sync_from_storage(self, source: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        source_path = self.remote.get_uri(source)
        if os.path.isfile(source_path):
            dir_name = os.path.dirname(target_local_path)
            base_name = os.path.basename(target_local_path)
            self.download_file(source, dir_name, base_name)
        elif os.path.isdir(source_path):
            sync(source_path, target_local_path, 'sync', **kwargs)