import os
import os.path
import random
import shutil
from datetime import datetime, timezone
from distutils.dir_util import copy_tree
from typing import List

from dirsync import sync

from app.services.storage_service.exceptions import StorageServiceException
from app.services.storage_service.models import SyncTaskResult, SyncCalculationTaskResult, SyncCalculationStatus, \
    SyncTaskStatus
from app.services.storage_service.mounted.local_file_manager import MountedVolumeFileManager
from app.services.storage_service.storage import Storage
from app.utils import MetabolightsException


class LocalStorage(Storage):

    def __init__(self, name, remote_folder):
        manager_name = name + '_local_file_manager'
        local_path = f"/tmp/{name}"
        os.makedirs(local_path, exist_ok=True)
        local_file_manager: MountedVolumeFileManager = MountedVolumeFileManager(manager_name, local_path)
        self.local_file_manager: MountedVolumeFileManager = local_file_manager

        manager_name = name + '_mounted_volume_file_manager'
        mounted_volume_file_manager: MountedVolumeFileManager = MountedVolumeFileManager(manager_name, remote_folder)
        self.remote_file_manager: MountedVolumeFileManager = mounted_volume_file_manager

        super(LocalStorage, self).__init__(name=name, remote_file_manager=self.remote_file_manager)

    def download_file(self, source_file: str, target_local_folder: str, new_name: str = None) -> str:
        if not source_file or not self.remote_file_manager.is_file(source_file):
            message = 'source file is None or not a file'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        source_path = self.remote_file_manager.get_uri(source_file)
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
        if not source_folder or not self.remote_file_manager.is_folder(source_folder):
            message = 'source is None or not a folder'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        target_path_backup_id = str(random.randint(100000009, 999999999))
        target_file_backup_path = None
        target_file_path = target_local_folder
        try:
            source_path = self.remote_file_manager.get_uri(source_folder)

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
        if not source_file or not self.local_file_manager.is_folder(source_file):
            message = 'source is None or not a file'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        source_path = self.local_file_manager.get_uri(source_file)

        target_path = self.remote_file_manager.get_uri(target_remote_folder)
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
            relative_path = file.replace(self.local_file_manager.get_uri(), '', 1)
            file_list.append(relative_path)

        return file_list

    def upload_folder(self, source_folder: str, target_remote_folder: str = None) -> str:
        if not source_folder or not self.local_file_manager.is_folder(source_folder):
            message = 'source is None or not a folder'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_OPERATION, message)

        source_path = self.local_file_manager.get_uri(source_folder)
        target_path = self.remote_file_manager.get_uri(target_remote_folder)
        copy_tree(source_path, target_path)
        return target_path

    def sync_from_local(self, source_local_folder: str, target_folder: str, ignore_list: List[str] = None, **kwargs):
        target_path = self.remote_file_manager.get_uri(target_folder)
        sync(source_local_folder, target_path, 'sync', **kwargs)

    def sync_from_storage(self, source: str, target_local_path: str, ignore_list: List[str] = None, **kwargs):
        source_path = self.remote_file_manager.get_uri(source)
        if os.path.isfile(source_path):
            dir_name = os.path.dirname(target_local_path)
            base_name = os.path.basename(target_local_path)
            self.download_file(source, dir_name, base_name)
        elif os.path.isdir(source_path):
            sync(source_path, target_local_path, 'sync', **kwargs)

    def calculate_sync_status(self, study_id: str, obfuscation_code: str, target_local_path: str, force: bool = True) \
            -> SyncCalculationTaskResult:
        self.remote_file_manager.get_uri(study_id)
        ftp_folder_name = f"{study_id.lower()}-{obfuscation_code}"

        source = self.remote_file_manager.get_uri(ftp_folder_name)
        updated_files = self.calculate_folder_sync_status(source, target_local_path)
        last_updated_time = 0
        new_file_count = 0
        updated_files_count = 0
        for file, status, modified, modified_timestamp in updated_files:
            if modified_timestamp > last_updated_time:
                last_updated_time = modified_timestamp
            if status == "NEW":
                new_file_count += 1
            elif status == "UPDATED":
                updated_files_count =+ 1

        result = SyncCalculationTaskResult()
        result.description = "There is no update on upload folder"
        if new_file_count + updated_files_count > 0:
            result.last_update_time = datetime.fromtimestamp(last_updated_time, tz=timezone.utc).strftime('%Y-%m-%d-%H:%M')
            result.description = f"New File Count: {new_file_count} Updated Files: {updated_files_count}"

        result.status = SyncCalculationStatus.SYNC_NEEDED if updated_files else SyncCalculationStatus.SYNC_NOT_NEEDED
        return result

    def check_folder_sync_status(self, study_id: str, obfuscation_code: str, target_local_path: str) -> SyncTaskResult:
        result = SyncTaskResult()
        result.status = SyncTaskStatus.NO_TASK
        result.description = f""
        result.last_update_time = ''
        return result

    @staticmethod
    def _is_update_needed(filename, dir1, dir2):
        file1 = os.path.join(dir1, filename)
        file2 = os.path.join(dir2, filename)

        try:
            st1 = os.stat(file1)
            st2 = os.stat(file2)
            is_updated = int((st1.st_mtime - st2.st_mtime) * 1000) > 0
            modified = datetime.fromtimestamp(st1.st_mtime, tz=timezone.utc).strftime('%Y-%m-%d-%H:%M')
            return is_updated, modified, st1.st_mtime
        except os.error:
            raise MetabolightsException(f'Error while comparing file {filename}')

    def calculate_folder_sync_status(self, source, target):
        source_files = self.get_file_set_in_folder(source)
        target_files = self.get_file_set_in_folder(target)

        common = source_files.intersection(target_files)

        data1 = sorted(source_files.difference(target_files))

        updated_files = set()
        for item in data1:
            if item.startswith('.'):
                continue
            file_path = os.path.join(source, item)
            modified = datetime.fromtimestamp(os.stat(file_path).st_mtime, tz=timezone.utc).strftime('%Y-%m-%d-%H:%M')
            modified_timestamp = os.stat(file_path).st_mtime
            updated_files.add((item, "NEW", modified, modified_timestamp))

        data2 = sorted(common)

        for item in data2:
            is_updated, modified, modified_timestamp = self._is_update_needed(item, source, target)
            if is_updated:
                updated_files.add((item, 'UPDATED', modified, modified_timestamp))
        return updated_files

    def get_file_set_in_folder(self, source):
        source_files = set()
        for cwd, dirs, files in os.walk(source):

            for f in files:
                path = os.path.relpath(os.path.join(cwd, f), source)
                re_path = path.replace('\\', os.path.sep).strip(os.path.sep)
                source_files.add(re_path)
        return source_files