import os
import shutil
from typing import Union, List

from app.file_utils import make_dir_with_chmod
from app.services.storage_service.acl import Acl
from app.services.storage_service.exceptions import StorageServiceException
from app.services.storage_service.file_manager import FileManager
from app.tasks.datamover_tasks.basic_tasks import file_management 
from app.ws.settings.utils import get_cluster_settings


class RemoteFileManager(FileManager):

    def __init__(self, name, mounted_root_folder):
        super(RemoteFileManager, self).__init__(name=name)
        self.mounted_root_folder = mounted_root_folder

    
    def create_folder(self, folder_path: Union[str, List[str]], acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
                
        inputs = {"folder_paths": folder_path, "acl": acl, "exist_ok": exist_ok }
        task = file_management.create_folders.apply_async(kwargs=inputs, expires=60*5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        if not output:
            return False

        for item in output:
            if not item["status"]:
                return False
        return True

    def delete_folder(self, folder_path: str) -> bool:
        inputs = {"folder_paths": folder_path}
        task = file_management.delete_folders.apply_async(kwargs=inputs, expires=60*5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds * 5)
        if not output:
            return False

        for item in output:
            if not item["status"]:
                return False
        return True


    def move(self, source_path: str, target_path: str, timeout=None) -> bool:
        inputs = {"source_path": source_path, "target_path": target_path}
        task = file_management.move.apply_async(kwargs=inputs, expires=60*5)
        cluster_settings = get_cluster_settings()
        if not timeout:
            output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds * 5)
        else:
            output = task.get(timeout=timeout)
        if not output:
            return False

        for item in output:
            if not item["status"]:
                return False
        return True
    
    def does_folder_exist(self, folder_path: str) -> bool:
        inputs = {"source_path": folder_path}
        task = file_management.exists.apply_async(kwargs=inputs, expires=60*5)
        cluster_settings = get_cluster_settings()
        
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        return output

    def get_folder_permission(self, source_path: str) -> Acl:
        inputs = {"source_path": source_path}
        task = file_management.get_permission.apply_async(kwargs=inputs, expires=60*5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)

        
        if output and "status" in output and output["status"]:
            permission_int = output["value"]
            chmod = int(oct(permission_int & 0o770), 8)
            try:
                permission = Acl(chmod)
            except:
                permission = Acl.UNKNOWN
        else:
            return Acl.UNKNOWN
        return permission

    def is_file(self, source_path: str) -> bool:
        inputs = {"source_path": source_path}
        task = file_management.isfile.apply_async(kwargs=inputs, expires=60*5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        return output

    def is_folder(self, source_path: str) -> bool:
        inputs = {"source_path": source_path}
        task = file_management.isdir.apply_async(kwargs=inputs, expires=60*5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        return output

    def update_folder_permission(self, paths: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE) -> bool:
        inputs = {"paths": paths, "acl": acl}
        task = file_management.chmod.apply_async(kwargs=inputs, expires=60*5)
        cluster_settings = get_cluster_settings()
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds)
        if not output:
            return False

        for item in output:
            if not item["status"]:
                return False
        return True

    # def get_base_uri(self, source):
    #     return self.mounted_root_folder

    # @staticmethod
    # def _update_chmod(file, chmod, guid: bool = False):
    #     previous_mask = os.umask(0)
    #     try:
    #         chmod = int(('2' + str(oct(chmod)[-3:])), 8) if guid else chmod
    #         os.chmod(file, chmod)
    #     except (OSError, Exception):
    #         return False
    #     finally:
    #         os.umask(previous_mask)
    #     return True

    # def _get_validated_abs_path(self, path: str):
    #     abs_path = self._get_abs_path(path)
    #     if not os.path.exists(abs_path):
    #         message = f'Source  {path} does not exist'
    #         raise StorageServiceException(StorageServiceException.ERR_CODE_FILE_NOT_EXIST, message)
    #     return abs_path

    # def get_uri(self, source_file):
    #     return self._get_abs_path(source_file)

    # def _validate_path(self, source: str):
    #     if not source or source.startswith('.') or source.startswith('..'):
    #         message = f'{source} is not allowed'
    #         raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_FILE, message)
        
    # def _get_abs_path(self, source):
    #     self._validate_path(source)
    #     if source == os.sep:
    #         return self.mounted_root_folder

    #     if not source.startswith(self.mounted_root_folder):
    #         return os.path.join(self.mounted_root_folder, source.lstrip(os.sep))
    #     return source

    # def _get_relative_path(self, source):
    #     self._validate_path(source)
    #     if source.startswith(self.mounted_root_folder):
    #         return source.replace(self.mounted_root_folder, '', 1)
    #     if source.startswith(os.sep):
    #         return source.lstrip(os.sep)
    #     return source
