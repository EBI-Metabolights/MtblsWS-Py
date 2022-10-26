import os
import shutil

from app.services.storage_service.acl import Acl
from app.services.storage_service.exceptions import StorageServiceException
from app.services.storage_service.file_manager import FileManager


class MountedVolumeFileManager(FileManager):

    def __init__(self, name, mounted_root_folder):
        super(MountedVolumeFileManager, self).__init__(name=name)
        self.mounted_root_folder = mounted_root_folder

    def create_folder(self, target: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
        try:
            path = self._get_abs_path(target)
            os.makedirs(path, mode=acl, exist_ok=exist_ok)
        except (OSError, FileExistsError):
            return False
        return True

    def delete_folder(self, target: str) -> bool:
        path = self._get_abs_path(target)
        if not os.path.exists(path):
            return True
        try:
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=False)
            else:
                return False
            return True
        except (OSError, Exception):
            return False

    def does_folder_exist(self, source: str) -> bool:
        source_path = self._get_abs_path(source)
        if os.path.exists(source_path):
            return os.path.isfile(source_path) or os.path.isdir(source_path)
        return False

    def get_folder_permission(self, source: str) -> Acl:
        source_path = self._get_abs_path(source)
        chmod = int(oct(os.stat(source_path).st_mode & 0o770), 8)
        try:
            permission = Acl(chmod)
        except:
            permission = Acl.UNKNOWN

        return permission

    def is_file(self, source: str) -> bool:
        source_path = self._get_abs_path(source)
        if os.path.exists(source_path):
            return os.path.isfile(source_path)
        return False

    def is_folder(self, source: str) -> bool:
        source_path = self._get_abs_path(source)
        if os.path.exists(source_path):
            return os.path.isdir(source_path)
        return False

    def update_folder_permission(self, source: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE) -> bool:
        source_path = self._get_abs_path(source)
        chmod = acl.value

        return self._update_chmod(source_path, chmod)

    def get_uri(self, source):
        self._validate_path(source)
        return self._get_abs_path(source)

    def get_base_uri(self, source):
        return self.mounted_root_folder

    @staticmethod
    def _update_chmod(file, chmod):
        previous_mask = os.umask(0)
        try:
            os.chmod(file, chmod)
        except (OSError, Exception):
            return False
        finally:
            os.umask(previous_mask)
        return True

    def _get_validated_abs_path(self, path: str):
        abs_path = self._get_abs_path(path)
        if not os.path.exists(abs_path):
            message = f'Source  {path} does not exist'
            raise StorageServiceException(StorageServiceException.ERR_CODE_FILE_NOT_EXIST, message)
        return abs_path

    @staticmethod
    def _validate_path(source: str):
        if not source or source.startswith('.') or source.startswith('..'):
            message = f'{source} is not allowed'
            raise StorageServiceException(StorageServiceException.ERR_CODE_NOT_ALLOWED_FILE, message)
        
    def _get_abs_path(self, source):
        self._validate_path(source)
        if source == os.sep:
            return self.mounted_root_folder

        if not source.startswith(self.mounted_root_folder):
            return os.path.join(self.mounted_root_folder, source.lstrip(os.sep))
        return source

    def _get_relative_path(self, source):
        self._validate_path(source)
        if source.startswith(self.mounted_root_folder):
            return source.replace(self.mounted_root_folder, '', 1)
        if source.startswith(os.sep):
            return source.lstrip(os.sep)
        return source
