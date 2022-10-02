import datetime
import hashlib
import os
import random
import shutil
from distutils.dir_util import copy_tree
from pathlib import Path
from typing import List

from dirhash import dirhash

from app.services.storage_service.acl import Acl
from app.services.storage_service.exceptions import StorageServiceException
from app.services.storage_service.file_descriptor import FileDescriptor, FileType
from app.services.storage_service.file_manager import FileManager


class MountedVolumeFileManager(FileManager):

    def __init__(self, name, mounted_root_folder):
        super(MountedVolumeFileManager, self).__init__(name=name)
        self.mounted_root_folder = mounted_root_folder

    def get_uri(self, source):
        self._validate_path(source)
        return self._get_abs_path(source)

    def get_base_uri(self, source):
        return self.mounted_root_folder

    def create_folder(self, target: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE, exist_ok: bool = True) -> bool:
        try:
            path = self._get_abs_path(target)
            os.makedirs(path, mode=acl.value, exist_ok=exist_ok)
        except (OSError, FileExistsError):
            return False
        return True

    def delete(self, target: str) -> bool:
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

    def move(self, source: str, target: str, force: bool = True, backup_target: bool = False) -> bool:
        return self._safe_copy_or_move(self._move, source, target, force, backup_target)

    def copy(self, source: str, target: str, force: bool = True, backup_target: bool = False) -> bool:
        return self._safe_copy_or_move(self._copy, source, target, force, backup_target)

    @staticmethod
    def _move(source_path: str, target_path: str):
        try:
            shutil.move(source_path, target_path)
        except (OSError, Exception):
            return False

    @staticmethod
    def _copy(source_path: str, target_path: str):
        try:
            if os.path.isfile(source_path):
                shutil.copyfile(source_path, target_path)
            elif os.path.isdir(source_path):
                copy_tree(source_path, target_path)
            else:
                return False
        except (OSError, Exception):
            return False

    def _safe_copy_or_move(self, operation, source: str, target: str, force: bool = True, backup_target: bool = False) -> bool:
        source_path = self._get_validated_abs_path(source)
        target_path = self._get_abs_path(target)
        if not force and os.path.exists(target_path):
            message = f'Target {target} exists'
            raise StorageServiceException(StorageServiceException.ERR_CODE_FILE_EXIST, message)

        backup_file_path = None
        if os.path.exists(target_path) and force:
            base_name = os.path.basename(target_path)
            dir_name = os.path.dirname(target_path)
            timestamp = str(int(datetime.datetime.now().timestamp()* 1000))
            random_data = random.randint(1000000, 9999999)
            file_name = base_name + timestamp + "-" + random_data
            backup_file_path = os.path.join(dir_name, file_name.lstrip(os.sep))
            os.rename(target_path, backup_file_path)

        try:
            operation(source_path, target_path)
        except (OSError, Exception):
            return False
        if backup_file_path and not backup_target:
            self.delete(self._get_relative_path(backup_file_path))
        return True

    def exists(self, source: str) -> bool:
        source_path = self._get_abs_path(source)
        if os.path.exists(source_path):
            return os.path.isfile(source_path) or os.path.isdir(source_path)
        return False

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

    def get_permission(self, source: str) -> Acl:
        source_path = self._get_abs_path(source)
        chmod = int(oct(os.stat(source_path).st_mode & 0o770), 8)
        try:
            permission = Acl(chmod)
        except:
            permission = Acl.UNKNOWN

        return permission

    def update_permission(self, source: str, acl: Acl = Acl.AUTHORIZED_READ_WRITE, recursive: bool = False) -> bool:
        source_path = self._get_abs_path(source)
        chmod = acl.value
        if self.is_file(source_path) or not recursive:
            return self._update_chmod(source_path, chmod)

        if self.is_folder(source_path):
            success = self._update_chmod(source_path, chmod)
            error = success
            for root, dirs, files in os.walk(source_path):
                for d in dirs:
                    success = self._update_chmod(os.path.join(root, d), chmod)
                    if not success:
                        error = True
                for f in files:
                    success = self._update_chmod(os.path.join(root, f), chmod)
                    if not success:
                        error = True
            return not error
        return False

    def update_owner(self, source: str, user: str, group, recursive: bool = False) -> bool:
        source_path = self._get_abs_path(source)

        shutil.chown(source_path, user, group)
        return True

    def get_owner(self, source: str) -> str:
        source_path = self._get_abs_path(source)
        path = Path(source_path)
        owner = path.owner()
        return owner

    def list_folder(self, source: str, filter_pattern=None) -> List[FileDescriptor]:
        source_path = self._get_abs_path(source)
        file_list = []
        with os.scandir(source_path) as entries:
            for entry in entries:
                file_type = FileType.UNKNOWN
                size = -1
                created_time = os.path.getctime(entry.path)
                modified_time = os.path.getmtime(entry.path)
                if self.is_file(entry.path):
                    file_type = FileType.FILE
                    size = os.path.getsize(entry.path)
                elif self.is_folder(entry.path):
                    file_type = FileType.FOLDER
                path = self._get_relative_path(entry.path)
                parent_folder = os.path.dirname(path)

                fd = FileDescriptor(entry.name, parent_folder, file_type,
                                    created_time=created_time, modified_time=modified_time, size_in_bytes=size)
                file_list.append(fd)

        return file_list

    def get_file_descriptor(self, source: str) -> FileDescriptor:
        source_path = self._get_abs_path(source)
        file_type = FileType.UNKNOWN
        size = -1
        created_time = os.path.getctime(source_path)
        modified_time = os.path.getmtime(source_path)
        if os.path.isfile(source_path):
            file_type = FileType.FILE
            size = os.path.getsize(source_path)
        elif os.path.isdir(source_path):
            file_type = FileType.FOLDER
        path = self._get_relative_path(source_path)
        parent_folder = os.path.dirname(path)
        base_name = os.path.basename(source_path)
        fd = FileDescriptor(base_name, parent_folder, file_type,
                            created_time=created_time, modified_time=modified_time, size_in_bytes=size)
        return fd

    def hash_sha256(self, source, ignore_list=None):
        source_path = self._get_abs_path(source)
        if os.path.isfile(source_path):
            return self._calculate_sha256(source_path)
        elif os.path.isdir(source_path):
            if ignore_list:
                return dirhash(source_path, 'sha256', ignore=ignore_list)
            else:
                return dirhash(source_path, 'sha256')
        else:
            return None

    @staticmethod
    def _calculate_sha256(file):
        block_size = 65536
        sha = hashlib.sha256()
        with open(file, 'rb') as f:
            file_buffer = f.read(block_size)
            while len(file_buffer) > 0:
                sha.update(file_buffer)
                file_buffer = f.read(block_size)
        return sha.hexdigest()

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
